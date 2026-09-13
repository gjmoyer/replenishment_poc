"""decision service: Kafka consumer -> rules/router -> Postgres + restock_tasks.

Consumes boh_updates (sale|receipt), truck_arrivals, restock_confirmations.
Owns per-key shelf state in memory, persists every transition to Postgres
for the dashboard. LLM is consulted only for routed exceptions (router.py);
any reasoner failure falls back to rules without blocking tasks.

Run: python -m decision.service
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from collections import deque

from core.config import get_config
from decision.bus import Consumer, Producer, wait_broker
from decision.pg import Store, wait_pg
from decision.router import (
    BOH_ANOMALY_CASES,
    BOH_ANOMALY_TRUCK_WINDOW,
    REPEAT_DROP_CASES,
    LlmCache,
    ReasonContext,
    RoutedOutcome,
    route,
)
from decision.rules import (
    Decision,
    ShelfState,
    effective_capacity,
    evaluate,
    evaluate_truck,
    priority_of,
)
from decision.state import MutableShelf
from sim.catalog import stores_from_config

log = logging.getLogger("decision.service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TOPICS = ["boh_updates", "truck_arrivals", "restock_confirmations"]
LATE_WINDOW_MIN = 5  # drop events older than key-max - 5 (doc/02)
REJECT_SUPPRESS_MIN = 30


def sim_min_of(sim_ts: str) -> int:
    """2026-01-05T09:14:00 -> 554. Raises ValueError on garbage."""
    try:
        hm = sim_ts.split("T")[1][:5]
        h, m = hm.split(":")
        return int(h) * 60 + int(m)
    except (IndexError, ValueError, AttributeError) as e:
        raise ValueError(f"bad sim_ts: {sim_ts!r}") from e


class Brain:
    """All live state. Epoch reset clears it (sim restart)."""

    def __init__(self, cfg, store: Store, producer: Producer) -> None:
        self.cfg = cfg
        self.store = store
        self.producer = producer
        self.timeout_s = float(os.getenv("LLM_TIMEOUT_S", "30"))
        self.cache = LlmCache()
        self.shelf: dict[tuple[str, str], MutableShelf] = {}
        self.sales_ts: dict[tuple[str, str], deque] = {}
        self.last_emit: dict[tuple[str, str], int] = {}
        self.open: dict[tuple[str, str], dict] = {}
        self.checks: dict[tuple[str, str], dict] = {}
        # Idempotency is PG-backed (processed table), not in-memory: it must
        # survive restarts or redeliveries double-apply sales.
        self.max_sim: dict[tuple[str, str], int] = {}
        self.last_now = 0
        self.last_truck: dict[str, int] = {}
        self.suppress_until: dict[tuple[str, str], int] = {}
        self.last_suppress_log: dict[tuple[str, str, str], int] = {}
        self.epoch = 1

    # -- lifecycle ------------------------------------------------------

    def load_or_seed(self) -> None:
        """Boot state: adopt the control epoch FIRST, then resume or seed.

        Adopting the epoch before loading is critical: otherwise the
        background epoch poll sees epoch 1 != control N and wipes the
        just-restored state with a fresh seed (phantom sales + orphaned
        open tasks that can never be swept).
        """
        try:
            ctl = self.store.read_control()
            self.epoch = ctl["epoch"]
            self.last_now = ctl["sim_min"]
        except Exception as e:
            log.warning("control read failed at boot: %r", e)
        with self.store.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM shelf_state")
            rows_exist = cur.fetchone()[0] > 0
        if not rows_exist:
            self.seed_opening()
            return
        log.info("resuming persisted shelf state")
        with self.store.conn.cursor() as cur:
            cur.execute(
                "SELECT store_id, sku, boh, shelf_est, effective_cap, case_size,"
                " zero_flag, zero_since_min, last_task_min FROM shelf_state"
            )
            for (sid, sku, boh, shelf_est, cap, case, zf, zsince, last) in cur.fetchall():
                key = (sid, sku)
                self.shelf[key] = MutableShelf(
                    boh=boh, shelf_est=shelf_est, effective_cap=cap,
                    case_size=case, zero_flag=zf, zero_since_min=zsince)
                self.sales_ts[key] = deque()
                if last is not None:
                    self.last_emit[key] = last
            cur.execute(
                "SELECT store_id, sku, task_id, emit_sim_min, cases, reason"
                " FROM tasks WHERE status='open'"
            )
            for (sid, sku, tid, emit, cases, reason) in cur.fetchall():
                key = (sid, sku)
                if key in self.shelf:
                    self.open[key] = {
                        "task_id": tid, "emit_min": emit, "cases": cases,
                        "reason": reason, "shelf_at_emit": self.shelf[key].shelf_est}
                    self.last_emit[key] = emit
            cur.execute(
                "SELECT store_id, max(sim_min) FROM events WHERE type='truck'"
                " GROUP BY store_id"
            )
            for sid, m in cur.fetchall():
                self.last_truck[sid] = m
            # checks resume too, or the check->task upgrade path stays broken
            # for pre-restart checks (and their rows orphan).
            cur.execute(
                "SELECT store_id, sku, task_id, emit_sim_min FROM tasks"
                " WHERE action='check' AND status='open'"
            )
            for sid, sku, tid, emit in cur.fetchall():
                if (sid, sku) in self.shelf:
                    self.checks[(sid, sku)] = {"task_id": tid, "emit_min": emit}
            # velocities rebuild from history; without this the promo guard
            # misfires (v30=0) for up to 2h after every recreate.
            cur.execute(
                "SELECT store_id, sku, sim_min, units FROM sales_hist"
                " WHERE sim_min > %s",
                (self.last_now - 120,),
            )
            for sid, sku, m, units in cur.fetchall():
                dq = self.sales_ts.get((sid, sku))
                if dq is not None:
                    dq.extend([m] * units)

    def seed_opening(self) -> None:
        """Opening assumption: shelves full (doc/02)."""
        from sim.catalog import ALL_SKUS

        stores = stores_from_config(self.cfg.sim.stores)
        for st in stores:
            for s in ALL_SKUS:
                key = (st.store_id, s.sku)
                cap = effective_capacity(s.shelf_capacity_units, s.is_promo)
                self.shelf[key] = MutableShelf(
                    boh=s.opening_boh, shelf_est=cap,
                    effective_cap=cap, case_size=s.case_size_units,
                )
                self.sales_ts[key] = deque()
                self.persist(key, self.max_sim, None)

    def reset(self, epoch: int) -> None:
        log.info("epoch %s -> %s, clearing live state", self.epoch, epoch)
        self.epoch = epoch
        self.shelf.clear()
        self.sales_ts.clear()
        self.last_emit.clear()
        self.open.clear()
        self.checks.clear()
        self.max_sim = {}
        self.last_now = 0
        self.last_truck.clear()
        self.suppress_until.clear()
        self.last_suppress_log.clear()
        self.cache = LlmCache()
        self.seed_opening()

    # -- persistence -----------------------------------------------------

    def velocities(self, key: tuple[str, str], now: int) -> tuple[float, float]:
        dq = self.sales_ts[key]
        while dq and dq[0] < now - 120:
            dq.popleft()
        return sum(1 for x in dq if x >= now - 30) / 30, len(dq) / 120

    def persist(self, key: tuple[str, str], now: int, open_task: dict | None) -> None:
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        store_id, sku = key
        m, s = self.shelf[key], skus[sku]
        v30, v120 = self.velocities(key, now)
        self.store.upsert_shelf({
            "store_id": store_id, "sku": sku, "boh": m.boh,
            "shelf_est": m.shelf_est, "effective_cap": m.effective_cap,
            "case_size": s.case_size_units, "is_promo": s.is_promo,
            "is_bulk": s.is_bulk, "velocity_30m": v30, "velocity_120m": v120,
            "zero_flag": m.zero_flag, "zero_since_min": m.zero_since_min,
            "last_task_min": self.last_emit.get(key),
            "open_task": open_task,
        })

    def snapshot(self, key: tuple[str, str], now: int) -> ShelfState:
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        m, s = self.shelf[key], skus[key[1]]
        v30, v120 = self.velocities(key, now)
        return ShelfState(
            boh=m.boh, shelf_est=m.shelf_est,
            shelf_capacity_units=s.shelf_capacity_units,
            case_size=s.case_size_units, is_promo=s.is_promo, is_bulk=s.is_bulk,
            threshold_pct=s.threshold_pct, velocity_30m=v30, velocity_120m=v120,
            last_task_min=self.last_emit.get(key),
            has_open_task=key in self.open,
            zero_flag=m.zero_flag, zero_since_min=m.zero_since_min,
            cover_trigger_min=self.cfg.sim.associate_delay_min + 20,
        )

    # -- task emission ----------------------------------------------------

    def emit_task(self, key: tuple[str, str], now: int, outcome: RoutedOutcome,
                  reason_code: str) -> str:
        """Emit (or refresh) the open task. Returns the owning task_id."""
        if key in self.open:  # one open task max: refresh, never duplicate
            self.refresh_open(key, now, outcome, reason_code)
            return self.open[key]["task_id"]
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        store_id, sku = key
        m = self.shelf[key]
        task_id = uuid.uuid4().hex[:12]
        priority = priority_of(self.snapshot(key, now), reason_code)
        entry = {"task_id": task_id, "emit_min": now, "cases": outcome.cases,
                 "reason": reason_code, "shelf_at_emit": m.shelf_est}
        self.open[key] = entry
        self.last_emit[key] = now
        if outcome.source == "rule":
            need = m.effective_cap - m.shelf_est
            pct = m.shelf_est / m.effective_cap if m.effective_cap else 1.0
            s = skus[sku]
            rationale = (
                f"{m.shelf_est}/{m.effective_cap}={pct:.0%} "
                f"<{s.threshold_pct:.0%} -> {outcome.cases} cases "
                f"({need} units, BOH {m.boh})"
            )
        else:
            rationale = outcome.rationale
        task = {
            "task_id": task_id, "store_id": store_id, "sku": sku,
            "emit_sim_min": now, "action": "task", "reason": reason_code,
            "cases": outcome.cases, "source": outcome.source,
            "rationale": rationale, "confidence": outcome.confidence,
            "status": "open", "priority": priority,
            "shelf_at_emit": m.shelf_est, "boh_at_emit": m.boh,
        }
        self.store.add_task(task)
        self.producer.send("restock_tasks", f"{store_id}:{sku}", task)
        loc = "endcap+aisle" if skus[sku].is_promo else "aisle"
        self.store.log_event(now, store_id, sku, "task",
                             f"P{priority} fetch {outcome.cases} cases ({loc})"
                             f" [{reason_code}]",
                             outcome.source)
        self.persist(key, now, {"task_id": task_id, "cases": outcome.cases})
        return task_id

    def emit_check(self, key: tuple[str, str], now: int, reason: str) -> None:
        if key in self.checks:
            return  # one open check max; first one owns the key
        store_id, sku = key
        task_id = uuid.uuid4().hex[:12]
        self.checks[key] = {"task_id": task_id, "emit_min": now}
        task = {
            "task_id": task_id, "store_id": store_id, "sku": sku,
            "emit_sim_min": now, "action": "check", "reason": reason,
            "cases": 0, "source": "rule",
            "rationale": "Truck lists zero-shelf SKU; verify dock before fetch.",
            "confidence": None, "status": "open",
            "shelf_at_emit": self.shelf[key].shelf_est,
            "boh_at_emit": self.shelf[key].boh,
        }
        self.store.add_task(task)
        self.producer.send("restock_tasks", f"{store_id}:{sku}", task)
        self.store.log_event(now, store_id, sku, "check", f"VERIFY DOCK [{reason}]", "rule")
        self.persist(key, now, {"task_id": task_id, "cases": 0})

    # -- routing -----------------------------------------------------------

    def build_ctx(self, key: tuple[str, str], now: int, trigger: str,
                  sim_ts: str) -> ReasonContext:
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        store_id, sku = key
        m, s = self.shelf[key], skus[sku]
        v30, v120 = self.velocities(key, now)
        o = self.open.get(key)
        return ReasonContext(
            store_id=store_id, sku=sku, sim_ts=sim_ts, boh=m.boh,
            shelf_est=m.shelf_est, effective_cap=m.effective_cap,
            case_size=s.case_size_units, is_promo=s.is_promo, is_bulk=s.is_bulk,
            threshold_pct=s.threshold_pct, velocity_30m=v30, velocity_120m=v120,
            trigger=trigger, recent_sales=self.recent_units(key, now),
            open_task=({"task_id": o["task_id"], "cases": o["cases"],
                        "emit_min": o["emit_min"]} if o else None),
            truck_eta=None,
        )

    def recent_units(self, key: tuple[str, str], now: int) -> list[int]:
        """Per-minute UNITS sold over the last 10 sim-minutes (oldest first).

        The LLM must see demand quantities, never raw event timestamps.
        """
        try:
            with self.store.conn.cursor() as cur:
                cur.execute(
                    "SELECT sim_min, units FROM sales_hist"
                    " WHERE store_id=%s AND sku=%s AND sim_min>%s",
                    (key[0], key[1], now - 10),
                )
                by_min: dict[int, int] = {}
                for m, u in cur.fetchall():
                    by_min[m] = by_min.get(m, 0) + u
        except Exception as e:
            log.warning("sales_hist read failed: %r", e)
            return []
        return [by_min.get(m, 0) for m in range(now - 10, now)]

    def _record_llm(self, key: tuple[str, str], now: int, ctx: ReasonContext,
                      decision: Decision, outcome: RoutedOutcome,
                      record: dict | None, task_id: str | None) -> None:
        """Persist one reasoner call with join keys for the feedback loop.

        task_id links restock verdicts to their task row (outcomes land via
        set_llm_outcome at confirm/abandon); suppress verdicts keep NULL and
        are labeled at day-end finalize from lost_sales (doc/04).
        """
        if not record:
            return
        from decision.history import weekday_of

        record.update({
            "sim_min": now,
            "epoch": self.epoch,
            "needs_restock": bool(record["output"].get(
                "needs_restock", outcome.action == "task")),
            "task_id": task_id,
            "input": {
                "shelf_est": ctx.shelf_est, "boh": ctx.boh,
                "effective_cap": ctx.effective_cap,
                "case_size": ctx.case_size,
                "threshold_pct": ctx.threshold_pct,
                "is_promo": ctx.is_promo, "is_bulk": ctx.is_bulk,
                "velocity_30m": ctx.velocity_30m,
                "velocity_120m": ctx.velocity_120m,
                "recent_sales": list(ctx.recent_sales),
                "has_open_task": ctx.open_task is not None,
                "rule_cases": decision.cases,
                "rule_reason": decision.reason_code,
                "sim_min": now,
                "weekday": weekday_of(ctx.sim_ts),
            },
        })
        call_id = self.store.add_llm_call(record)
        if task_id is not None and call_id is not None:
            self.store.link_llm_task(call_id, task_id)
        self.store.log_event(
            now, key[0], key[1], "llm",
            f"{ctx.trigger}: restock={record['output'].get('needs_restock')} "
            f"conf={record['output'].get('confidence')}",
            outcome.source,
        )

    def _history_for(self, ctx: ReasonContext, now: int) -> list:
        """Retrieve labeled precedent for outcome-aware few-shots (doc/04 #2).

        Lazy and best-effort: [] on cold start, bad config, or any DB error —
        the static prompt few-shots carry the decision alone. Never raises.
        """
        if self.cfg.llm.history_cases <= 0:
            return []
        try:
            candidates = self.store.recent_labeled_calls(
                ctx.trigger, self.cfg.llm.history_pool)
        except Exception as e:
            log.warning("history retrieval failed: %r", e)
            return []
        try:
            from decision.history import pick_cases, weekday_of

            return pick_cases(
                {"shelf_est": ctx.shelf_est, "effective_cap": ctx.effective_cap,
                 "boh": ctx.boh, "velocity_30m": ctx.velocity_30m,
                 "velocity_120m": ctx.velocity_120m,
                 "sim_min": now, "weekday": weekday_of(ctx.sim_ts)},
                candidates, self.cfg.llm.history_cases)
        except Exception as e:
            log.warning("history selection failed: %r", e)
            return []

    def _route_with_history(self, decision: Decision, ctx: ReasonContext,
                            now: int) -> tuple:
        """route() with precedent attached — but only on cache miss.

        past_cases is excluded from the cache bucket, so identical states
        share one verdict without paying a SELECT; history only changes which
        examples justify a fresh verdict.
        """
        if self.cache.get(ctx, now) is None:
            ctx.past_cases = self._history_for(ctx, now)
        return route(decision, ctx, now, self.cache, self.timeout_s)

    def apply_routed(self, key: tuple[str, str], now: int, decision: Decision,
                     ctx: ReasonContext) -> None:
        outcome, record = self._route_with_history(decision, ctx, now)
        if outcome.action == "task":
            task_id = self.emit_task(key, now, outcome, decision.reason_code)
        else:
            task_id = None
        self._record_llm(key, now, ctx, decision, outcome, record, task_id)
        if outcome.action in ("suppress", "no_action"):
            if outcome.suppress_until_min:
                self.suppress_until[key] = now + outcome.suppress_until_min
            prev = self.last_suppress_log.get((key[0], key[1], decision.reason_code), -10**9)
            if now - prev >= 30:  # log transitions, not every tick
                self.last_suppress_log[(key[0], key[1], decision.reason_code)] = now
                self.store.log_event(now, key[0], key[1], "suppress",
                                     f"{decision.reason_code}: {outcome.rationale[:160]}",
                                     outcome.source)

    def route_with_repeat_check(self, key: tuple[str, str], now: int,
                                decision: Decision, sim_ts: str) -> None:
        """repeat_task trigger: open task exists and shelf fell >=1 case since emit.

        A repeat outcome REFRESHES the open entry (cases/rationale) — it never
        emits a second open task for the same key (one-open-task rule).
        """
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        o = self.open.get(key)
        if o and decision.action == "supersede":
            fell = o["shelf_at_emit"] - self.shelf[key].shelf_est
            if fell >= REPEAT_DROP_CASES * skus[key[1]].case_size_units:
                forced = Decision(
                    action="task", reason_code=decision.reason_code,
                    cases=decision.cases, detail=decision.detail,
                    llm_candidate=True, llm_trigger="repeat_task",
                )
                ctx = self.build_ctx(key, now, "repeat_task", sim_ts)
                outcome, record = self._route_with_history(forced, ctx, now)
                if outcome.action == "task":
                    self.refresh_open(key, now, outcome, decision.reason_code)
                    self._record_llm(key, now, ctx, forced, outcome, record,
                                     o["task_id"])
                else:
                    self._record_llm(key, now, ctx, forced, outcome, record,
                                     None)
                return
            self.refresh_open(key, now, RoutedOutcome(
                action="task", reason_code=decision.reason_code,
                cases=decision.cases, source="rule",
                rationale=decision.detail), decision.reason_code)
            return
        if decision.action == "task":
            ctx = self.build_ctx(key, now, decision.llm_trigger or "promo_ambiguous", sim_ts)
            self.apply_routed(key, now, decision, ctx)
        elif decision.action == "zero_marker":
            m = self.shelf[key]
            m.zero_flag = True
            m.zero_since_min = now
            self.persist(key, now, None)
        elif decision.action == "suppress":
            ctx = self.build_ctx(key, now, decision.llm_trigger or "promo_ambiguous", sim_ts)
            self.apply_routed(key, now, decision, ctx)

    def refresh_open(self, key: tuple[str, str], now: int,
                     outcome: RoutedOutcome, reason_code: str) -> None:
        """Update the existing open task in place (no duplicate task_id).

        Refreshes quantity/source/confidence but keeps the ORIGINAL emit
        rationale — the supersede bookkeeping text ("open task exists…")
        would otherwise replace the human-readable reason in the UI.
        """
        o = self.open[key]
        o["cases"] = outcome.cases
        with self.store.conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET cases=%s, source=%s,"
                " confidence=%s WHERE task_id=%s",
                (outcome.cases, outcome.source,
                 outcome.confidence, o["task_id"]),
            )
        self.store.log_event(now, key[0], key[1], "task",
                             f"refreshed to {outcome.cases} cases [{reason_code}]",
                             outcome.source)
        self.persist(key, now, {"task_id": o["task_id"], "cases": outcome.cases})

    # -- topic handlers -----------------------------------------------------

    def note_epoch(self, msg: dict) -> bool:
        """Epoch fence against restart time-travel. True = may process.

        The runner stamps every message with the day-epoch and truncates PG
        *before* bumping it, so: a higher epoch means a new day (adopt +
        reset, then process); a lower epoch is a stale pre-restart message
        (drop — otherwise a sim-730 event emits a future-stamped task into a
        sim-432 day). Missing epoch (adhoc publishers) processes as current.
        """
        epoch = msg.get("epoch")
        if epoch is None:
            return True
        try:
            epoch = int(epoch)
        except (TypeError, ValueError):
            log.warning("bad epoch %r, dropping message", msg.get("epoch"))
            return False
        if epoch == self.epoch:
            return True
        if epoch > self.epoch:
            self.reset(epoch)
            return True
        log.info("dropping stale epoch %s message (current %s)", epoch, self.epoch)
        return False

    def on_boh(self, msg: dict) -> None:
        try:
            store_id, sku = msg["store_id"], msg["sku"]
            now, boh = sim_min_of(msg["sim_ts"]), int(msg["boh"])
            key = (store_id, sku)
        except (KeyError, TypeError, ValueError) as e:
            log.warning("dropping malformed boh_updates %r: %r", msg, e)
            return
        if not self.note_epoch(msg):
            return
        if not self.store.claim_event(msg.get("event_id")):
            return  # redelivery: already applied
        if key not in self.shelf:
            log.warning("unknown key %s, skipping", key)
            return
        if now < self.max_sim.get(key, now) - LATE_WINDOW_MIN:
            log.info("dropping late event %s (%d)", key, now)
            return
        self.max_sim[key] = max(self.max_sim.get(key, now), now)
        self.last_now = max(self.last_now, now)
        if self.suppress_until.get(key, 0) > now:
            self.sweep_timeouts(now)
            self.persist(key, now, self.open.get(key))
            return

        m = self.shelf[key]
        old_boh = m.boh
        kind = m.apply_boh_update(boh, now)
        reason = msg.get("reason", "sale")
        if kind == "sale" and reason == "sale":
            sold = msg.get("delta")
            units = abs(int(sold)) if isinstance(sold, int) and sold < 0 else 1
            self.sales_ts[key].extend([now] * units)
            self.store.add_sale_hist(store_id, sku, now, units)
        elif kind == "receipt":
            from sim.catalog import ALL_SKUS

            skus = {s.sku: s for s in ALL_SKUS}
            self.maybe_boh_anomaly(key, now, msg.get("sim_ts", ""), boh - old_boh)
            if key in self.checks:  # receipt may upgrade check -> task
                d = evaluate_truck(
                    zero_flag=m.zero_flag, boh=m.boh, shelf_est=m.shelf_est,
                    effective_cap=m.effective_cap,
                    case_size=skus[sku].case_size_units,
                )
                if d.action == "task":
                    chk = self.checks.pop(key)
                    self.store.set_task_status(chk["task_id"], "superseded")
                    self.emit_task(key, now, RoutedOutcome(
                        action="task", reason_code="truck_zero", cases=d.cases,
                        source="rule", rationale=d.detail), "truck_zero")

        decision = evaluate(self.snapshot(key, now), now)
        self.route_with_repeat_check(key, now, decision, msg.get("sim_ts", ""))
        self.persist(key, now, self.open.get(key))
        self.sweep_timeouts(now)

    def maybe_boh_anomaly(self, key: tuple[str, str], now: int, sim_ts: str,
                          receipt_units: int) -> None:
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        last = self.last_truck.get(key[0])
        if last is None or now - last > BOH_ANOMALY_TRUCK_WINDOW:
            if receipt_units >= BOH_ANOMALY_CASES * skus[key[1]].case_size_units:
                m = self.shelf[key]
                decision = Decision(
                    action="no_action", reason_code="boh_anomaly_note",
                    detail=f"receipt +{receipt_units} units with no truck in "
                           f"{BOH_ANOMALY_TRUCK_WINDOW}m (BOH now {m.boh}).",
                    llm_candidate=True, llm_trigger="boh_anomaly",
                )
                self.apply_routed(key, now, decision,
                                  self.build_ctx(key, now, "boh_anomaly", sim_ts))

    def on_truck(self, msg: dict) -> None:
        try:
            store_id = msg["store_id"]
            now = sim_min_of(msg["sim_ts"])
            manifest = list(msg["manifest_skus"])
        except (KeyError, TypeError, ValueError) as e:
            log.warning("dropping malformed truck_arrivals %r: %r", msg, e)
            return
        if not self.note_epoch(msg):
            return
        if not self.store.claim_event(msg.get("event_id")):
            return
        self.last_now = max(self.last_now, now)
        self.last_truck[store_id] = now
        from sim.catalog import ALL_SKUS

        skus = {s.sku: s for s in ALL_SKUS}
        self.store.log_event(now, store_id, None, "truck",
                             f"manifest: {', '.join(manifest)}", None)
        for sku in manifest:
            key = (store_id, sku)
            if key not in self.shelf:
                continue
            m = self.shelf[key]
            d = evaluate_truck(
                zero_flag=m.zero_flag, boh=m.boh, shelf_est=m.shelf_est,
                effective_cap=m.effective_cap, case_size=skus[sku].case_size_units,
            )
            if d.action == "task":
                self.emit_task(key, now, RoutedOutcome(
                    action="task", reason_code="truck_zero", cases=d.cases,
                    source="rule", rationale=d.detail), "truck_zero")
            elif d.action == "check":
                self.emit_check(key, now, "truck_zero")
        # Wake-up call: silent zeros NOT on the manifest still get a fresh
        # look (they generate no sales events of their own). With the zero
        # split, covered ones task immediately, empty ones keep waiting.
        manifest_set = set(manifest)
        for (sid, sku), m in self.shelf.items():
            if sid != store_id or not m.zero_flag or sku in manifest_set:
                continue
            sim_ts = msg.get("sim_ts", "")
            key = (sid, sku)
            self.route_with_repeat_check(
                key, now, evaluate(self.snapshot(key, now), now), sim_ts)

    def on_confirm(self, msg: dict) -> None:
        try:
            store_id, sku = msg["store_id"], msg["sku"]
            now = sim_min_of(msg["sim_ts"])
            action = msg.get("action", "done")
            cases = int(msg.get("cases_fetched", 0))
            key = (store_id, sku)
        except (KeyError, TypeError, ValueError) as e:
            log.warning("dropping malformed confirmation %r: %r", msg, e)
            return
        if cases < 0:
            log.warning("negative cases_fetched, ignoring %r", msg)
            return
        if not self.note_epoch(msg):
            return
        if not self.store.claim_event(msg.get("event_id")):
            return  # redelivered confirm: never double-apply a refill
        if key not in self.shelf:
            return
        log.info("confirm %s %s action=%s cases=%s task=%s",
                 store_id, sku, action, cases, msg.get("task_id"))
        self.last_now = max(self.last_now, now)
        asked_cases = cases  # associate intent, pre-BOH-clamp (feedback loop)
        if action != "reject":
            # Never shelve more than the building holds: clamp to BOH cover.
            # A stale task (cases decided when stock existed) must not mint
            # phantom shelf. Held tasks stay open for the next receipt.
            from sim.catalog import ALL_SKUS

            case_size = next((s.case_size_units for s in ALL_SKUS if s.sku == sku), None)
            if case_size is None:
                log.warning("unknown sku %s, ignoring confirm", sku)
                return
            affordable = self.shelf[key].boh // case_size
            if cases > affordable:
                if affordable <= 0:
                    self.store.log_event(
                        now, store_id, sku, "confirm",
                        f"held: BOH {self.shelf[key].boh} covers no case — send a truck", None)
                    self.persist(key, now, self.open.get(key))
                    return
                log.info("confirm clamped %s -> %s cases (BOH cover)", cases, affordable)
                cases = affordable
        o = self.open.pop(key, None)
        self.checks.pop(key, None)
        if action == "reject":
            self.suppress_until[key] = now + REJECT_SUPPRESS_MIN
            if o:
                self.store.set_task_status(o["task_id"], "rejected")
                self.store.set_llm_outcome(o["task_id"], "rejected", now)
            self.store.log_event(now, store_id, sku, "confirm",
                                 f"rejected by associate; quiet {REJECT_SUPPRESS_MIN}m", None)
        else:
            self.shelf[key].apply_confirmation(cases, now)
            if o:
                from decision.feedback import classify_confirm

                self.store.mark_done(o["task_id"], now)
                self.store.set_llm_outcome(
                    o["task_id"],
                    classify_confirm(o["cases"], asked_cases, action), now)
            self.store.log_event(now, store_id, sku, "confirm",
                                 f"restocked {cases} cases (associate)", None)
        self.persist(key, now, None)

    def sweep_timeouts(self, now: int) -> None:
        timeout = self.cfg.sim.open_timeout_min
        for key, o in list(self.open.items()):
            if now - o["emit_min"] >= timeout:
                del self.open[key]
                self.store.set_task_status(o["task_id"], "abandoned")
                self.store.set_llm_outcome(o["task_id"], "abandoned", now)
                self.store.log_event(now, key[0], key[1], "abandon",
                                     f"open {timeout}m, abandoned", None)
                self.persist(key, now, None)
        # PG backstop: memory can lose an open entry across recreates,
        # redeliveries, or races while the PG row stays open. Abandon by
        # query so the timeout invariant holds structurally, not just
        # in memory. Throttled to ~10 wall-sec.
        if time.time() - getattr(self, "_last_pg_sweep", 0) > 10:
            self._last_pg_sweep = time.time()
            cutoff = now - timeout
            with self.store.conn.cursor() as cur:
                cur.execute(
                    "SELECT task_id, store_id, sku FROM tasks"
                    " WHERE status='open' AND emit_sim_min < %s",
                    (cutoff,),
                )
                stale = cur.fetchall()
            for task_id, sid, sku in stale:
                key = (sid, sku)
                if key in self.open and self.open[key]["task_id"] == task_id:
                    continue  # memory owns it; handled above
                self.store.set_task_status(task_id, "abandoned")
                self.store.set_llm_outcome(task_id, "abandoned", now)
                self.store.log_event(now, sid, sku, "abandon",
                                     f"open >{timeout}m (backstop), abandoned", None)
                if key in self.shelf:
                    self.persist(key, now, None)


def main() -> None:
    cfg = get_config()
    log.info("broker=%s pg=%s", cfg.kafka.broker, cfg.pg.url.split("@")[-1])
    wait_broker(cfg.kafka.broker)
    conn = wait_pg(cfg.pg.url)
    store = Store(conn)
    producer = Producer(cfg.kafka.broker)
    brain = Brain(cfg, store, producer)
    brain.load_or_seed()

    consumer = Consumer(cfg.kafka.broker, TOPICS, group="decision-v1")
    log.info("decision service live")
    last_epoch_poll = 0.0
    last_wall_sweep = 0.0
    try:
        while True:
            if time.time() - last_epoch_poll > 2:
                last_epoch_poll = time.time()
                try:
                    ctl = store.read_control()
                    if ctl["epoch"] != brain.epoch:
                        brain.reset(ctl["epoch"])
                except Exception as e:
                    log.warning("control poll failed: %r", e)
            # Wall-clock sweep: paused/idle days starve event-driven sweeps.
            if time.time() - last_wall_sweep > 10:
                last_wall_sweep = time.time()
                try:
                    brain.sweep_timeouts(brain.last_now)
                except Exception as e:
                    log.warning("wall sweep failed: %r", e)
            batch = consumer.poll(timeout_ms=500)
            n = 0
            clean = True
            for _tp, msgs in batch.items():
                for msg in msgs:
                    n += 1
                    topic = msg.topic
                    try:
                        if topic == "boh_updates":
                            brain.on_boh(msg.value)
                        elif topic == "truck_arrivals":
                            brain.on_truck(msg.value)
                        elif topic == "restock_confirmations":
                            brain.on_confirm(msg.value)
                    except Exception:
                        clean = False
                        log.exception("handler crashed on %s", topic)
            if n and clean:
                # Durable first: PG rows, then produced tasks, then offsets.
                # A dirty batch is NOT committed: redelivery replays into the
                # processed() idempotency guard instead of losing data.
                brain.producer.flush(5.0)
                consumer.commit()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
