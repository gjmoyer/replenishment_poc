"""DaySim: headless deterministic store-day loop (M2).

One process, in-memory state (Kafka arrives in M3). Each sim-minute tick:
sales -> receipts -> trucks -> silent-drain -> rule evaluation ->
associate auto-confirm. Prints tasks as they fire; ends with a summary +
acceptance assertions (doc/07 M2).

Time windows are half-open [now-W, now): a sale stamped exactly now-30
counts in velocity_30m; older than now-120 is evicted. See test_sim
boundary tests.

Run:  uv run python -m sim.loop --seed 42
"""
from __future__ import annotations

import argparse
from collections import Counter, deque
from dataclasses import dataclass, field

from core.config import get_config
from decision.rules import (
    ShelfState,
    cases_needed,
    effective_capacity,
    evaluate,
    evaluate_truck,
)
from decision.state import MutableShelf
from sim.catalog import STORES, Sku, Store, build_catalog
from sim.clock import SimClock, fmt
from sim.scenarios import ScenarioFlags
from sim.shopper import ShopperSim
from sim.truck import TruckSim

Key = tuple[str, str]  # (store_id, sku)
NOON_MIN = 12 * 60
RECEIPT_WAVES_MIN = (8 * 60, 15 * 60)
RECEIPT_SPIKE_MIN = 12 * 60
RECEIPT_WAVE_SIZE = 4  # SKUs per scheduled wave


@dataclass
class TaskEvent:
    store_id: str
    sku: str
    emit_min: int
    action: str  # task | check
    reason: str
    cases: int


@dataclass
class DaySummary:
    seed: int
    tasks: list[TaskEvent] = field(default_factory=list)
    done_count: int = 0
    abandoned_count: int = 0
    suppress_counts: Counter = field(default_factory=Counter)
    sales_units: int = 0
    demand_units: int = 0
    # (store, sku) -> units of unmet demand. shelf_gap = shelf empty while
    # backroom stocked (the replenishment miss); boh_empty = store truly out.
    lost_gap: Counter = field(default_factory=Counter)
    lost_empty: Counter = field(default_factory=Counter)
    gap_events: list[tuple[int, tuple[str, str], int]] = field(default_factory=list)
    dones: list[tuple[tuple[str, str], int, int]] = field(default_factory=list)
    # ((store, sku), task_emit_min, done_min)

    def count(self, reason: str, before_min: int | None = None) -> int:
        return sum(
            1
            for t in self.tasks
            if t.reason == reason and (before_min is None or t.emit_min <= before_min)
        )


def recoverable_gap(summary: DaySummary, sla_min: int = 25,
                    day_end: int = 22 * 60) -> dict[tuple[str, str], int]:
    """Gap units arriving >sla_min after a task fired, before it closed.

    Approximation of "recoverable with a faster associate": loss inside
    [emit+sla, close], where close is done time, next emit for the key, or
    day end. Methodology is intentionally conservative-documented; see docs.
    """
    closes: dict[tuple[tuple[str, str], int], int] = {(k, e): d for k, e, d in summary.dones}
    emits: dict[tuple[str, str], list[int]] = {}
    for t in summary.tasks:
        if t.action == "task":
            emits.setdefault((t.store_id, t.sku), []).append(t.emit_min)
    for v in emits.values():
        v.sort()
    out: dict[tuple[str, str], int] = {}
    for minute, key, units in summary.gap_events:
        for i, e in enumerate(emits.get(key, [])):
            nxt = emits[key][i + 1] if i + 1 < len(emits[key]) else day_end
            close = closes.get((key, e), nxt)
            if e + sla_min < minute <= min(close, nxt):
                out[key] = out.get(key, 0) + units
                break
    return {k: v for k, v in out.items() if v > 0}


class DaySim:
    def __init__(
        self,
        seed: int | None = None,
        flags: ScenarioFlags | None = None,
        stores: tuple[Store, ...] = STORES,
        skus: tuple[Sku, ...] | None = None,
        associate_delay_min: int | None = None,
        verbose: bool = True,
    ) -> None:
        cfg = get_config()
        self.seed = cfg.sim.seed if seed is None else seed
        self.flags = flags or ScenarioFlags()
        self.stores = stores
        self.skus: dict[str, Sku] = {s.sku: s for s in (skus or build_catalog())}
        self.associate_delay = (
            cfg.sim.associate_delay_min if associate_delay_min is None else associate_delay_min
        )
        self.open_timeout = cfg.sim.open_timeout_min
        self.receipt_cases = cfg.sim.receipt_cases
        self.verbose = verbose
        self.clock = SimClock(cfg.sim.open_min, cfg.sim.close_min)
        self.shoppers = {
            st.store_id: ShopperSim(st, self.seed, self.flags, tuple(self.skus.values()))
            for st in stores
        }
        self.trucks = {
            st.store_id: TruckSim(
                store_id=st.store_id, schedule_min=cfg.sim.trucks_min, seed=self.seed
            )
            for st in stores
        }
        self.shelf: dict[Key, MutableShelf] = {}
        for st in stores:
            for s in self.skus.values():
                cap = effective_capacity(s.shelf_capacity_units, s.is_promo)
                self.shelf[(st.store_id, s.sku)] = MutableShelf(
                    boh=s.opening_boh,
                    shelf_est=cap,  # opening assumption: full (doc/02)
                    effective_cap=cap,
                    case_size=s.case_size_units,
                )
        self.sales_ts: dict[Key, deque] = {k: deque() for k in self.shelf}
        self.last_task_emit: dict[Key, int] = {}
        self.open: dict[Key, dict] = {}
        self.checks: dict[Key, dict] = {}  # verify-dock checks: NOT tasks (no debounce reset)
        self.pending_receipts: list[tuple[int, str, tuple[str, ...]]] = []
        self.summary = DaySummary(seed=self.seed)

    def log(self, msg: str) -> None:
        if self.verbose:
            print(msg)

    # -- helpers ---------------------------------------------------------

    def _velocities(self, key: Key, now: int) -> tuple[float, float]:
        """Half-open windows: [now-120, now) and [now-30, now)."""
        dq = self.sales_ts[key]
        while dq and dq[0] < now - 120:
            dq.popleft()
        v30 = sum(1 for x in dq if x >= now - 30) / 30
        v120 = len(dq) / 120
        return v30, v120

    def _snapshot(self, key: Key, now: int) -> ShelfState:
        s = self.skus[key[1]]
        m = self.shelf[key]
        v30, v120 = self._velocities(key, now)
        return ShelfState(
            boh=m.boh,
            shelf_est=m.shelf_est,
            shelf_capacity_units=s.shelf_capacity_units,
            case_size=s.case_size_units,
            is_promo=s.is_promo,
            is_bulk=s.is_bulk,
            threshold_pct=s.threshold_pct,
            velocity_30m=v30,
            velocity_120m=v120,
            last_task_min=self.last_task_emit.get(key),
            has_open_task=key in self.open,
            zero_flag=m.zero_flag,
            zero_since_min=m.zero_since_min,
        )

    def _emit_task(self, key: Key, now: int, reason: str, cases: int) -> None:
        store_id, sku = key
        self.open[key] = {"emit_min": now, "cases": cases, "reason": reason}
        self.last_task_emit[key] = now  # only real tasks reset debounce
        self.summary.tasks.append(TaskEvent(store_id, sku, now, "task", reason, cases))
        loc = "endcap+aisle" if self.skus[sku].is_promo else "aisle"
        self.log(f"[{fmt(now)}] {store_id} {sku}: fetch {cases} cases ({loc}) [{reason}]")

    def _emit_check(self, key: Key, now: int, reason: str) -> None:
        store_id, sku = key
        self.checks[key] = {"emit_min": now, "reason": reason}
        self.summary.tasks.append(TaskEvent(store_id, sku, now, "check", reason, 0))
        self.log(f"[{fmt(now)}] {store_id} {sku}: VERIFY DOCK [{reason}]")

    # -- tick stages ------------------------------------------------------

    def _stage_sales(self, now: int) -> None:
        for st in self.stores:
            for sku, units in self.shoppers[st.store_id].gen_sales(now).items():
                key = (st.store_id, sku)
                m = self.shelf[key]
                self.summary.demand_units += units
                # A shelf can't hand over more than it holds; anything above
                # min(BOH, shelf) is unmet demand, split by root cause.
                sellable = min(units, m.boh, m.shelf_est)
                lost = units - sellable
                if lost > 0:
                    if m.boh > 0:
                        self.summary.lost_gap[key] += lost
                        self.summary.gap_events.append((now, key, lost))
                    else:
                        self.summary.lost_empty[key] += lost
                if sellable <= 0:
                    continue
                m.apply_boh_update(m.boh - sellable, now)
                self.sales_ts[key].extend([now] * sellable)
                self.summary.sales_units += sellable

    def _apply_receipt(self, store_id: str, sku: str, now: int) -> None:
        m = self.shelf[(store_id, sku)]
        m.apply_boh_update(m.boh + self.skus[sku].case_size_units * self.receipt_cases, now)

    def _stage_receipts(self, now: int) -> None:
        if now in RECEIPT_WAVES_MIN or (self.flags.receipt_spike and now == RECEIPT_SPIKE_MIN):
            for st in self.stores:
                lowest = sorted(
                    self.skus.values(), key=lambda s: self.shelf[(st.store_id, s.sku)].boh
                )[:RECEIPT_WAVE_SIZE]
                for s in lowest:
                    self._apply_receipt(st.store_id, s.sku, now)
                skus = ", ".join(s.sku for s in lowest)
                self.log(f"[{fmt(now)}] {st.store_id} receipt wave: {skus}")
        for _, store_id, skus in [p for p in self.pending_receipts if p[0] <= now]:
            for sku in skus:
                self._apply_receipt(store_id, sku, now)
            self.log(f"[{fmt(now)}] {store_id} truck receipt (backroom, shelf unchanged)")
        self.pending_receipts = [p for p in self.pending_receipts if p[0] > now]

    def _stage_trucks(self, now: int) -> None:
        all_ids = list(self.skus)
        for st in self.stores:
            need_goods = [sku for (sid, sku), m in self.shelf.items()
                          if sid == st.store_id and m.boh < self.skus[sku].case_size_units]
            manifest = self.trucks[st.store_id].manifest_at(now, need_goods, all_ids)
            if manifest is None:
                continue
            self.log(f"[{fmt(now)}] {st.store_id} TRUCK {manifest}")
            for sku in manifest:
                key = (st.store_id, sku)
                m = self.shelf[key]
                s = self.skus[sku]
                d = evaluate_truck(
                    zero_flag=m.zero_flag, boh=m.boh, shelf_est=m.shelf_est,
                    effective_cap=m.effective_cap, case_size=s.case_size_units,
                )
                if d.action == "task":
                    self._emit_task(key, now, d.reason_code, d.cases)
                elif d.action == "check":
                    self._emit_check(key, now, d.reason_code)
                else:
                    self.summary.suppress_counts["truck_not_needed"] += 1
            rmin = self.trucks[st.store_id].receipt_min(now)
            self.pending_receipts.append((rmin, st.store_id, tuple(manifest)))

    def _stage_silent_drain(self, now: int) -> None:
        if self.flags.silent_oos and now == self.flags.silent_drain_min:
            for st in self.stores:
                # Drain the BUILDING, not just the shelf (parity with the
                # runner's correction): a silent OOS the truck must rescue
                # has no stock anywhere, so the zero rule waits for it.
                m = self.shelf[(st.store_id, self.flags.silent_sku)]
                if m.boh > 0:
                    m.apply_boh_update(0, now)
                m.zero_flag = True
                m.zero_since_min = now
                self.log(f"[{fmt(now)}] {st.store_id} {self.flags.silent_sku}: "
                         "silent OOS (shelf 0, no sales will follow)")

    def _stage_evaluate(self, now: int) -> None:
        for key in self.shelf:
            if key in self.open:
                d = evaluate(self._snapshot(key, now), now)
                if d.action == "supersede":
                    self.open[key]["cases"] = d.cases
                elif d.action == "suppress":
                    self.summary.suppress_counts[d.reason_code] += 1
                continue
            d = evaluate(self._snapshot(key, now), now)
            if d.action == "task":
                self._emit_task(key, now, d.reason_code, d.cases)
            elif d.action == "zero_marker":
                m = self.shelf[key]
                m.zero_flag = True
                m.zero_since_min = now
            elif d.action == "suppress":
                self.summary.suppress_counts[d.reason_code] += 1

    def _confirm(self, key: Key, now: int) -> bool:
        """Attempt associate confirmation. Returns True if closed."""
        m = self.shelf[key]
        s = self.skus[key[1]]
        cases = cases_needed(m.shelf_est, m.effective_cap, s.case_size_units, m.boh)
        if cases < 1:
            return False  # BOH empty (pre-receipt check): keep waiting
        m.apply_confirmation(cases, now)
        self.summary.done_count += 1
        self.log(f"[{fmt(now)}] {key[0]} {key[1]}: restocked {cases} cases (associate)")
        return True

    def _stage_confirm(self, now: int) -> None:
        for key, o in list(self.open.items()):
            age = now - o["emit_min"]
            if age >= self.open_timeout:
                del self.open[key]
                self.summary.abandoned_count += 1
                self.log(f"[{fmt(now)}] {key[0]} {key[1]}: task abandoned after {age}m")
            elif age >= self.associate_delay and self._confirm(key, now):
                self.summary.dones.append((key, o["emit_min"], now))
                del self.open[key]
        for key in list(self.checks):
            aged = now - self.checks[key]["emit_min"] >= self.associate_delay
            if aged and self._confirm(key, now):
                self.summary.dones.append((key, self.checks[key]["emit_min"], now))
                del self.checks[key]

    # -- run ---------------------------------------------------------------

    def run(self) -> DaySummary:
        while True:  # process every minute in [open, close], inclusive
            now = self.clock.now_min
            self._stage_sales(now)
            self._stage_receipts(now)
            self._stage_trucks(now)
            self._stage_silent_drain(now)
            self._stage_evaluate(now)
            self._stage_confirm(now)
            if self.clock.done:
                break
            self.clock.tick()
        return self.summary


def assert_acceptance(summary: DaySummary, flags: ScenarioFlags) -> None:
    errors = []
    n_normal_am = summary.count("normal_low", NOON_MIN)
    if n_normal_am < 3:
        errors.append(f"normal_low pre-noon tasks: {n_normal_am} < 3")
    if summary.count("promo_low") < 1:
        errors.append("no promo_low task all day")
    n_bulk = summary.count("bulk_due")
    if n_bulk > 3:
        errors.append(f"bulk_due tasks: {n_bulk} > 3 (debounce failed)")
    if flags.silent_oos:
        early = [t for t in summary.tasks
                 if t.sku == flags.silent_sku
                 and flags.silent_drain_min <= t.emit_min < 14 * 60]
        if early:
            errors.append(f"silent SKU tasked before truck rescue: {early}")
        rescue = [t for t in summary.tasks
                  if t.sku == flags.silent_sku and t.emit_min >= 14 * 60
                  and t.reason == "truck_zero"]
        if not rescue:
            errors.append("silent SKU never rescued by truck_zero task")
    if errors:
        raise AssertionError("M2 acceptance failed: " + "; ".join(errors))


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay one deterministic store day")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--no-assert", action="store_true")
    ap.add_argument("--promo-rush", action="store_true")
    ap.add_argument("--bulk-thrash", action="store_true")
    ap.add_argument("--no-silent-oos", action="store_true")
    ap.add_argument("--receipt-spike", action="store_true")
    args = ap.parse_args()

    flags = ScenarioFlags(
        promo_rush=args.promo_rush,
        bulk_thrash=args.bulk_thrash,
        silent_oos=not args.no_silent_oos,
        receipt_spike=args.receipt_spike,
    )
    sim = DaySim(seed=args.seed, flags=flags, verbose=not args.quiet)
    summary = sim.run()

    print(f"\n--- day summary (seed {sim.seed}) ---")
    print(f"sales units: {summary.sales_units}, tasks: {len(summary.tasks)}, "
          f"done: {summary.done_count}, abandoned: {summary.abandoned_count}, "
          f"open at close: {len(sim.open)}")
    print("by reason:", dict(Counter(t.reason for t in summary.tasks)))
    print("suppressed:", dict(summary.suppress_counts))
    print(f"normal pre-noon: {summary.count('normal_low', NOON_MIN)}, "
          f"promo: {summary.count('promo_low')}, bulk: {summary.count('bulk_due')}")

    if not args.no_assert:
        assert_acceptance(summary, flags)
        print("M2 acceptance: PASS")


if __name__ == "__main__":
    main()
