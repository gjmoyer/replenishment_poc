"""Router: rules decide, LLM advises on exceptions (doc/04).

- Decision.llm_candidate=True -> consult llm-reasoner (sync HTTP), with
  sim-time-aware cache: (store, sku, trigger, state-bucket) TTL 15 sim-min.
- Produces the two missing triggers from doc/04 with zero rule producers:
  boh_anomaly (receipt with no truck behind it) and repeat_task (open task
  + shelf still falling).
- Final quantity ALWAYS passes through cases_needed() + BOH clamp here,
  even if the LLM service already clamped (defense in depth).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field

from decision.rules import Decision, cases_needed

log = logging.getLogger("decision.router")

LLM_CACHE_TTL_SIM_MIN = 15
ROUTER_MAX_TIMEOUT_S = 12.0  # never stall the consumer loop behind the LLM
REPEAT_DROP_CASES = 1  # shelf fell >= this many cases since emit -> repeat_task
BOH_ANOMALY_CASES = 8  # receipt >= this many cases with no recent truck.
# Legitimate waves are 4 cases (scheduled or truck), so 8 means strictly more
# than any single wave — a genuinely unexpected jump, not routine restocking.
BOH_ANOMALY_TRUCK_WINDOW = 60  # ... within this many sim-min


@dataclass
class ReasonContext:
    store_id: str
    sku: str
    sim_ts: str
    boh: int
    shelf_est: int
    effective_cap: int
    case_size: int
    is_promo: bool
    is_bulk: bool
    threshold_pct: float
    velocity_30m: float
    velocity_120m: float
    trigger: str
    recent_sales: list
    open_task: dict | None
    truck_eta: str | None
    # Retrieved precedent (doc/04 #2), attached lazily by the service only
    # on cache miss. Excluded from the cache bucket: identical states share
    # one verdict, history only changes WHICH examples justify it.
    past_cases: list = field(default_factory=list)


@dataclass
class RoutedOutcome:
    action: str  # task | check | suppress | no_action
    reason_code: str
    cases: int
    source: str  # rule | llm | rule_fallback
    rationale: str
    confidence: float | None = None
    suppress_until_min: int = 0


class LlmCache:
    """Sim-time TTL cache. Keys expire in sim-minutes, not wall time."""

    def __init__(self, ttl_min: int = LLM_CACHE_TTL_SIM_MIN) -> None:
        self.ttl = ttl_min
        self._items: dict[tuple, tuple[int, RoutedOutcome]] = {}

    @staticmethod
    def bucket(ctx: ReasonContext) -> tuple:
        return (
            ctx.store_id,
            ctx.sku,
            ctx.trigger,
            ctx.shelf_est // max(ctx.case_size, 1),
            ctx.boh // max(ctx.case_size, 1),
            round(ctx.velocity_30m, 1),
            round(ctx.velocity_120m, 1),
        )

    def get(self, ctx: ReasonContext, now_min: int) -> RoutedOutcome | None:
        hit = self._items.get(self.bucket(ctx))
        if hit and now_min - hit[0] <= self.ttl:
            return hit[1]
        return None

    def put(self, ctx: ReasonContext, now_min: int, outcome: RoutedOutcome) -> None:
        self._items[self.bucket(ctx)] = (now_min, outcome)
        if len(self._items) > 2000:  # bounded; POC-scale eviction
            oldest = min(self._items, key=lambda k: self._items[k][0])
            del self._items[oldest]


def reasoner_url() -> str:
    return os.getenv("LLM_REASONER_URL", "http://127.0.0.1:8000").rstrip("/")


def call_reasoner(ctx: ReasonContext, timeout_s: float) -> tuple[dict, int, str]:
    """POST /reason. Returns (output_dict, latency_ms, input_hash).

    Raises on transport/validation failure — caller falls back to rules.
    """
    import httpx

    body = {
        "store_id": ctx.store_id,
        "sku": ctx.sku,
        "sim_ts": ctx.sim_ts,
        "state": {
            "boh": ctx.boh,
            "shelf_est": ctx.shelf_est,
            "effective_capacity": ctx.effective_cap,
            "velocity_30m": ctx.velocity_30m,
            "velocity_120m": ctx.velocity_120m,
        },
        "product": {
            "is_promo": ctx.is_promo,
            "is_bulk": ctx.is_bulk,
            "case_size": ctx.case_size,
            "threshold_pct": ctx.threshold_pct,
        },
        "trigger": ctx.trigger,
        "recent_sales": list(ctx.recent_sales)[-10:],
        "open_task": ctx.open_task,
        "truck_eta": ctx.truck_eta,
        "past_cases": list(ctx.past_cases)[-3:],
    }
    input_hash = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:12]
    started = time.perf_counter()
    with httpx.Client(timeout=timeout_s) as client:
        r = client.post(f"{reasoner_url()}/reason", json=body)
        r.raise_for_status()
        out = r.json()
    latency_ms = int((time.perf_counter() - started) * 1000)
    return out, latency_ms, input_hash


def final_cases(ctx: ReasonContext, override: int | None) -> int:
    """Rule-owned quantity math. Override may only reduce, never inflate."""
    rule_cases = cases_needed(ctx.shelf_est, ctx.effective_cap, ctx.case_size, ctx.boh)
    if override is None:
        return rule_cases
    return max(0, min(override, rule_cases))


def route(
    decision: Decision,
    ctx: ReasonContext,
    now_min: int,
    cache: LlmCache,
    timeout_s: float,
) -> tuple[RoutedOutcome, dict | None]:
    """Route one rule decision. Returns (outcome, llm_call_record|None).

    Non-candidates pass through untouched (source=rule). Candidates hit the
    cache, then the reasoner; any failure falls back to the rule outcome
    with source=rule_fallback (never blocks the task path).
    """
    timeout_s = min(timeout_s, ROUTER_MAX_TIMEOUT_S)
    if not decision.llm_candidate:
        if decision.action == "task":
            return RoutedOutcome(
                action="task",
                reason_code=decision.reason_code,
                cases=decision.cases,
                source="rule",
                rationale=decision.detail,
            ), None
        return RoutedOutcome(
            action="suppress" if decision.action == "suppress" else "no_action",
            reason_code=decision.reason_code,
            cases=0,
            source="rule",
            rationale=decision.detail,
        ), None

    cached = cache.get(ctx, now_min)
    if cached:
        return cached, None

    try:
        out, latency_ms, input_hash = call_reasoner(ctx, timeout_s)
    except Exception as e:  # transport down, timeout, bad status
        log.warning("reasoner unreachable (%r), rule fallback", e)
        return RoutedOutcome(
            action=decision.action if decision.action == "task" else "suppress",
            reason_code=decision.reason_code,
            cases=decision.cases if decision.action == "task" else 0,
            source="rule_fallback",
            rationale=f"{decision.detail} (LLM unreachable, rule fallback.)",
        ), {
            "store_id": ctx.store_id,
            "sku": ctx.sku,
            "trigger": ctx.trigger,
            "model": "unreached",
            "prompt_version": "?",
            "input_hash": "n/a",
            "latency_ms": 0,
            "fallback": True,
            "output": {"error": f"{type(e).__name__}"},
        }

    record = {
        "store_id": ctx.store_id,
        "sku": ctx.sku,
        "trigger": ctx.trigger,
        "model": out["meta"]["model"],
        "prompt_version": out["meta"]["prompt_version"],
        "input_hash": input_hash,
        "latency_ms": latency_ms,
        "fallback": out["meta"]["fallback"],
        "output": out["decision"],
    }
    d = out["decision"]
    if out["meta"]["fallback"]:
        return RoutedOutcome(
            action=decision.action if decision.action == "task" else "suppress",
            reason_code=decision.reason_code,
            cases=decision.cases if decision.action == "task" else 0,
            source="rule_fallback",
            rationale=f"{decision.detail} (LLM fallback.)",
        ), record

    cases = final_cases(ctx, d.get("cases_override"))
    if d["needs_restock"] and cases > 0:
        outcome = RoutedOutcome(
            action="task",
            reason_code=decision.reason_code,
            cases=cases,
            source="llm",
            rationale=d["rationale"],
            confidence=d.get("confidence"),
        )
    else:
        outcome = RoutedOutcome(
            action="suppress",
            reason_code=decision.reason_code + "+llm",
            cases=0,
            source="llm",
            rationale=d["rationale"],
            confidence=d.get("confidence"),
            suppress_until_min=d.get("suppress_until_min") or 0,
        )
    cache.put(ctx, now_min, outcome)
    return outcome, record
