"""Deterministic rules engine. Pure functions, no I/O, no LLM.

Implements doc/02-rules-engine.md. All quantities in UNITS except case
counts. Time is sim-minutes since midnight (int); the sim/service layer
converts clock times.

Quantity math lives HERE. The LLM (doc/04) may only advise reduce/delay;
its output always passes back through cases_needed() + clamp.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import ceil, floor
from typing import Literal

Action = Literal["task", "check", "suppress", "supersede", "zero_marker", "no_action"]

DEFAULT_THRESHOLD_PCT = 0.35
DEFAULT_BULK_DEBOUNCE_MIN = 75
DEFAULT_BULK_MIN_UNITS = 2
PROMO_SECONDARY_PCT = 0.5
# Locked with doc/04 trigger 1: endcap considered empty when BOH can't cover
# effective capacity with headroom, or velocity spikes.
PROMO_BOH_COVER_FACTOR = 1.2
PROMO_SPIKE_FACTOR = 2.0
MIN_SPIKE_VELOCITY = 0.2  # u/min floor: one stray sale is not a spike
STALE_ZERO_LLM_MIN = 60  # doc/04: stale_zero only after 60 sim-min
MAX_SUPPRESS_MIN = 180  # LLM/schema upper bound guard
EPS = 1e-9


def effective_capacity(shelf_capacity_units: int, is_promo: bool) -> int:
    """Shelf + endcap. Endcap is +50% of shelf for promo, else 0."""
    if shelf_capacity_units <= 0:
        raise ValueError("shelf_capacity_units must be > 0")
    secondary = round(shelf_capacity_units * PROMO_SECONDARY_PCT) if is_promo else 0
    return shelf_capacity_units + secondary


def cases_needed(
    shelf_est: int,
    effective_cap: int,
    case_size: int,
    boh: int,
) -> int:
    """Cases to fetch to refill to effective_cap, clamped to fit and BOH.

    Returns 0 when full OR when BOH cannot cover a single case (caller
    suppresses with reason boh_constrained).
    """
    if case_size <= 0:
        raise ValueError("case_size must be > 0")
    if boh < 0:
        raise ValueError("boh must be >= 0")
    if shelf_est < 0:
        raise ValueError("shelf_est must be >= 0")
    if shelf_est >= effective_cap:
        return 0
    need_units = effective_cap - shelf_est
    cases = ceil(need_units / case_size)
    return max(0, min(cases, floor(boh / case_size)))


def cover_min(shelf_est: int, velocity_30m: float) -> float:
    """Sim-minutes of shelf cover at current velocity; inf when stalled."""
    if velocity_30m <= 0:
        return float("inf")
    return shelf_est / max(velocity_30m, EPS)


@dataclass(frozen=True)
class ShelfState:
    """Per (store, sku) snapshot the rules evaluate."""

    boh: int
    shelf_est: int
    shelf_capacity_units: int
    case_size: int
    is_promo: bool = False
    is_bulk: bool = False
    threshold_pct: float = DEFAULT_THRESHOLD_PCT
    velocity_30m: float = 0.0
    velocity_120m: float = 0.0
    # sim-minutes since midnight of last emitted task; None = never tasked
    last_task_min: int | None = None
    bulk_debounce_min: int = DEFAULT_BULK_DEBOUNCE_MIN
    min_units_before_task: int = DEFAULT_BULK_MIN_UNITS
    has_open_task: bool = False
    zero_flag: bool = False
    zero_since_min: int | None = None

    def __post_init__(self) -> None:
        if self.boh < 0:
            raise ValueError("boh must be >= 0")
        if self.shelf_est < 0:
            raise ValueError("shelf_est must be >= 0")
        if self.shelf_capacity_units <= 0:
            raise ValueError("shelf_capacity_units must be > 0")
        if self.case_size <= 0:
            raise ValueError("case_size must be > 0")
        if not 0 < self.threshold_pct < 1:
            raise ValueError("threshold_pct must be in (0, 1)")
        if self.velocity_30m < 0 or self.velocity_120m < 0:
            raise ValueError("velocities must be >= 0")

    @property
    def effective_cap(self) -> int:
        return effective_capacity(self.shelf_capacity_units, self.is_promo)


@dataclass(frozen=True)
class Decision:
    action: Action
    reason_code: str
    cases: int = 0
    detail: str = ""
    # True when the router should consult the LLM (doc/04) instead of
    # finalizing the rule outcome. Rules stay deterministic; this is routing.
    llm_candidate: bool = False
    llm_trigger: str | None = None


def _pct(state: ShelfState) -> float:
    cap = state.effective_cap
    if cap <= 0:
        raise ValueError("effective_cap must be > 0")
    return state.shelf_est / cap


def _with_open_task_guard(decision: Decision, state: ShelfState) -> Decision:
    """One open task max per key: refresh, don't duplicate (doc/02)."""
    if decision.action == "task" and state.has_open_task:
        return Decision(
            action="supersede",
            reason_code=decision.reason_code,
            cases=decision.cases,
            detail=f"open task exists; refresh to {decision.cases} cases. {decision.detail}",
            llm_candidate=decision.llm_candidate,
            llm_trigger=decision.llm_trigger,
        )
    return decision


def evaluate(state: ShelfState, now_min: int) -> Decision:
    """Evaluate one (store, sku) at sim time now_min. Pure."""
    cap = state.effective_cap

    # 1. Silent zero — marker once, then wait (truck/LLM), never busy-loop.
    if state.shelf_est <= 0:
        if not state.zero_flag:
            return Decision(
                action="zero_marker",
                reason_code="zero_open",
                detail="shelf reads 0 units; marker set, awaiting truck/receipt.",
            )
        zero_age = now_min - (state.zero_since_min if state.zero_since_min is not None else now_min)
        if zero_age > STALE_ZERO_LLM_MIN:
            return Decision(
                action="no_action",
                reason_code="zero_waiting",
                detail=f"zero for {zero_age} sim-min; stale, needs review.",
                llm_candidate=True,
                llm_trigger="stale_zero",
            )
        return Decision(
            action="no_action",
            reason_code="zero_waiting",
            detail=f"zero for {zero_age} sim-min; waiting on truck.",
        )

    # 2. Healthy shelf — nothing to do.
    if _pct(state) >= state.threshold_pct:
        return Decision(
            action="no_action",
            reason_code="above_threshold",
            detail=f"{state.shelf_est}/{cap} units above {state.threshold_pct:.0%}.",
        )

    # 3. Shelf low — per-type gates.
    if state.is_bulk:
        return _with_open_task_guard(_evaluate_bulk(state, now_min), state)
    if state.is_promo:
        return _with_open_task_guard(_evaluate_promo(state), state)
    return _with_open_task_guard(_evaluate_normal(state), state)


def _finalize_task(state: ShelfState, reason_code: str, detail: str) -> Decision:
    cases = cases_needed(state.shelf_est, state.effective_cap, state.case_size, state.boh)
    if cases <= 0:
        return Decision(
            action="suppress",
            reason_code="boh_constrained",
            detail=f"{detail} BOH {state.boh} units covers no full case of {state.case_size}.",
        )
    return Decision(action="task", reason_code=reason_code, cases=cases, detail=detail)


def _evaluate_normal(state: ShelfState) -> Decision:
    cap = state.effective_cap
    return _finalize_task(
        state,
        "normal_low",
        f"{state.shelf_est}/{cap} units below {state.threshold_pct:.0%}.",
    )


def _evaluate_promo(state: ShelfState) -> Decision:
    """Promo: pct on effective (1.5x) capacity, plus endcap guard.

    Suppression is the interesting outcome — it becomes an llm_candidate
    (promo_ambiguous) for the router.
    """
    cap = state.effective_cap
    boh_suggests_empty = state.boh < cap * PROMO_BOH_COVER_FACTOR
    spike = (
        state.velocity_30m > PROMO_SPIKE_FACTOR * state.velocity_120m
        and state.velocity_30m >= MIN_SPIKE_VELOCITY
    )
    if not (boh_suggests_empty or spike):
        return Decision(
            action="suppress",
            reason_code="promo_endcap_likely",
            detail=(
                f"{state.shelf_est}/{cap} units low but BOH {state.boh} "
                f">= {PROMO_BOH_COVER_FACTOR}x effective and no velocity spike "
                f"({state.velocity_30m:.2f} vs 120m {state.velocity_120m:.2f} u/min); "
                "stock likely on endcap."
            ),
            llm_candidate=True,
            llm_trigger="promo_ambiguous",
        )
    d = _finalize_task(
        state,
        "promo_low",
        f"{state.shelf_est}/{cap} units (incl. endcap) low; "
        f"BOH {state.boh}, velocity {state.velocity_30m:.2f} u/min.",
    )
    if d.action == "task":
        return Decision(
            action=d.action,
            reason_code=d.reason_code,
            cases=d.cases,
            detail=d.detail,
            llm_candidate=True,
            llm_trigger="promo_ambiguous",
        )
    return d


def _evaluate_bulk(state: ShelfState, now_min: int) -> Decision:
    """Bulk: pct alone thrashes — require units + cases + debounce.

    Open-task dedup happens in _with_open_task_guard (supersede), so a
    second evaluation while open refreshes instead of suppressing (doc/02).
    """
    if state.shelf_est > state.min_units_before_task:
        return Decision(
            action="suppress",
            reason_code="bulk_min_units",
            detail=(
                f"shelf {state.shelf_est} units above bulk floor "
                f"{state.min_units_before_task}; pct ignored to avoid thrash."
            ),
        )
    cases = cases_needed(state.shelf_est, state.effective_cap, state.case_size, state.boh)
    if cases < 1:
        return Decision(
            action="suppress",
            reason_code="boh_constrained",
            detail=f"shelf {state.shelf_est} units but BOH {state.boh} covers no case.",
        )
    if state.last_task_min is not None:
        age = now_min - state.last_task_min
        if age < state.bulk_debounce_min:
            cover = cover_min(state.shelf_est, state.velocity_30m)
            rising = cover < state.bulk_debounce_min
            return Decision(
                action="suppress",
                reason_code="bulk_debounced",
                detail=f"last bulk task {age} sim-min ago (< {state.bulk_debounce_min}); suppress.",
                llm_candidate=rising,
                llm_trigger="bulk_candidate_suppressed" if rising else None,
            )
    return Decision(
        action="task",
        reason_code="bulk_due",
        cases=cases,
        detail=f"shelf {state.shelf_est}/{state.effective_cap} units, debounce clear.",
    )


def evaluate_truck(
    *,
    zero_flag: bool,
    boh: int,
    shelf_est: int,
    effective_cap: int,
    case_size: int,
) -> Decision:
    """Truck arrival for one SKU. Pure. Called per manifest entry.

    Ordering: manifest may arrive before the receipt BOH update. With BOH 0
    (or an empty shelf BOH can't fill) we emit a `check` (cases=0, verify
    dock), upgraded when the receipt lands. A healthy shelf never triggers,
    even at BOH 0 — that is a backroom problem, not a shelf task.
    """
    if not zero_flag and shelf_est > 0:
        return Decision(
            action="no_action",
            reason_code="truck_not_needed",
            detail="shelf healthy; manifest needs no action.",
        )
    cases = cases_needed(shelf_est, effective_cap, case_size, boh)
    if cases <= 0:
        return Decision(
            action="check",
            reason_code="truck_zero",
            cases=0,
            detail="truck manifest lists SKU with zero shelf/BOH; verify dock before fetch.",
        )
    return Decision(
        action="task",
        reason_code="truck_zero",
        cases=cases,
        detail=f"truck arrival resolves zero state; fetch {cases} cases.",
    )
