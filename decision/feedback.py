"""LLM feedback loop: outcome labels + metric definitions (doc/04).

Pure functions, no I/O — the PG writes live in decision/pg.py, the event
wiring in decision/service.py. Metric definitions here are normative; the
dashboard SQL mirrors them (see doc/04 "Feedback metrics").

Outcome lifecycle for one llm_calls row:
  NULL (awaiting outcome) -> done | adjusted | rejected | abandoned
                             (restock path, set online at confirm/abandon)
                          -> suppressed_ok | suppressed_regret
                             (suppress path, set at day-end finalize from
                             lost_sales inside the regret window)

- override_rate = (adjusted + rejected) / (done + adjusted + rejected),
  per trigger. Doc/04 target: < 20%.
- suppress regret = a suppress verdict followed by lost sales for the same
  key inside [call_min, call_min + regret_window]. Restock precision (did a
  done task actually sell through?) is computed live in the dashboard by
  joining tasks to sales_hist — no persisted column needed.
"""
from __future__ import annotations

from typing import Literal

TaskOutcome = Literal["done", "adjusted", "rejected"]
SuppressOutcome = Literal["suppressed_ok", "suppressed_regret"]

REGRET_WINDOW_MIN = 30  # floor: even suppress_until=0 gets a 30-min audit
REGRET_WINDOW_MAX = 90  # cap: matches llm clamp upper bound
OVERRIDE_TARGET = 0.20  # doc/04: associate override should trend under this


def classify_confirm(tasked_cases: int, fetched_cases: int, action: str) -> TaskOutcome:
    """Label a restock-path outcome at confirmation time.

    adjusted = associate fetched a different quantity than tasked (the -1/+1
    buttons then Restock done). System BOH clamps do NOT count: pass the
    dashboard-asked cases, not the post-clamp value, as fetched_cases.
    """
    if action == "reject":
        return "rejected"
    return "done" if fetched_cases == tasked_cases else "adjusted"


def regret_window(suppress_until_min: int) -> int:
    """Audit window for a suppress verdict, clamped to [30, 90] sim-min."""
    return min(max(int(suppress_until_min or 0), REGRET_WINDOW_MIN), REGRET_WINDOW_MAX)


def suppress_verdict(lost_units: int) -> SuppressOutcome:
    """Label a suppress-path outcome from lost sales inside its window."""
    return "suppressed_regret" if lost_units > 0 else "suppressed_ok"


def override_rate(done: int, adjusted: int, rejected: int) -> float | None:
    """Share of decided restock tasks the associate changed or refused."""
    decided = done + adjusted + rejected
    if decided <= 0:
        return None
    return (adjusted + rejected) / decided


def regret_rate(regrets: int, suppressed: int) -> float | None:
    """Share of suppress verdicts followed by lost sales in-window."""
    if suppressed <= 0:
        return None
    return regrets / suppressed
