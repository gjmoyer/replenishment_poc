"""Outcome-aware few-shots: retrieve labeled precedent (doc/04 #2).

Pure functions, no I/O. The candidate rows come from
Store.recent_labeled_calls() (persistent llm_calls history, #1); selection
here scores them against the current state and enforces outcome diversity:
the most instructive pair is the closest success AND the closest mistake,
not three near-identical successes.

Feature space (all scale-free, so promo and bulk cases compare sanely):
  shelf_pct  = shelf_est / effective_cap   (how empty)
  boh_cover  = boh / effective_cap         (backroom depth in shelffuls)
  vel_ratio  = v30 / max(v120, 0.05), capped at 5 (spike intensity)
Distance is weighted Manhattan: pct counts double — emptiness is the
primary axis, cover and spike break ties.

Two temporal axes sit on top (retail days differ — Saturday evening is not
Tuesday morning, even at identical shelf readings):
  time_of_day: circular distance on sim_min (23:00 ~ 01:00), weighted like
    cover. Missing on legacy snapshots -> neutral, never penalized.
  weekday: soft same-day bonus, not a filter. Sparse triggers (stale_zero
    fires ~once a day) can't afford hard weekday matching — a Tuesday
    precedent still beats no precedent on Saturday.
"""

from __future__ import annotations

GOOD_OUTCOMES = frozenset({"done", "suppressed_ok"})
BAD_OUTCOMES = frozenset({"adjusted", "rejected", "suppressed_regret", "abandoned"})
VEL_FLOOR = 0.05
VEL_CAP = 5.0
DAY_MINUTES = 24 * 60
SAME_WEEKDAY_BONUS = 0.15


def weekday_of(sim_ts: str) -> str:
    """Sim weekday abbrev (Mon..Sun) from a sim_ts; ? when unparseable."""
    try:
        from datetime import date

        return date.fromisoformat(str(sim_ts).split("T")[0]).strftime("%a")
    except (ValueError, TypeError, IndexError):
        return "?"


def features(state: dict) -> tuple[float, float, float]:
    """Project a state snapshot into (shelf_pct, boh_cover, vel_ratio)."""
    cap = max(float(state.get("effective_cap") or state.get("effective_capacity") or 1), 1.0)
    v120 = max(float(state.get("velocity_120m", 0.0)), VEL_FLOOR)
    vel = min(float(state.get("velocity_30m", 0.0)) / v120, VEL_CAP)
    return (
        float(state.get("shelf_est", 0)) / cap,
        float(state.get("boh", 0)) / cap,
        vel,
    )


def distance(a: tuple[float, float, float], b: tuple[float, float, float]) -> float:
    """Weighted Manhattan distance in feature space."""
    return abs(a[0] - b[0]) + 0.5 * abs(a[1] - b[1]) + 0.5 * abs(a[2] - b[2])


def time_dist(a_min: int | None, b_min: int | None) -> float:
    """Circular time-of-day distance as day-fraction (0..0.5).

    None-safe: legacy snapshots without sim_min score neutral instead of
    being penalized for predating the feature.
    """
    if a_min is None or b_min is None:
        return 0.0
    d = abs(a_min - b_min) / DAY_MINUTES
    return min(d, 1.0 - d)


def match_distance(current: dict, candidate: dict) -> float:
    """Full match score: state distance + time-of-day - weekday bonus."""
    base = distance(features(current), features(candidate))
    temporal = 0.5 * time_dist(current.get("sim_min"), candidate.get("sim_min"))
    bonus = 0.0
    wd = current.get("weekday", "?")
    if wd != "?" and candidate.get("weekday", "?") == wd:
        bonus = SAME_WEEKDAY_BONUS
    return max(base + temporal - bonus, 0.0)


def _format(candidate: dict) -> dict:
    """Project a candidate row onto the PastCase schema keys."""
    return {
        "shelf_est": int(candidate.get("shelf_est", 0)),
        "effective_capacity": int(
            candidate.get("effective_cap", candidate.get("effective_capacity", 1)) or 1
        ),
        "boh": int(candidate.get("boh", 0)),
        "velocity_30m": float(candidate.get("velocity_30m", 0.0)),
        "velocity_120m": float(candidate.get("velocity_120m", 0.0)),
        "weekday": str(candidate.get("weekday", "?")),
        "needs_restock": bool(candidate.get("needs_restock", False)),
        "outcome": candidate.get("outcome", "suppressed_ok"),
        "rationale": str(candidate.get("rationale", ""))[:200],
    }


def pick_cases(current: dict, candidates: list[dict], k: int) -> list[dict]:
    """Select up to k precedent cases for the current state.

    Diversity first: closest GOOD outcome + closest BAD outcome (a success
    and a mistake teach the boundary); a third slot, if configured, goes to
    the next-closest remaining row. k=1 returns the closest overall.
    Closeness is match_distance: state + time-of-day, same-weekday bonus.
    Cold start (no candidates) returns [] — the static prompt few-shots
    carry the decision alone.
    """
    if k <= 0 or not candidates:
        return []
    scored = sorted(
        ((match_distance(current, c), i) for i, c in enumerate(candidates)),
        key=lambda t: t[0],
    )
    order = [candidates[i] for _, i in scored]
    if k == 1:
        return [_format(order[0])]
    good = next((c for c in order if c.get("outcome") in GOOD_OUTCOMES), None)
    bad = next((c for c in order if c.get("outcome") in BAD_OUTCOMES), None)
    picked: list[dict] = []
    for c in (good, bad):
        if c is not None and len(picked) < k:
            picked.append(c)
    if not picked:  # unknown outcome vocabulary — fall back to similarity
        picked = order[:k]
    else:
        for c in order:
            if len(picked) >= k:
                break
            if c not in picked:
                picked.append(c)
    return [_format(c) for c in picked[:k]]
