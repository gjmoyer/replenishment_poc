"""Outcome-aware few-shots: retrieval scoring + service wiring (doc/04 #2).

Run: uv run pytest tests/test_history.py -q
"""

import pytest

from decision.history import distance, features, pick_cases


def cand(shelf=18, cap=72, boh=60, v30=1.8, v120=0.6, restock=True, outcome="done", rationale="r"):
    return {
        "shelf_est": shelf,
        "effective_cap": cap,
        "boh": boh,
        "velocity_30m": v30,
        "velocity_120m": v120,
        "needs_restock": restock,
        "outcome": outcome,
        "rationale": rationale,
    }


CUR = {"shelf_est": 18, "effective_cap": 72, "boh": 60, "velocity_30m": 1.8, "velocity_120m": 0.6}


def test_features_scale_free_and_capped():
    pct, cover, ratio = features(CUR)
    assert pct == pytest.approx(0.25)
    assert cover == pytest.approx(60 / 72)
    assert ratio == pytest.approx(3.0)
    assert (
        features(
            {
                "shelf_est": 0,
                "effective_cap": 72,
                "boh": 0,
                "velocity_30m": 2.0,
                "velocity_120m": 0.0,
            }
        )[2]
        == 5.0
    )


def test_distance_prefers_emptiness():
    near = features(CUR)  # (0.25, 0.833, 3.0)
    same_pct = (0.25, 1.5, 3.0)  # same emptiness, deeper backroom
    diff_pct = (0.9, 60 / 72, 3.0)  # full shelf, same cover
    assert distance(near, same_pct) < distance(near, diff_pct)


def test_pick_diversifies_good_and_bad():
    cands = [
        cand(outcome="done", rationale="good-near"),
        cand(shelf=19, outcome="done", rationale="good-near2"),
        cand(shelf=17, outcome="suppressed_regret", rationale="bad-near"),
        cand(shelf=50, cap=72, outcome="done", rationale="good-far"),
    ]
    picked = pick_cases(CUR, cands, 2)
    assert [p["outcome"] for p in picked] == ["done", "suppressed_regret"]
    assert picked[0]["rationale"] == "good-near"


def test_pick_single_returns_closest():
    cands = [
        cand(shelf=50, cap=72, outcome="done", rationale="far"),
        cand(outcome="suppressed_regret", rationale="near"),
    ]
    assert pick_cases(CUR, cands, 1)[0]["rationale"] == "near"


def test_pick_third_slot_goes_to_next_closest():
    cands = [
        cand(outcome="done", rationale="g1"),
        cand(shelf=19, outcome="suppressed_regret", rationale="b1"),
        cand(shelf=20, outcome="done", rationale="g2"),
        cand(shelf=60, cap=72, outcome="done", rationale="far"),
    ]
    picked = pick_cases(CUR, cands, 3)
    assert [p["rationale"] for p in picked] == ["g1", "b1", "g2"]


def test_pick_cold_start_and_zero_k():
    assert pick_cases(CUR, [], 2) == []
    assert pick_cases(CUR, [cand()], 0) == []


def test_pick_projects_past_case_schema():
    picked = pick_cases(CUR, [cand(rationale="x" * 500)], 1)[0]
    assert set(picked) == {
        "shelf_est",
        "effective_capacity",
        "boh",
        "velocity_30m",
        "velocity_120m",
        "weekday",
        "needs_restock",
        "outcome",
        "rationale",
    }
    assert len(picked["rationale"]) == 200
    assert picked["weekday"] == "?"  # legacy snapshots predate the field


def test_past_case_schema_truncates_and_validates():
    from llm.schemas import PastCase, ReasonRequest

    p = PastCase.model_validate({**pick_cases(CUR, [cand()], 1)[0]})
    assert p.outcome == "done"
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        PastCase.model_validate({**pick_cases(CUR, [cand()], 1)[0], "outcome": "maybe"})
    req = ReasonRequest.model_validate(
        {
            "store_id": "s",
            "sku": "k",
            "sim_ts": "2026-01-05T10:00",
            "state": {
                "boh": 60,
                "shelf_est": 18,
                "effective_capacity": 72,
                "velocity_30m": 1.8,
                "velocity_120m": 0.6,
            },
            "product": {"is_promo": True, "case_size": 12},
            "trigger": "promo_ambiguous",
            "past_cases": [p.model_dump()],
        }
    )
    assert len(req.past_cases) == 1
    assert (
        ReasonRequest.model_validate(
            {k: v for k, v in req.model_dump().items() if k != "past_cases"}
        ).past_cases
        == []
    )


def test_config_history_defaults_and_bounds(monkeypatch):
    from core.config import load

    monkeypatch.delenv("LLM_HISTORY_CASES", raising=False)
    cfg = load()
    assert cfg.llm.history_cases == 2
    assert cfg.llm.history_pool == 20
    assert cfg.llm.prompt_version == "v2"
    monkeypatch.setenv("LLM_HISTORY_CASES", "9")
    with pytest.raises(ValueError):
        load()


def _ctx(**kw):
    from decision.router import ReasonContext

    base = dict(
        store_id="store-001",
        sku="soda-12pk-101",
        sim_ts="2026-01-05T10:00",
        boh=60,
        shelf_est=18,
        effective_cap=72,
        case_size=12,
        is_promo=True,
        is_bulk=False,
        threshold_pct=0.35,
        velocity_30m=1.8,
        velocity_120m=0.6,
        trigger="promo_ambiguous",
        recent_sales=[],
        open_task=None,
        truck_eta=None,
    )
    base.update(kw)
    return ReasonContext(**base)


def _brain():
    from collections import deque
    from unittest.mock import MagicMock

    from core.config import get_config
    from decision.service import Brain
    from decision.state import MutableShelf

    brain = Brain(get_config(), MagicMock(), MagicMock())
    brain.shelf[("store-001", "soda-12pk-101")] = MutableShelf(
        boh=60, shelf_est=18, effective_cap=72, case_size=12
    )
    brain.sales_ts[("store-001", "soda-12pk-101")] = deque()
    brain.epoch = 6
    return brain


def test_history_for_selects_from_store_rows():
    brain = _brain()
    brain.store.recent_labeled_calls.return_value = [
        cand(outcome="done", rationale="worked"),
        cand(shelf=17, outcome="suppressed_regret", rationale="oops"),
        cand(shelf=60, cap=72, outcome="done", rationale="far"),
    ]
    picked = brain._history_for(_ctx(), 600)
    assert [p["outcome"] for p in picked] == ["done", "suppressed_regret"]
    brain.store.recent_labeled_calls.assert_called_once_with(
        "promo_ambiguous", brain.cfg.llm.history_pool
    )


def test_history_for_never_raises():
    brain = _brain()
    brain.store.recent_labeled_calls.side_effect = RuntimeError("db down")
    assert brain._history_for(_ctx(), 600) == []


def test_route_attaches_history_on_cache_miss_only(monkeypatch):
    import decision.service as svc
    from decision.router import RoutedOutcome
    from decision.rules import Decision

    brain = _brain()
    seen = {}

    def fake_route(decision, ctx, now, cache, timeout):
        seen["past"] = list(ctx.past_cases)
        return RoutedOutcome(
            action="suppress",
            reason_code="promo_endcap_likely",
            cases=0,
            source="llm",
            rationale="r",
        ), None

    monkeypatch.setattr(svc, "route", fake_route)
    brain.store.recent_labeled_calls.return_value = [cand(outcome="done")]
    ctx = _ctx()
    brain._route_with_history(
        Decision(
            action="suppress",
            reason_code="promo_endcap_likely",
            detail="d",
            llm_candidate=True,
            llm_trigger="promo_ambiguous",
        ),
        ctx,
        600,
    )
    assert len(seen["past"]) == 1  # cache miss -> retrieved

    brain.store.recent_labeled_calls.reset_mock()
    ctx2 = _ctx()  # identical bucket -> cache... nothing cached yet
    # Seed the cache through a real route call is complex; instead assert the
    # skip path directly: pre-populate the bucket.
    from decision.router import LlmCache

    assert isinstance(brain.cache, LlmCache)
    brain.cache.put(
        ctx2,
        600,
        RoutedOutcome(
            action="suppress", reason_code="x", cases=0, source="llm", rationale="cached"
        ),
    )
    brain._route_with_history(
        Decision(
            action="suppress",
            reason_code="x",
            detail="d",
            llm_candidate=True,
            llm_trigger="promo_ambiguous",
        ),
        _ctx(),
        600,
    )
    brain.store.recent_labeled_calls.assert_not_called()  # cache hit: no SELECT


def test_call_reasoner_posts_past_cases(monkeypatch):
    import httpx

    import decision.router as router

    captured = {}

    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"decision": {}, "meta": {}}

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def post(self, url, json):
            captured["body"] = json
            return FakeResp()

    monkeypatch.setattr(httpx, "Client", FakeClient)
    ctx = _ctx()
    ctx.past_cases = pick_cases(CUR, [cand(outcome="done")], 1)
    router.call_reasoner(ctx, 5.0)
    assert len(captured["body"]["past_cases"]) == 1
    assert captured["body"]["past_cases"][0]["outcome"] == "done"


# --- temporal matching: time-of-day + weekday ---


def test_weekday_of_parses_sim_ts():
    from decision.history import weekday_of

    assert weekday_of("2026-01-05T09:14:00") == "Mon"  # SIM_DATE is a Monday
    assert weekday_of("2026-01-10T18:00:00") == "Sat"
    assert weekday_of("garbage") == "?"
    assert weekday_of("") == "?"


def test_time_dist_is_circular_and_none_safe():
    from decision.history import time_dist

    assert time_dist(600, 600) == 0.0
    assert time_dist(0, 720) == pytest.approx(0.5)  # 12h apart = max
    # 23:00 vs 01:00 are 2h apart, not 22h
    assert time_dist(1380, 60) == pytest.approx(2 / 24)
    assert time_dist(None, 600) == 0.0
    assert time_dist(600, None) == 0.0


def test_pick_prefers_same_time_of_day():
    from decision.history import match_distance

    cur = {**CUR, "sim_min": 1100}  # 18:20 evening rush
    evening = {**cand(outcome="done"), "sim_min": 1090}
    morning = {**cand(outcome="done"), "sim_min": 540}
    assert match_distance(cur, evening) < match_distance(cur, morning)


def test_pick_same_weekday_bonus_flips_tie():
    cur = {**CUR, "sim_min": 1100, "weekday": "Sat"}
    same_day = {**cand(outcome="done", shelf=19), "sim_min": 1100, "weekday": "Sat"}
    other_day = {**cand(outcome="done", shelf=19), "sim_min": 1100, "weekday": "Tue"}
    picked = pick_cases(cur, [other_day, same_day], 1)
    assert picked[0]["weekday"] == "Sat"


def test_pick_weekday_bonus_is_soft_not_filter():
    # Sparse trigger: only Tuesday precedent exists for a Saturday case.
    # It must still be returned — a cross-day precedent beats none.
    cur = {**CUR, "weekday": "Sat"}
    picked = pick_cases(cur, [{**cand(outcome="done"), "weekday": "Tue"}], 1)
    assert len(picked) == 1
    assert picked[0]["weekday"] == "Tue"


def test_pick_legacy_rows_without_temporal_keys():
    # Pre-feature snapshots have no sim_min/weekday: neutral, no crash.
    picked = pick_cases({**CUR, "sim_min": 1100, "weekday": "Sat"}, [cand(outcome="done")], 1)
    assert picked[0]["outcome"] == "done"


def test_past_case_weekday_defaults_unknown():
    from llm.schemas import PastCase

    assert PastCase.model_validate({**pick_cases(CUR, [cand()], 1)[0]}).weekday == "?"
