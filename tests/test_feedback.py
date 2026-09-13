"""Feedback loop: outcome labels + service wiring (doc/04).

Pure verdict helpers plus Brain wiring (link calls to tasks, label outcomes
at confirm/abandon). Run: uv run pytest tests/test_feedback.py -q
"""
from collections import deque
from unittest.mock import MagicMock

from core.config import get_config
from decision.feedback import (
    classify_confirm,
    override_rate,
    regret_rate,
    regret_window,
    suppress_verdict,
)
from decision.service import Brain

KEY = ("store-001", "soda-12pk-101")  # case_size 12


# --- pure verdicts ---


def test_classify_confirm_done_adjusted_rejected():
    assert classify_confirm(4, 4, "done") == "done"
    assert classify_confirm(4, 2, "done") == "adjusted"  # associate -1/-1
    assert classify_confirm(4, 6, "done") == "adjusted"  # associate +2
    assert classify_confirm(4, 4, "reject") == "rejected"
    assert classify_confirm(4, 0, "reject") == "rejected"


def test_regret_window_clamped():
    assert regret_window(0) == 30
    assert regret_window(45) == 45
    assert regret_window(500) == 90


def test_suppress_verdict():
    assert suppress_verdict(0) == "suppressed_ok"
    assert suppress_verdict(3) == "suppressed_regret"


def test_override_and_regret_rates():
    assert override_rate(8, 1, 1) == 0.2
    assert override_rate(0, 0, 0) is None
    assert regret_rate(1, 4) == 0.25
    assert regret_rate(0, 0) is None


# --- service wiring (mocked store) ---


def make_brain(**shelf_kw):
    from decision.state import MutableShelf

    brain = Brain(get_config(), MagicMock(), MagicMock())
    params = dict(boh=50, shelf_est=0, effective_cap=72, case_size=12)
    params.update(shelf_kw)
    brain.shelf[KEY] = MutableShelf(**params)
    brain.sales_ts[KEY] = deque()
    brain.epoch = 6
    return brain


def confirm_msg(**kw):
    base = {"task_id": "t1", "store_id": "store-001", "sku": "soda-12pk-101",
            "sim_ts": "2026-01-05T10:00", "action": "done", "cases_fetched": 4,
            "event_id": "e1", "epoch": 6}
    base.update(kw)
    return base


def test_confirm_done_labels_llm_outcome():
    brain = make_brain()
    brain.open[KEY] = {"task_id": "t1", "emit_min": 600, "cases": 4,
                       "reason": "promo_low", "shelf_at_emit": 0}
    brain.on_confirm(confirm_msg())
    brain.store.set_llm_outcome.assert_called_once_with("t1", "done", 600)


def test_confirm_adjusted_when_associate_changes_cases():
    brain = make_brain()
    brain.open[KEY] = {"task_id": "t1", "emit_min": 600, "cases": 4,
                       "reason": "promo_low", "shelf_at_emit": 0}
    brain.on_confirm(confirm_msg(cases_fetched=2, event_id="e2"))
    brain.store.set_llm_outcome.assert_called_once_with("t1", "adjusted", 600)


def test_confirm_reject_labels_llm_outcome():
    brain = make_brain()
    brain.open[KEY] = {"task_id": "t1", "emit_min": 600, "cases": 4,
                       "reason": "promo_low", "shelf_at_emit": 0}
    brain.on_confirm(confirm_msg(action="reject", cases_fetched=0, event_id="e3"))
    brain.store.set_llm_outcome.assert_called_once_with("t1", "rejected", 600)


def test_abandon_labels_llm_outcome():
    brain = make_brain()
    brain.open[KEY] = {"task_id": "t1", "emit_min": 600, "cases": 4,
                       "reason": "promo_low", "shelf_at_emit": 0}
    brain._last_pg_sweep = 10**12  # throttle the PG backstop path
    brain.sweep_timeouts(720)  # age 120 >= open_timeout 120
    brain.store.set_llm_outcome.assert_called_once_with("t1", "abandoned", 720)


def test_apply_routed_links_task_to_call(monkeypatch):
    import decision.service as svc
    from decision.router import RoutedOutcome
    from decision.rules import Decision

    brain = make_brain(boh=60, shelf_est=18)
    record = {"store_id": "store-001", "sku": "soda-12pk-101",
              "trigger": "promo_ambiguous", "model": "m", "prompt_version": "v1",
              "input_hash": "abc", "latency_ms": 5, "fallback": False,
              "output": {"needs_restock": True, "cases_override": None,
                         "confidence": 0.8, "rationale": "r",
                         "suppress_until_min": 0, "missing_data": []}}
    monkeypatch.setattr(
        svc, "route",
        lambda *a, **k: (RoutedOutcome(action="task", reason_code="promo_low",
                                       cases=4, source="llm",
                                       rationale="r", confidence=0.8), record),
    )
    from decision.router import ReasonContext

    ctx = ReasonContext(
        store_id="store-001", sku="soda-12pk-101", sim_ts="2026-01-05T10:00",
        boh=60, shelf_est=18, effective_cap=72, case_size=12,
        is_promo=True, is_bulk=False, threshold_pct=0.35,
        velocity_30m=1.8, velocity_120m=0.6, trigger="promo_ambiguous",
        recent_sales=[], open_task=None, truck_eta=None)
    brain.apply_routed(
        KEY, 600,
        Decision(action="task", reason_code="promo_low", cases=4, detail="d",
                 llm_candidate=True, llm_trigger="promo_ambiguous"),
        ctx)
    saved = brain.store.add_llm_call.call_args[0][0]
    assert saved["sim_min"] == 600
    assert saved["epoch"] == 6
    assert saved["needs_restock"] is True
    assert saved["input"]["shelf_est"] == 18
    assert saved["input"]["rule_cases"] == 4
    assert saved["input"]["sim_min"] == 600
    assert saved["input"]["weekday"] == "Mon"  # 2026-01-05T10:00 sim_ts
    task_id = brain.open[KEY]["task_id"]
    brain.store.link_llm_task.assert_called_once_with(
        brain.store.add_llm_call.return_value, task_id)


def test_emit_task_persists_priority():
    from decision.router import RoutedOutcome

    brain = make_brain(boh=50, shelf_est=0)  # empty shelf, stocked -> P0
    tid = brain.emit_task(
        KEY, 600,
        RoutedOutcome(action="task", reason_code="zero_fetch", cases=3,
                      source="rule", rationale="r"), "zero_fetch")
    assert isinstance(tid, str)
    saved = brain.store.add_task.call_args[0][0]
    assert saved["priority"] == 0
