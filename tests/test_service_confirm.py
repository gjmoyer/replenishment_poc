"""Regression tests for the over-BOH confirmation bug (phantom shelf).

Live symptom was shelf_est=12 with BOH=8: a stale task confirmed more
cases than the backroom held, minting shelf from thin air and clearing
zero_flag. Guarded at three levels: UI clamp, service clamp, state
invariant. Run: uv run pytest tests/test_service_confirm.py -q
"""
from collections import deque
from unittest.mock import MagicMock

from core.config import get_config
from decision.service import Brain
from decision.state import MutableShelf

KEY = ("store-001", "soda-12pk-101")  # case_size 12


def make_brain(key=KEY, **shelf_kw):
    brain = Brain(get_config(), MagicMock(), MagicMock())
    params = dict(boh=8, shelf_est=0, effective_cap=72, case_size=12)
    params.update(shelf_kw)
    brain.shelf[key] = MutableShelf(**params)
    brain.sales_ts[key] = deque()
    return brain


def confirm_msg(cases):
    return {
        "task_id": "t1", "store_id": "store-001", "sku": "soda-12pk-101",
        "sim_ts": "2026-01-05T10:00", "action": "done", "cases_fetched": cases,
        "event_id": "e1",
    }


def test_over_boh_confirm_is_held_and_task_stays_open():
    brain = make_brain(boh=8, shelf_est=12)  # pre-existing phantom state
    brain.open[KEY] = {"task_id": "t1", "emit_min": 600, "cases": 4,
                       "reason": "normal_low", "shelf_at_emit": 0}
    brain.on_confirm(confirm_msg(4))  # BOH 8 covers 0 full cases of 12
    assert KEY in brain.open  # held, not popped
    brain.store.mark_done.assert_not_called()
    assert brain.shelf[KEY].shelf_est == 12  # untouched
    held = [c.args[4] for c in brain.store.log_event.call_args_list]
    assert any("held" in m for m in held)


def test_partial_confirm_clamps_to_boh_cover():
    brain = make_brain(boh=20, shelf_est=0)
    brain.open[KEY] = {"task_id": "t1", "emit_min": 600, "cases": 4,
                       "reason": "normal_low", "shelf_at_emit": 0}
    brain.on_confirm(confirm_msg(4))  # BOH 20 covers 1 case of 12
    assert KEY not in brain.open
    assert brain.shelf[KEY].shelf_est == 12  # min(72, 0+12, 20), not 48
    brain.store.mark_done.assert_called_once_with("t1", 600)


def test_state_invariant_survives_receipt_and_confirm():
    m = MutableShelf(boh=8, shelf_est=12, effective_cap=72, case_size=12)
    m.apply_boh_update(50, 700)  # receipt heals the phantom
    assert m.shelf_est <= m.boh
    m.apply_confirmation(10, 700)
    assert m.shelf_est <= m.boh
    assert m.shelf_est == 50


def truck_msg(manifest, sim_ts="2026-01-05T14:00", epoch=6):
    return {"store_id": "store-001", "sim_ts": sim_ts, "truck_id": "t",
            "manifest_skus": manifest, "epoch": epoch, "event_id": "trk"}


def test_truck_wakes_non_manifest_zero_with_stock():
    brain = make_brain(MILK, boh=50, shelf_est=0, effective_cap=24, case_size=6)
    brain.epoch = 6
    brain.shelf[MILK].zero_flag = True
    brain.shelf[MILK].zero_since_min = 800
    brain.on_truck(truck_msg(["pasta-001"]))  # milk NOT on manifest
    assert brain.store.add_task.called
    task = brain.store.add_task.call_args[0][0]
    assert task["sku"] == "milk-1gal-001"
    assert task["reason"] == "zero_fetch"
    assert task["source"] == "rule"


def test_truck_wake_leaves_empty_building_waiting():
    brain = make_brain(MILK, boh=0, shelf_est=0, effective_cap=24, case_size=6)
    brain.epoch = 6
    brain.shelf[MILK].zero_flag = True
    brain.shelf[MILK].zero_since_min = 800
    brain.on_truck(truck_msg(["pasta-001"]))
    brain.store.add_task.assert_not_called()


MILK = ("store-001", "milk-1gal-001")  # opening 180, cap 24, case 6


def sale_msg(**kw):
    base = {"store_id": "store-001", "sku": "milk-1gal-001",
            "sim_ts": "2026-01-05T10:00", "boh": 179, "delta": -1,
            "reason": "sale", "epoch": 6, "event_id": "s1"}
    base.update(kw)
    return base


def test_stale_epoch_message_dropped():
    brain = make_brain(MILK, boh=100, shelf_est=20, effective_cap=24, case_size=6)
    brain.epoch = 6
    brain.on_boh(sale_msg(epoch=5, boh=90, event_id="old"))
    assert brain.shelf[MILK].boh == 100  # untouched
    brain.store.upsert_shelf.assert_not_called()
    brain.store.add_task.assert_not_called()


def test_new_epoch_adopts_resets_and_processes():
    brain = make_brain(MILK, boh=100, shelf_est=20, effective_cap=24, case_size=6)
    brain.epoch = 5
    brain.on_boh(sale_msg(epoch=6, event_id="new"))
    assert brain.epoch == 6
    # reseeded to opening (180) then the sale applied: 179 / shelf 23
    assert brain.shelf[MILK].boh == 179
    assert brain.shelf[MILK].shelf_est == 23


def test_missing_epoch_processes_as_current():
    brain = make_brain(MILK, boh=100, shelf_est=20, effective_cap=24, case_size=6)
    brain.epoch = 6
    msg = sale_msg()
    del msg["epoch"]
    msg["event_id"] = "legacy"
    brain.on_boh(msg)
    assert brain.shelf[MILK].boh == 179
