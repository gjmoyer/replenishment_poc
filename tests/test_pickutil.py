"""Tests for truck-picker newcomer merging (the reported bug: a SKU hitting
zero later never appeared because Streamlit defaults apply once)."""

from dashboard.pickutil import merge_newcomers

ALL = ["beans-001", "eggs-12ct-005", "milk-1gal-001", "soda-12pk-101"]


def test_fresh_key_leaves_widget_to_default():
    pick, seen = merge_newcomers(None, [], ["eggs-12ct-005"], ALL)
    assert pick is None
    assert seen == ["eggs-12ct-005"]


def test_new_zero_appended_to_existing_pick():
    pick, seen = merge_newcomers(
        ["eggs-12ct-005"], ["eggs-12ct-005"], ["eggs-12ct-005", "soda-12pk-101"], ALL
    )
    assert pick == ["eggs-12ct-005", "soda-12pk-101"]
    assert seen == ["eggs-12ct-005", "soda-12pk-101"]


def test_manual_deselect_respected():
    pick, seen = merge_newcomers([], ["eggs-12ct-005"], ["eggs-12ct-005"], ALL)
    assert pick is None
    assert seen == ["eggs-12ct-005"]


def test_manual_pick_preserved_no_touch():
    pick, seen = merge_newcomers(["milk-1gal-001"], ["eggs-12ct-005"], ["eggs-12ct-005"], ALL)
    assert pick is None
    assert seen == ["eggs-12ct-005"]


def test_unknown_sku_never_added():
    pick, _ = merge_newcomers([], [], ["ghost-sku"], ALL)
    assert pick is None


def test_cleared_picker_rearms_on_change():
    # after dispatch-clear: picker [] with snapshot; fresh zero re-arms
    pick, _ = merge_newcomers([], ["eggs-12ct-005"], ["eggs-12ct-005", "milk-1gal-001"], ALL)
    assert pick == ["milk-1gal-001"]
