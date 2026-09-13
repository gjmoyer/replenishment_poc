"""M1 acceptance: deterministic rules. Run: uv run pytest tests/test_rules.py -q."""

import pytest

from decision.rules import (
    ShelfState,
    cases_needed,
    cover_min,
    effective_capacity,
    evaluate,
    evaluate_truck,
)
from decision.state import MutableShelf


def std(**kw):
    base = dict(
        boh=100,
        shelf_est=30,
        shelf_capacity_units=30,
        case_size=6,
        threshold_pct=0.35,
    )
    base.update(kw)
    return ShelfState(**base)


# --- calculator ---


def test_cases_needed_full_refill():
    # 18/72 units, case 12 -> ceil(54/12) = 5
    assert cases_needed(18, 72, 12, 60) == 5


def test_cases_needed_full_shelf_is_zero():
    assert cases_needed(72, 72, 12, 60) == 0
    assert cases_needed(80, 72, 12, 60) == 0


def test_cases_needed_clamped_to_zero_when_boh_covers_no_case():
    assert cases_needed(18, 72, 12, 10) == 0  # floor(10/12) = 0


def test_cases_needed_partial_clamp():
    assert cases_needed(18, 72, 12, 30) == 2  # need 5, BOH covers 2


def test_cases_needed_rejects_bad_inputs():
    with pytest.raises(ValueError):
        cases_needed(10, 72, 0, 60)
    with pytest.raises(ValueError):
        cases_needed(10, 72, 6, -1)
    with pytest.raises(ValueError):
        cases_needed(-3, 72, 6, 60)


def test_state_rejects_nonsense():
    with pytest.raises(ValueError):
        std(boh=-1)
    with pytest.raises(ValueError):
        std(shelf_est=-1)
    with pytest.raises(ValueError):
        std(case_size=0)
    with pytest.raises(ValueError):
        std(threshold_pct=1.5)
    with pytest.raises(ValueError):
        std(velocity_30m=-0.1)


def test_effective_capacity_promo_adds_half():
    assert effective_capacity(48, True) == 72  # 48 + round(24)
    assert effective_capacity(30, False) == 30
    with pytest.raises(ValueError):
        effective_capacity(0, False)


def test_cover_min_stalled_is_inf():
    assert cover_min(5, 0.0) == float("inf")
    assert cover_min(6, 0.5) == pytest.approx(12.0)


# --- normal ---


def test_normal_fires_below_threshold():
    d = evaluate(std(shelf_est=9), now_min=600)  # 9/30 = 30% < 35%
    assert d.action == "task"
    assert d.reason_code == "normal_low"
    assert d.cases == 4  # ceil(21/6)


def test_normal_quiet_at_and_above_threshold():
    assert evaluate(std(shelf_est=11), now_min=600).reason_code == "above_threshold"  # 36.7%
    d = evaluate(std(shelf_est=10, shelf_capacity_units=20), now_min=600)  # exactly 50% vs 35%
    assert d.action == "no_action"


def test_normal_boh_constrained_suppresses():
    d = evaluate(std(shelf_est=5, boh=2), now_min=600)  # need cases, BOH covers 0
    assert d.action == "suppress"
    assert d.reason_code == "boh_constrained"


def test_open_task_supersedes_instead_of_duplicates():
    d = evaluate(std(shelf_est=5, has_open_task=True), now_min=600)
    assert d.action == "supersede"
    assert d.reason_code == "normal_low"
    assert d.cases == 5  # ceil(25/6), BOH covers


# --- promo (endcap 1.5x) ---


def test_promo_uses_effective_capacity_not_shelf():
    # shelf_est 20 vs shelf 48 alone = 42% (naive "healthy"), but vs
    # effective 72 = 28% < 35% -> task. Proves endcap is in the math.
    d = evaluate(
        std(
            shelf_est=20,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=30,
            velocity_30m=0.5,
            velocity_120m=0.5,
        ),
        now_min=600,
    )
    assert d.action == "task"
    assert d.cases == 2  # need 5, BOH covers 2


def test_promo_guard_suppresses_when_endcap_likely_full():
    # low pct, but BOH high and no spike -> suppress + LLM candidate
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=200,
            velocity_30m=0.5,
            velocity_120m=0.5,
        ),
        now_min=600,
    )
    assert d.action == "suppress"
    assert d.reason_code == "promo_endcap_likely"
    assert d.llm_candidate is True
    assert d.llm_trigger == "promo_ambiguous"


def test_promo_single_sale_is_not_a_spike():
    # v120 == 0, v30 == 0.03 (one sale): ratio infinite, but below floor.
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=200,
            velocity_30m=0.03,
            velocity_120m=0.0,
        ),
        now_min=600,
    )
    assert d.action == "suppress"
    assert d.reason_code == "promo_endcap_likely"


def test_promo_exact_double_is_not_a_spike():
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=200,
            velocity_30m=1.0,
            velocity_120m=0.5,
        ),
        now_min=600,
    )
    assert d.action == "suppress"  # strict > required


def test_promo_guard_passes_on_low_boh():
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=20,
            velocity_30m=0.5,
            velocity_120m=0.5,
        ),
        now_min=600,
    )
    assert d.action == "task"
    assert d.reason_code == "promo_low"


def test_promo_guard_passes_on_velocity_spike():
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=200,
            velocity_30m=1.8,
            velocity_120m=0.5,
        ),
        now_min=600,
    )
    assert d.action == "task"
    assert d.reason_code == "promo_low"


# --- bulk (dog food) ---


def bulk(**kw):
    base = dict(
        boh=20,
        shelf_est=2,
        shelf_capacity_units=6,
        case_size=2,
        is_bulk=True,
        last_task_min=None,
    )
    base.update(kw)
    return ShelfState(**base)


def test_bulk_fires_when_all_gates_pass():
    d = evaluate(bulk(), now_min=600)
    assert d.action == "task"
    assert d.reason_code == "bulk_due"
    assert d.cases == 2  # ceil(4/2)


def test_bulk_suppressed_by_debounce_proves_no_thrash():
    d = evaluate(bulk(last_task_min=580), now_min=600)  # 20 min < 75
    assert d.action == "suppress"
    assert d.reason_code == "bulk_debounced"


def test_bulk_eligible_exactly_at_debounce_edge():
    d = evaluate(bulk(last_task_min=525), now_min=600)  # age exactly 75
    assert d.action == "task"
    assert d.reason_code == "bulk_due"


def test_bulk_suppressed_above_min_units_pct_ignored():
    # threshold 80% so 4/6 units enters the bulk branch, then floor-2 gate holds.
    d = evaluate(bulk(shelf_est=4, threshold_pct=0.8), now_min=600)
    assert d.action == "suppress"
    assert d.reason_code == "bulk_min_units"


def test_bulk_open_task_supersedes_per_idempotency_rule():
    d = evaluate(bulk(has_open_task=True), now_min=600)
    assert d.action == "supersede"
    assert d.reason_code == "bulk_due"
    assert d.cases == 2


# --- empty shelf: split by where the goods are ---


def test_zero_with_stock_tasks_immediately_no_truck_needed():
    d = evaluate(std(shelf_est=0), now_min=600)  # boh=100 covers
    assert d.action == "task"
    assert d.reason_code == "zero_fetch"
    assert d.cases == 5  # ceil(30/6), BOH covers
    assert d.llm_candidate is False  # unambiguous: fetch


def test_zero_with_stock_bulk_tasks_despite_debounce():
    d = evaluate(
        ShelfState(
            boh=20,
            shelf_est=0,
            shelf_capacity_units=6,
            case_size=2,
            is_bulk=True,
            last_task_min=590,
        ),
        now_min=600,
    )
    assert d.action == "task"
    assert d.reason_code == "zero_fetch"


def test_zero_with_stock_promo_tasks_rule_direct():
    d = evaluate(
        std(shelf_est=0, shelf_capacity_units=48, case_size=12, is_promo=True, boh=200),
        now_min=600,
    )
    assert d.action == "task"
    assert d.reason_code == "zero_fetch"
    assert d.llm_candidate is False


def test_zero_open_task_supersedes_not_duplicates():
    d = evaluate(std(shelf_est=0, has_open_task=True), now_min=600)
    assert d.action == "supersede"
    assert d.reason_code == "zero_fetch"


def test_zero_empty_building_emits_single_marker():
    d = evaluate(std(shelf_est=0, boh=2), now_min=600)  # case 6: BOH covers 0
    assert d.action == "zero_marker"
    assert d.reason_code == "zero_open"


def test_zero_empty_repeat_does_not_busy_loop_or_spam_llm():
    d = evaluate(std(shelf_est=0, boh=2, zero_flag=True, zero_since_min=590), now_min=600)
    assert d.action == "no_action"
    assert d.reason_code == "zero_waiting"
    assert d.llm_candidate is False  # only 10 min old


def test_zero_empty_stale_routes_to_llm():
    d = evaluate(std(shelf_est=0, boh=2, zero_flag=True, zero_since_min=500), now_min=600)
    assert d.action == "no_action"
    assert d.llm_candidate is True
    assert d.llm_trigger == "stale_zero"


def test_truck_zero_resolves_with_cases():
    d = evaluate_truck(
        zero_flag=True,
        boh=24,
        shelf_est=0,
        effective_cap=24,
        case_size=6,
    )
    assert d.action == "task"
    assert d.reason_code == "truck_zero"
    assert d.cases == 4


def test_truck_before_receipt_emits_check_not_task():
    d = evaluate_truck(
        zero_flag=True,
        boh=0,
        shelf_est=0,
        effective_cap=24,
        case_size=6,
    )
    assert d.action == "check"
    assert d.cases == 0


def test_truck_ignores_healthy_shelf_even_at_boh_zero():
    d = evaluate_truck(
        zero_flag=False,
        boh=0,
        shelf_est=20,
        effective_cap=24,
        case_size=6,
    )
    assert d.action == "no_action"
    assert d.reason_code == "truck_not_needed"


def test_truck_noop_when_healthy():
    d = evaluate_truck(
        zero_flag=False,
        boh=50,
        shelf_est=20,
        effective_cap=24,
        case_size=6,
    )
    assert d.action == "no_action"
    assert d.reason_code == "truck_not_needed"


# --- state evolution ---


def test_sale_decrements_shelf_and_stamps_zero():
    m = MutableShelf(boh=10, shelf_est=3, effective_cap=24, case_size=6)
    assert m.apply_boh_update(7, 600) == "sale"
    assert (m.boh, m.shelf_est) == (7, 0)
    assert m.zero_flag is True
    assert m.zero_since_min == 600


def test_sale_rejects_negative_boh():
    m = MutableShelf(boh=10, shelf_est=3, effective_cap=24, case_size=6)
    with pytest.raises(ValueError):
        m.apply_boh_update(-1, 600)


def test_receipt_does_not_autofill_shelf():
    m = MutableShelf(
        boh=7, shelf_est=0, zero_flag=True, zero_since_min=500, effective_cap=24, case_size=6
    )
    assert m.apply_boh_update(31, 600) == "receipt"
    assert (m.boh, m.shelf_est, m.zero_flag) == (31, 0, True)


def test_confirmation_refills_capped_and_clears_zero():
    m = MutableShelf(
        boh=31, shelf_est=0, zero_flag=True, zero_since_min=500, effective_cap=24, case_size=6
    )
    m.apply_confirmation(cases_fetched=10, now_min=600)  # 60 units, cap 24
    assert m.shelf_est == 24
    assert m.zero_flag is False
    assert m.zero_since_min is None
    assert m.last_task_min == 600


def test_confirmation_rejects_negative_cases():
    m = MutableShelf(boh=31, shelf_est=5, effective_cap=24, case_size=6)
    with pytest.raises(ValueError):
        m.apply_confirmation(cases_fetched=-1, now_min=600)


# --- cover-aware early trigger (fast movers) ---


def test_cover_trigger_fires_above_threshold():
    # shelf 12/24 = 50% (healthy) but velocity 0.5 -> cover 24 < 45 trigger.
    d = evaluate(
        std(shelf_est=12, velocity_30m=0.5, velocity_120m=0.4, cover_trigger_min=45),
        now_min=600,
    )
    assert d.action == "task"
    assert d.reason_code == "normal_low"
    assert d.cases >= 1


def test_cover_trigger_disabled_preserves_legacy():
    d = evaluate(
        std(shelf_est=12, velocity_30m=0.5, velocity_120m=0.4, cover_trigger_min=None),
        now_min=600,
    )
    assert d.action == "no_action"
    assert d.reason_code == "above_threshold"


def test_cover_trigger_ignores_stalled_velocity():
    d = evaluate(
        std(shelf_est=12, velocity_30m=0.0, velocity_120m=0.4, cover_trigger_min=45),
        now_min=600,
    )
    assert d.reason_code == "above_threshold"


def test_cover_trigger_rejects_nonpositive():
    with pytest.raises(ValueError):
        std(cover_trigger_min=0)


def test_bulk_exempt_from_cover_trigger():
    # bulk shelf 4 > floor 2: pct/cover ignored, still suppressed.
    d = evaluate(
        ShelfState(
            boh=20,
            shelf_est=4,
            shelf_capacity_units=6,
            case_size=2,
            is_bulk=True,
            threshold_pct=0.8,
            velocity_30m=0.5,
            velocity_120m=0.4,
            cover_trigger_min=45,
        ),
        now_min=600,
    )
    assert d.action == "suppress"
    assert d.reason_code == "bulk_min_units"


def test_promo_cover_critical_bypasses_endcap_guard():
    # High BOH + no spike would suppress, but cover 18 < 45 forces a task.
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=200,
            velocity_30m=1.0,
            velocity_120m=0.9,
            cover_trigger_min=45,
        ),
        now_min=600,
    )
    assert d.action == "task"
    assert d.reason_code == "promo_low"
    assert d.llm_candidate is True


def test_promo_guard_still_suppresses_when_cover_healthy():
    # Same high-BOH setup but slow velocity -> cover inf, guard holds.
    d = evaluate(
        std(
            shelf_est=18,
            shelf_capacity_units=48,
            case_size=12,
            is_promo=True,
            boh=200,
            velocity_30m=0.05,
            velocity_120m=0.05,
            cover_trigger_min=45,
        ),
        now_min=600,
    )
    assert d.action == "suppress"
    assert d.reason_code == "promo_endcap_likely"


# --- restock priority tiers (doc/02) ---


def test_priority_zero_shelf_is_p0():
    from decision.rules import priority_of

    assert priority_of(std(shelf_est=0), "zero_fetch") == 0
    assert priority_of(std(shelf_est=5), "truck_zero") == 0
    assert priority_of(std(shelf_est=0), "normal_low") == 0


def test_priority_critical_cover_is_p1():
    from decision.rules import priority_of

    d = std(shelf_est=12, velocity_30m=0.5, velocity_120m=0.4, cover_trigger_min=45)
    assert priority_of(d, "normal_low") == 1
    assert priority_of(d, "promo_low") == 1


def test_priority_routine_otherwise_and_bulk_exempt():
    from decision.rules import priority_of

    assert priority_of(std(shelf_est=9), "normal_low") == 2  # low, slow
    # Bulk at critical cover stays routine: debounce-gated by design.
    d = bulk(shelf_est=2, velocity_30m=0.5, velocity_120m=0.4, cover_trigger_min=45)
    assert priority_of(d, "bulk_due") == 2
