"""M2 acceptance as regression tests. Quiet, deterministic, fast (<5s)."""
import pytest

from sim.catalog import STORES
from sim.loop import DaySim, assert_acceptance
from sim.scenarios import ScenarioFlags


def run(seed=42, **flag_kw):
    flags = ScenarioFlags(**flag_kw)
    sim = DaySim(seed=seed, flags=flags, verbose=False)
    return sim, sim.run()


def test_m2_acceptance_seed_42():
    sim, summary = run()
    assert_acceptance(summary, sim.flags)
    assert summary.count("normal_low", 12 * 60) >= 3
    assert summary.count("promo_low") >= 1
    assert summary.count("bulk_due") <= 3


def test_deterministic_same_seed_same_tasks():
    _, a = run()
    _, b = run()
    ka = [(t.store_id, t.sku, t.emit_min, t.reason, t.cases) for t in a.tasks]
    kb = [(t.store_id, t.sku, t.emit_min, t.reason, t.cases) for t in b.tasks]
    assert ka == kb


def test_deterministic_across_processes():
    # Guards against salted-hash seeding (PYTHONHASHSEED): the demo must
    # replay identically in a fresh process, under DIFFERENT hash seeds.
    import subprocess
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]

    def replay(hashseed: str):
        import os

        env = dict(os.environ, PYTHONHASHSEED=hashseed)
        r = subprocess.run(
            ["uv", "run", "python", "-m", "sim.loop", "--seed", "42", "--quiet"],
            capture_output=True, text=True, cwd=root, env=env,
        )
        assert r.returncode == 0, r.stderr[-2000:]
        wanted = ("sales units", "by reason", "normal pre-noon")
        return [line for line in r.stdout.splitlines() if line.startswith(wanted)]

    assert replay("0") == replay("1")


def test_noon_boundary_task_counts_as_pre_noon():
    from sim.loop import DaySummary, TaskEvent

    s = DaySummary(seed=1, tasks=[
        TaskEvent("store-001", "milk-1gal-001", 720, "task", "normal_low", 2),
    ])
    assert s.count("normal_low", 720) == 1
    assert s.count("normal_low", 719) == 0


def test_velocity_windows_are_half_open():
    from collections import deque

    sim, _ = run()
    key = ("store-001", "milk-1gal-001")
    sim.sales_ts[key] = deque([569, 570, 571])  # now=600: 570,571 in [570,600)
    v30, _ = sim._velocities(key, 600)
    assert v30 == pytest.approx(2 / 30)
    sim.sales_ts[key] = deque([479, 480])  # 480 kept (>= now-120), 479 evicted
    _, v120 = sim._velocities(key, 600)
    assert v120 == pytest.approx(1 / 120)


def test_truck_streams_differ_per_store():
    from sim.truck import _seeded

    a = _seeded(42, "store-001", 630).randint(0, 10**9)
    b = _seeded(42, "store-002", 630).randint(0, 10**9)
    assert a != b


def test_bulk_thrash_stays_debounced():
    sim, summary = run(bulk_thrash=True)
    # 2h triple-demand window / 75-min debounce -> at most a handful
    assert summary.count("bulk_due") <= 4


def test_promo_rush_still_yields_promo_task():
    _, summary = run(promo_rush=True)
    assert summary.count("promo_low") >= 1


def test_silent_oos_rescued_only_after_truck():
    flags = ScenarioFlags(silent_oos=True)
    sim = DaySim(seed=42, flags=flags, verbose=False)
    summary = sim.run()
    early = [t for t in summary.tasks
             if t.sku == flags.silent_sku
             and flags.silent_drain_min <= t.emit_min < 14 * 60]
    assert early == []
    assert any(t.sku == flags.silent_sku and t.reason == "truck_zero"
               and t.emit_min >= 14 * 60 for t in summary.tasks)


def test_two_stores_diverge():
    _, summary = run()
    per_store = {}
    for t in summary.tasks:
        per_store.setdefault(t.store_id, 0)
        per_store[t.store_id] += 1
    assert set(per_store) == {s.store_id for s in STORES}
    assert per_store["store-001"] != per_store["store-002"]


def test_lost_sales_accounting_balances():
    from sim.loop import recoverable_gap

    _, summary = run()
    lost = sum(summary.lost_gap.values()) + sum(summary.lost_empty.values())
    assert summary.demand_units == summary.sales_units + lost
    assert sum(summary.lost_gap.values()) > 0  # shelf gaps must occur to demo
    rec = recoverable_gap(summary)
    assert sum(rec.values()) <= sum(summary.lost_gap.values())
    assert all(v >= 0 for v in rec.values())


def test_receipt_waves_target_lowest_days_of_supply():
    """Waves must prefer fast movers (milk) over absolute-low slow movers."""
    from collections import Counter

    from sim.loop import DaySim

    sim = DaySim(seed=42, flags=ScenarioFlags(), verbose=False)
    receipts: Counter = Counter()
    orig = sim._apply_receipt

    def counting(store_id, sku, now, _o=orig):
        receipts[(store_id, sku)] += 1
        return _o(store_id, sku, now)

    sim._apply_receipt = counting
    sim.run()
    # Milk (popularity 20, highest demand) must get at least one receipt;
    # with days-of-supply targeting it is no longer starved by absolute-BOH
    # ranking (baseline gave store-002 soda zero receipts).
    assert receipts[("store-001", "milk-1gal-001")] >= 1
    assert receipts[("store-002", "milk-1gal-001")] >= 1


def test_check_confirm_closes_sibling_open_task_as_done():
    """Check maturing first must not orphan the open task into abandon."""
    from sim.loop import DaySim

    sim = DaySim(seed=42, flags=ScenarioFlags(), verbose=False)
    key = ("store-001", "eggs-12ct-005")
    # Recreate the race: check (emit 840) + task (emit 850), receipt landed.
    m = sim.shelf[key]
    m.apply_boh_update(48, 850)
    m.zero_flag = True
    m.zero_since_min = 780
    sim.checks[key] = {"emit_min": 840, "reason": "truck_zero"}
    sim.open[key] = {"emit_min": 850, "cases": 3, "reason": "zero_fetch"}
    sim.last_task_emit[key] = 850
    aband_before = sim.summary.abandoned_count
    sim._stage_confirm(865 + 25)  # both matured; check confirms first
    assert key not in sim.open
    assert key not in sim.checks
    assert sim.summary.abandoned_count == aband_before
    assert sim.shelf[key].shelf_est == 36
