# 09 — Simulation Realism & Calibration Handoff

Status as of 2026-09-13: **no real POS data has been provided.** Everything
below records what is general-knowledge, what was invented/demo-tuned, and
exactly which knobs to turn when calibration data arrives. Read this before
changing any demand parameter.

## Provenance legend

- `[KNOWLEDGE]` — general grocery-retail pattern, no store-specific data.
- `[TUNED]` — invented by the builder, adjusted until the demo behaved.
  Treat as placeholder, not truth.
- `[STRUCTURAL]` — modeling simplification no parameter fixes; needs code.

## Demand model (sim/shopper.py, sim/scenarios.py)

Day curve in baskets/min, × store rate_mult (Downtown 1.2, Suburb 0.9):

| Sim time | Rate | Provenance |
|----------|------|------------|
| 07:00–09:00 | 0.30 | [TUNED] shape [KNOWLEDGE] (slow open, ramp) |
| 09:00–12:00 | 0.60 | [TUNED] shape [KNOWLEDGE] (midday build) |
| 12:00–14:00 | 0.90 | [TUNED] shape [KNOWLEDGE] (lunch bump) |
| 14:00–17:00 | 0.70 | [TUNED] shape [KNOWLEDGE] (afternoon lull) |
| 17:00–19:00 | 1.10 | [TUNED] shape [KNOWLEDGE] (after-work peak) |
| 19:00–22:00 | 0.50 | [TUNED] shape [KNOWLEDGE] (taper to close) |

Absolute volumes are scaled ~5x below a real store so one day compresses
into ~15 watchable minutes at 60x. Do NOT compare units/hour to reality.

Basket: 1 line + 30% second + 10% third + 3% fourth (avg ~1.43) [TUNED].
Multi-unit lines (2 units): 15% for milk/soda only [TUNED].
Profile multipliers [TUNED shapes, daypart assignment KNOWLEDGE]:

- morning (bread, eggs, cereal, coffee): 2.5x before 10:00, 0.4x after
- evening (chips): 2.0x from 17:00, 0.7x before
- spiky (promo soda): 2.2x 17:00–19:00, 1.0x otherwise
- promo_rush scenario: soda 3x 18:00–20:00; bulk_thrash: dog food 3x
  10:00–12:00 (both [TUNED] demo scenarios, see `03`)

Popularity weights (relative line-pick; all [TUNED] for demo pacing):
milk 20, soda 14, bread 12, eggs 12, coffee 9, cereal 8, chips 7, pasta 6,
beans 5, dog food 0.8. Dog food was retuned 0.6 → 0.4 → 0.8 across builds
to land bulk tasks at 2/day base, ≤4 under thrash — that history is a
warning: these numbers serve the acceptance test, not reality.

## Supply model (sim/catalog.py, config/poc.yaml, sim/runner.py)

- Opening BOH (milk 180, soda 225, dogfood 30, bread 135, eggs 162,
  fillers 90–108): [TUNED]. Bumped 1.5x mid-build *because shelves kept
  starving* — fitted to the demo, not to any store.
- Scheduled receipts 08:00 + 15:00 (+12:00 spike scenario): 4 lowest-BOH
  SKUs × 4 cases [TUNED schedule-shaped guess].
- Trucks 10:30 + 14:00, manifest = zero-flag SKUs + 2 seeded fillers,
  dock-to-backroom receipts +5–15 min later [TUNED].
- Associate confirm delay 25 min flat, open-task timeout 120 min [TUNED].
  No labor capacity, prioritization, or shifts [STRUCTURAL].

## Known structural unrealisms (parameter-proof)

10 SKUs vs 30k+; no day-of-week/weather/payday/seasonality; no shrink,
mis-scans, or phantom inventory; no perishables/waste/expiry; endcap is a
static +50% never replenished itself; receipts materialize rather than
arriving via DC manifests with putaway labor; single basket-rate stream
(no customer-count × basket-size split).

## Calibration worksheet (for the domain expert)

Any subset improves the sim. Encode answers as noted; then run
`make replay` + `uv run pytest -q` and retune acceptance bounds in
`sim/loop.py::assert_acceptance` if the honest numbers break them
(that breakage is *information* — record old vs new counts).

1. Transactions/hour by hour (or peak ÷ 8am ratio)? → `DAY_CURVE`,
   `sim/shopper.py`.
2. Units/day for a milk-, bread-, and bulk-equivalent? → `popularity` +
   `opening_boh`, `sim/catalog.py`.
3. Real promo lift and endcap endurance? → `profile_mult` spiky boost,
   `promo_rush` mult, `PROMO_SECONDARY_PCT`, `decision/rules.py`.
4. Delivery schedule + receipt sizes in cases? → `trucks_min`,
   `RECEIPT_WAVES_MIN`, `receipt_cases`, `config/poc.yaml` + runner/loop.
5. True shelf capacities / case sizes for 2–3 known SKUs? → catalog rows.
6. Real associate task time and carry capacity? → `associate_delay_min`,
   and later a labor-capacity model (does not exist yet).

## Export spec for offline replay (what to pull from real stores)

Five CSVs. No PII needed anywhere — event grain suffices, no baskets,
customers, or loyalty data. Field names mirror `01-data-contracts.md`.

### File 1 — `boh_events.csv` (the core: replays demand + supply)
One row per inventory change: `store_id, sku, ts (ISO8601, tz-aware),
boh (units AFTER the event), delta (signed units), reason
(sale|receipt|correction|return|unknown), event_id (if available)`.
- `delta < 0` = sale, `delta > 0` = receipt. Per-unit rows ideal;
  per-minute aggregates (`units`, end `boh`) acceptable.
- **Corrections/recounts MUST be labeled as such** — an unlabeled recount
  to zero looks exactly like a demand spike and will poison velocities
  (same bug class as our silent-drain handling in `sim/loop.py`).
- Day-opening BOH per `(store, sku)`: explicit snapshot rows preferred
  (`delta=0, reason=snapshot`); otherwise we take the last `boh` before
  open as opening and say so.

### File 2 — `truck_arrivals.csv`
`store_id, arrival_ts, truck_id, sku, units_delivered` (one row per
SKU on the truck; cases acceptable with case size noted). Quantities let
us join truck → receipt events and measure the real dock-to-backroom
delay (sim assumes +5–15 min, `sim/truck.py`).

### File 3 — `associate_restocks.csv` (the labels — most valuable file)
`task_shown_ts, store_id, sku, suggested_cases (NULL if
associate-initiated), action (done|adjusted|skipped), cases_stocked,
completed_ts`. This yields everything the feedback loop needs:
- **Delay distribution** = `completed_ts − task_shown_ts`, by hour —
  calibrates `associate_delay_min` (sim: flat 25) and reveals whether
  evenings actually run slower.
- **Ground-truth overrides**: `suggested_cases ≠ cases_stocked` →
  adjusted; `skipped` → rejected. Replay precision/recall scores our
  fired tasks against what staff really did.

### File 4 — `product_master.csv` (one-time, small)
`sku, name, shelf_capacity_units (facings), case_size_units, is_promo,
is_bulk`. Without facings/case sizes we cannot compute `shelf_est` or
`cases_needed`. Fallback if missing: infer `case_size` from the mode of
stocked quantities — approximate, flagged wherever used.

### File 5 — `stores.csv` (tiny)
`store_id, open_time, close_time`. Defaults to 07:00–22:00 if missing.

### Scope + quality bar
- Minimum: **1 store, 2 full weeks including a weekend** (weekday
  variation is a first-class model input now — retrieval matches on it).
- Top ~50 SKUs by volume at minimum; all SKUs preferred.
- Declare known gaps alongside the export: shrink/theft, returns,
  substitutions — anything where register sales ≠ inventory movement.

## Rules for future AI recalibrators

1. Never tune demand to satisfy `assert_acceptance` — tune acceptance to
   honest demand, and say so in the commit message.
2. Keep determinism: all randomness via sha256 seeds (`_seeded`,
   `ShopperSim.rng`); never `random.seed()`, builtin `hash()`, or
   wall-clock in demand paths. Verify with the cross-process test
   (`PYTHONHASHSEED=0` vs `1`).
3. Change ONE parameter family per commit; report before/after
   `make replay` summary lines (sales units, tasks by reason, bulk count).
4. Record every provided real number here with source + date, replacing
   its [TUNED] row. This file is the audit trail.
