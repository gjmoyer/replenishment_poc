# 03 — Simulation Publishers (Shoppers + Trucks + Clock)

## Goal
Run a believable 07:00–22:00 day for one store (Downtown, 1.2x demand),
sped up, with visible restock events on demand. The stack stays N-store
generic (`stores_from_config`) — one store is a demo choice, not a limit:
stores are fully independent per `(store, sku)` keys, so extra stores add
volume, not behavior.

## Sim clock
- Central `SimClock` service owns sim time. All publishers query it; all events stamp `sim_ts` from it.
- Speeds: `30x` (30 wall-min/day), `60x` default (15 wall-min/day), `120x` (~7.5 wall-min/day). Plus `pause / step +15 sim-min / seek`.
- Tick: 1 sim-minute per tick. At 60x, tick every 1 wall-second.
- Deterministic seed per run (`--seed 42`) so demos are repeatable.

## Shopper publisher (per store)
Poisson-ish arrivals per sim-minute with day curve:

```
base_rate (baskets/min): 07-09: 1.5, 09-12: 3.0, 12-14: 4.5 (lunch),
14-17: 3.5, 17-19: 5.5 (peak), 19-22: 2.5
```

Per basket: pick 1–4 SKUs weighted by SKU `popularity` + promo boost (`is_promo` × 2.2 during peak) + noise. Each unit decrements BOH by 1 and emits one `boh_updates` (batch multi-unit as single delta for simplicity, or one event per unit — config flag, default batched).

SKU catalog for POC (per store, tweakable):
- `milk-1gal-001` — normal, capacity 24, case 6, popularity high, steady.
- `soda-12pk-101` — promo, capacity 48, case 12, promo 1.5x capacity, spiky.
- `chips-001` — promo, capacity 30, case 6, evening-heavy (pairs with soda
  for the promo-rush event).
- `dogfood-40lb-007` — bulk, capacity 6, case 2, popularity low but each sale hurts.
- `bread-loaf-003` — normal, capacity 30, case 10, morning-heavy.
- `eggs-12ct-005` — normal, capacity 36, case 12, morning-heavy.
- 15–45 more filler SKUs with randomized params for scale testing.

Each additional store would get its own seed + rate multiplier so dashboards
diverge; the default config runs Downtown alone.

## Receipt / BOH-increase publisher
Models backroom replenishment from DC, separate from shelf restocking:
- Scheduled receipts at 08:00 + 15:00 sim (configurable): pick the 4 SKUs
  with the lowest days-of-supply (`BOH / popularity`), `BOH += case_size * N`.
  Ranking by absolute BOH starved fast movers (milk at 177 looks "healthy"
  next to dogfood at 30 but covers far fewer hours), so waves target cover.
- Emits `boh_updates` with `reason=receipt`, `delta>0`.

Catalog par levels cover measured peak daily demand + 1 case buffer
(15-seed sweep: downtown milk mean 323/max 358 → opening 360; soda
269/295 → 300; chips 109/124 → 130). Under-par backrooms caused evening
stockouts no shelf rule can fix — the goods must be in the building
before the associate can fetch them.

This must NOT auto-fill shelf — shelf only fills on associate confirmation. Tests that distinction.

## Truck publisher
- Fixed trucks: 10:30 + 14:00 sim per store (configurable), plus a `Send truck now` dashboard button that injects an ad-hoc `truck_arrivals` with chosen SKUs.
- Manifest lists SKUs needing GOODS (`boh < case_size`) + 2 seeded fillers. Shelf-zero with backroom stock is deliberately NOT manifested — it needs an associate fetch, not a delivery — but every truck arrival re-evaluates all zero-flag SKUs as a wake-up call.
- Ad-hoc button is how the user "triggers restock events" on demand during demo.
- After manifest, schedule matching `receipt` BOH increases within +5–15 sim-min (models dock-to-backroom delay) so `truck_zero` tasks can compute real cases.
- Silent drain zeroes the BUILDING too (correction, not a sale: no velocity pollution), matching the runner. The loop used to zero shelf only — fixed for parity.

## Scenario presets (one-click)
- `promo-rush`: 18:00 peak + promo boost 3x on EVERY promo SKU (soda +
  chips endcap row) → promo exception path.
- `bulk-thrash-test`: dog food popularity 3x for 2 sim-hours → proves debounce suppresses.
- `silent-oos`: force one SKU to zero at 13:00 with no further sales → only truck at 14:00 rescues it.
- `receipt-spike`: large BOH increase mid-day → tests receipt vs shelf distinction.

## Config
```yaml
seed: 42
speed: 60
stores: [store-001]
day: {open: "07:00", close: "22:00"}
trucks: ["10:30", "14:00"]
scenarios: []
```

## Acceptance for this slice
With seed 42 at 60x: Downtown hits ≥3 normal tasks before noon, ≥1 promo task, dog food fires ≤3 tasks all day (debounced), silent-OOS SKU only clears after truck + manual restock.
