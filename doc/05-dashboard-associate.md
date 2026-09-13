# 05 — Dashboard + Associate Workflow

## Users
Single simulated user plays observer + associate. No auth in POC.

## Layout
```
┌ Header: sim clock (sim time + speed + pause/play/step) + seed + store selector ┐
├ Left: SKU shelf grid (per selected store)                                      │
├ Right: Restock task queue (open/done) + Truck panel + Event log                │
└ Footer: metrics (tasks fired, suppressed, LLM calls, overrides)                ┘
```

## Store selector
Dropdown for `store-001`, `store-002`, (add more). Switching store re-queries state + tasks. URL param `?store=store-001` for deep link.

## Shelf grid (per store)
Card per SKU showing:
- Name, flags (`PROMO` / `BULK`), `shelf_est / effective_capacity` progress bar (red <threshold, yellow <50%, green ok).
- `BOH`, velocity sparkline (last 60 sim-min), `zero` badge if flagged, `open task` badge.
- Sort: needs-restock first, then lowest pct. Filter: All / Needs restock / Promo / Bulk / Zero.
- Auto-refresh every tick (1 wall-sec at 60x) + on task events. Must handle 50 SKUs without lag (virtualize or paginate if needed).

## Task queue
Each open task: `SKU, location (aisle/endcap for promo), fetch N cases (M units), source badge (RULE blue / LLM purple), reason_code, rationale sentence, sim age`.
Actions:
- **Restock done** (primary): emits `restock_confirmations(done)` → shelf fills per `02` math. This is the manual associate simulation.
- **Adjust** (optional POC): change cases ±1 then done.
- **Reject / Skip**: emits `reject` → suppresses re-fire for 30 sim-min, logs override for LLM eval.
- Truck-zero `check` tasks show `Verify dock` variant when cases=0.

## Time + scenario controls (header or side panel)
- Play / Pause / Step +15 sim-min / speed select (30x/60x/120x) / Restart day.
- `Send truck now` button: opens SKU multi-select (preselect zero-flag SKUs) → publishes `truck_arrivals`.
- Scenario buttons: `Promo rush`, `Bulk thrash test`, `Silent OOS`, `Receipt spike` (see `03`).
- `Force sale burst` button: inject 10 sales on selected SKU to trigger restock on demand (key for demo: "be able to trigger restock events").

## Event log
Append-only feed: sales bursts, receipts, truck arrivals, tasks fired/suppressed (with source), confirmations. Filter by SKU. Shows `sim_ts` + wall latency.

## UX requirements
- Full day visible: user can watch 07:00→22:00 in ~15 min at default speed and still click restock in time. Tasks must persist (not auto-expire) until confirmed or day ends.
- Manual restock must feel instant (<500ms wall) even while sim runs.
- LLM rationale shown verbatim with confidence; rule tasks show formula string (e.g. `18/72=25% < 35% → 5 cases? clamped to 4 by BOH`).
- Empty states: "All shelves stocked", "No trucks yet", day-end summary modal (tasks done, overrides, LLM calls).

## Tech suggestion
Streamlit for fastest POC (single Python app container, session state per store). Migrate to Next.js only if Streamlit refresh can't keep up. Dashboard reads Postgres `shelf_state` (poll every 1s is fine at POC scale).
