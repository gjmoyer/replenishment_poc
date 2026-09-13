# 08 — Dashboard User Guide

How to operate the monitor at `http://localhost:8501/` (`make up` first).
Spec is `05-dashboard-associate.md`; this is the human operator's version.

## The shelf bar (every card)

The 8px line under each product name is the **shelf level**:
fill width = estimated units on shelf ÷ total shelf room
(`effective capacity` = shelf facings + endcap for promo items).

- **Red** — below the restock threshold (default 35%, printed on the card
  as `refill <35%`). The shelf needs a visit; expect a matching Task queue
  card. For promos red means shelf *and* endcap are low.
- **Amber** — below 50%, draining but no task yet. Watch it.
- **Green** — 50%+, healthy. The gray track is the empty portion.

Companion lines: `Shelf 18/72 (25%)` is the same numbers as text;
`BOH 164 (backroom ~142)` is everything in the building vs. the
backroom estimate (`BOH − shelf_est`); `vel 1.8/0.6 u/m` is current pace
vs. 2h baseline; `cover ~10m` is minutes until empty at current pace
(`EMPTY` when zero); `sold today 42` and `last fill 09:12` are history.

## Header (sticky)

- **Store dropdown** — switch stores; the URL tracks it (`?store=store-002`
  deep-links straight to a store).
- **Clock + progress** — sim time and % of the 07:00–22:00 day. Shaded
  bands are the 12–14 and 17–19 rushes.
- **30 / 60 / 120** — speed. 60x = one sim-minute per real second, so a
  full day plays in ~15 minutes.
- **Pause / Step +15m / Restart** — freeze time to read, hop forward, or
  start a fresh day (seed chip shows the deterministic seed).

## Shelf grid (left)

One card per product, **urgent-first** (zeros and reds float up). Toolbar:
search box, filter pills (`All | Needs restock | Promo | Bulk | Zero`),
`Show all` toggle past the first 24, and a showing-count.

- **ZERO** — shelf reads 0 units. If BOH is also ~0, only a truck can fix it.
- **OPEN TASK** — a restock job already exists; work the queue, don't duplicate.
- **PROMO** — sale item; room includes +50% endcap stock.
- **BULK** — small facing (e.g. dog food); restocks are debounced (~75 min)
  so one card covers the quantity *and* the wait.

## Task queue (right) — the associate job

One card per trip, oldest (most starved) first:

1. `FETCH 4 cases (24 units)` — what to pull from the backroom.
2. Drift line (`Shelf 12→8/36 · BOH 132→128`) — shelf/BOH at fire time vs.
   now, so you see what moved while it waited.
3. Rationale — gray mono box = deterministic rule with exact math
   (`8/24=33% <35% → 3 cases (16 units, BOH 92)`, prefixed with fire
   time); purple-bordered quote = LLM exception reasoning with
   confidence chip.
4. Buttons — **Restock done** (shelf refills, card clears), **−1/+1**
   (adjust cases, then Done), **Skip** (associate rejects; key goes quiet
   30 sim-min and it counts as an override), **Verify dock** (truck checks
   with 0 cases: confirm the delivery arrived before fetching).

Ages turn red past +60m sim. Anything past ~120m is auto-abandoned by the
decision service and will re-fire if the shelf is still low.

## Inject events

- **Send truck now** — the picker lists SKUs whose building can't cover
  one case (preselected). Below it, shelf-empty-but-stocked SKUs are named
  with the guidance to send an associate instead: they are auto re-checked
  on every truck arrival, but the truck carries nothing they need.
- **Burst** — force N sales on one SKU to trigger a restock immediately.
- **Scenario buttons** — toggle day flags, then restart the day with the new
  set: promo rush, 2h bulk thrash, silent OOS, receipt spike. Flags compose,
  so several can be on at once; the header pills always show the active set.
  Clicking a lit button switches just that flag off. Each change restarts
  the day; that is intended.
- All commands go through the runner one at a time; a "busy, retry" toast
  means a previous command is still being picked up.

## Event log + LLM calls

Append-only feed (newest top, fixed scroll): sales bursts, receipts
(`backroom only — shelf unchanged`), trucks, tasks, suppressions
(struck through, with source badge), confirmations, LLM calls, abandons.
Every row shows SIM time + wall-clock time. Filter by SKU text and type
chips. `LLM calls (eval)` below it shows each exception call with trigger,
latency, confidence, and rationale — the tuning surface for prompts.

## Footer + day end

Always-visible totals for the selected store (LLM calls are all stores):
tasks fired, suppressed, LLM calls, rejected, open. At 22:00 a day summary
banner appears (`done X/Y, rejected, LLM calls, shelves still zero`) with
a Restart button.

## First run (5 minutes, seed 42, 60x)

1. **Restart**, **Play**. Milk/bread/eggs drain through the morning peak.
2. First red card + queue task → **Restock done** → bar jumps green.
3. ~13:00 eggs silently empties (no task — nothing left to sell). 14:00
   truck → `VERIFY DOCK` check → receipt lands → restock task → Done.
   That scene is the whole system thesis.
4. **Burst** soda for an on-demand task; **Skip** something to see
   suppression in the log.

## Rule of thumb

**Red bar + open task = normal, go fetch. Red bar + no task = read the
log** — it was suppressed with a stated reason (endcap likely full,
bulk debounce, BOH can't cover a case) or it is waiting on a truck.
Exception: an empty shelf WITH backroom stock always tasks immediately
(`zero_fetch`) — if you see shelf 0, BOH high, and no card, that is a bug,
not patience.
