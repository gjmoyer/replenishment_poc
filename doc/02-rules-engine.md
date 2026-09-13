# 02 — Rules Engine (Deterministic, No LLM)

LLM never replaces this. LLM only advises on exceptions (see `04-llm-reasoner.md`). Quantity math lives here.

## State per (store_id, sku)

```
BOH, shelf_est, sales_since_last_task,
velocity_30m/60m/120m (units per sim-min),
last_task_sim_ts, zero_flag, zero_since_sim_ts, open_task_id
```

Init at 07:00: `shelf_est = effective_capacity` (promo includes endcap), `BOH = opening_boh`.

On `boh_updates`:
- If `new_boh > old_boh`: receipt. `shelf_est` unchanged until associate restocks (stock is in backroom, not shelf). Clear `zero_flag` only after restock confirmation OR if policy says receipt implies floor-ready — POC default: keep `zero_flag` until confirmation, but allow truck-zero tasks to reference new BOH.
- Else `sold = old-old - new_boh`, `shelf_est = max(0, shelf_est - sold)`.

On `restock_confirmations(done)`:
- `shelf_est = min(effective_capacity, shelf_est + cases_fetched * case_size)`
- `sales_since_last_task = 0`, `last_task_sim_ts = sim_ts`, clear `zero_flag`, mark task done.

## Capacity

```
secondary_qty = round(shelf_capacity * 0.5) if is_promo else 0
effective_capacity = shelf_capacity + secondary_qty
```

## Task quantity (single calculator function)

```python
def cases_needed(shelf_est, effective_capacity, case_size, boh):
    if shelf_est >= effective_capacity:
        return 0
    need_units = effective_capacity - shelf_est
    cases = ceil(need_units / case_size)
    # clamp: can't fetch more than BOH allows, can't exceed what fits
    cases = min(cases, floor(boh / case_size) if case_size else 0)
    max_fit = ceil(need_units / case_size)
    return min(cases, max_fit)
```
If result is 0 → no task even if pct low (BOH-constrained). Emit `suppressed` event for observability.

## Trigger rules

### Normal SKU
```
if shelf_est / effective_capacity < restock_threshold_pct (default 0.35):
    emit task
```

### Promo SKU (sale + endcap)
- Use `effective_capacity` (1.5x) in pct check, NOT shelf alone.
- Extra guard: require `BOH < effective_capacity * 1.2` OR `velocity_30m > 2 * velocity_120m` (spike). Otherwise suppress — stock is probably on endcap.
- `reason_code = promo_low`.

Why: avoids firing when main shelf dips but endcap still holds 50% extra.

### Bulk SKU (dog food: small facing, e.g. capacity 6, case 2)
Pct alone thrashes. Require ALL:
1. `shelf_est <= min_units_before_task (default 2)`, AND
2. `cases_needed >= 1`, AND
3. `sim_now - last_task_sim_ts >= bulk_debounce_min (default 75 sim-min)`, AND
4. No open task for this key.
- Else suppress with counter `bulk_suppressed_total`.
- `reason_code = bulk_due`.

### Silent zero
```
if shelf_est <= 0:
    zero_flag = True, zero_since = sim_now
    # do NOT emit repeat tasks every sale (there are no sales). Emit one `zero_open` marker.
    # Real task comes from truck_arrivals (see below) or from LLM exception if stale > threshold.
```

### Truck arrival
```
on truck_arrivals:
  for sku in manifest_skus:
    if zero_flag OR BOH == 0 OR open zero marker:
        emit check/restock task using current BOH (post-receipt BOH update usually follows within minutes)
        reason_code = truck_zero
```
Handle ordering: if manifest arrives before `boh_updates` receipt, emit task as `pending_boh` and upgrade cases when receipt lands. Never emit `cases > floor(BOH/case_size)` — if BOH is 0 at that instant, emit `check` with cases=0 + instruction "verify dock".

## Idempotency / ordering
- One open task max per `(store,sku)`. New trigger while open → `superseded` (update units) not duplicate.
- Ignore duplicate `event_id`. Allow late `sim_ts` within 5 sim-min window; older → log + drop.
