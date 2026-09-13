# 01 — Data Contracts

All events carry both `sim_ts` (simulated store time) and `wall_ts` (emit time) to keep speedup explicit.

## Topics (Kafka logical names)

- `boh_updates` — one message per sale AND per receipt. Single source of truth for BOH.
- `truck_arrivals` — truck docked / manifest available.
- `restock_tasks` — output of decision service.
- `restock_confirmations` — associate (dashboard button) confirms fetch.
- `product_master` + `store_config` — compacted topics or REST snapshot loaded at sim start (simpler for POC).

Transport for POC may be real Kafka (Redpanda in Docker) or in-memory bus with same schemas. Schemas must not change between modes.

All three event topics carry `epoch` (the day counter from `sim_control`).
The decision service adopts higher epochs (reset + process) and drops
lower ones, so stale pre-restart messages can never emit future-stamped
tasks into a fresh day (see `10-session-log.md` incident 8).

## `boh_updates`
```json
{
  "store_id": "store-001",
  "sku": "milk-1gal-001",
  "sim_ts": "2026-01-05T09:14:00",
  "wall_ts": "2026-09-12T21:00:14Z",
  "boh": 84,
  "delta": -1,
  "reason": "sale | receipt | correction",
  "event_id": "uuid"
}
```
Rules:
- `delta < 0` = sale. `delta > 0` = receipt / BOH increase (new stock arrived at store).
- Consumers must handle out-of-order + duplicates via `event_id` (idempotent upsert, keep max `sim_ts` per key only if `event_id` unseen).
- No message is emitted when shelf hits zero and no one buys — this is the silent-OOS gap closed by `truck_arrivals` + `zero_flag` logic.

## `truck_arrivals`
```json
{
  "store_id": "store-001",
  "sim_ts": "2026-01-05T14:00:00",
  "wall_ts": "2026-09-12T21:05:00Z",
  "truck_id": "truck-042",
  "manifest_skus": ["milk-1gal-001", "dogfood-40lb-007"],
  "event_id": "uuid"
}
```

## `product_master` (per SKU, shared across stores with per-store overrides)
```json
{
  "sku": "dogfood-40lb-007",
  "name": "Bulk Dog Food 40lb",
  "shelf_capacity_units": 6,
  "case_size_units": 2,
  "is_bulk": true,
  "is_promo": false,
  "promo_secondary_pct": 0.0,
  "restock_threshold_pct": 0.45,
  "bulk_debounce_min": 75,
  "min_units_before_task": 2
}
```
Promo example:
```json
{
  "sku": "soda-12pk-101",
  "shelf_capacity_units": 48,
  "case_size_units": 12,
  "is_bulk": false,
  "is_promo": true,
  "promo_secondary_pct": 0.5,
  "restock_threshold_pct": 0.35
}
```
**Endcap rule (locked):** `promo_secondary_qty = round(shelf_capacity_units * 0.5)`. `effective_capacity = shelf_capacity + promo_secondary_qty` when `is_promo=true`, else `shelf_capacity`.

## `store_config`
```json
{
  "store_id": "store-001",
  "name": "Downtown",
  "open_sim": "07:00",
  "close_sim": "22:00",
  "skus": ["milk-1gal-001", "soda-12pk-101", "dogfood-40lb-007"],
  "opening_boh": {"milk-1gal-001": 120, "soda-12pk-101": 150, "dogfood-40lb-007": 20}
}
```

## `restock_tasks`
```json
{
  "task_id": "uuid",
  "store_id": "store-001",
  "sku": "soda-12pk-101",
  "sim_ts": "...",
  "cases": 2,
  "units_to_fetch": 24,
  "shelf_est": 18,
  "boh_at_decision": 60,
  "source": "rule | llm",
  "reason_code": "normal_low | promo_low | bulk_due | truck_zero | llm_exception",
  "rationale": "human-readable, LLM-generated only when source=llm",
  "confidence": 0.82,
  "status": "open | done | suppressed | superseded"
}
```

## `restock_confirmations`
```json
{
  "task_id": "uuid",
  "store_id": "store-001",
  "sku": "soda-12pk-101",
  "sim_ts": "...",
  "action": "done | adjust | reject",
  "cases_fetched": 2,
  "actor": "dashboard | sim-user"
}
```
On `done`: decision service sets `shelf_est = min(effective_capacity, shelf_est + cases_fetched*case_size)` and clears `zero_flag`.

Persisted `tasks` rows additionally snapshot `shelf_at_emit` / `boh_at_emit`
(units at fire time) so the dashboard can show at-emit → live drift.
