# 12 — Scalability: From POC to 2,000 Stores × 10K SKUs

Evaluation target: 2,000 stores × 10,000 SKUs = 20M `(store, sku)` keys.
Headline: **the decision math scales, the scaffolding doesn't.** `rules.py`
is pure O(1)-per-key logic and ports to any architecture; everything around
it needs rework at fleet scale. See `11-future-work.md` for the related
multi-store roadmap (shared DC, fleet routing).

## Load assumptions (back-of-envelope)

Real grocery volumes, not the 5x-scaled-down demo curve:

- ~20K units sold / store / day → **~40M sales events/day** ≈ 500/sec
  average, **3–5K/sec at evening peak**.
- Shelf-state transitions, history writes, and task traffic scale with the
  above; LLM exceptions at ~2/key/day → up to tens of millions of reason
  calls/day unconstrained.

## Component verdicts

### Decision consumer — bottleneck #1 (single-threaded loop)
`decision/service.py` runs one poll loop: one message at a time, several
Postgres roundtrips each (idempotency insert, history upsert, shelf
upsert, task insert, per-batch Kafka flush). Realistic ceiling is low
single-digit thousands of msgs/sec — evening peak lands on top of it with
no headroom. A second instance cannot help today: in-memory shelf state is
unsharded, so two consumers would double-apply.
**Fix:** partition Kafka topics by store, run N consumers each owning a
shard. Requires a sharding concept the code does not have (consumer group
is a single `decision-v1`, state is one big dict).

### In-memory state — bottleneck #2 (per-unit timestamps)
`Brain` holds a `MutableShelf` plus a `sales_ts` deque per key, and the
deque stores **one Python int per unit sold** (~28 bytes each). A fast
SKU's 120-minute window holds thousands of ints; across 20M keys that is
tens of GB in a single process. **Fix:** fixed-window minute counters or
exponential-decay velocities instead of per-unit timestamps, plus sharding
(above). The velocity *semantics* (`02`) survive; the container doesn't.

### LLM economics — bottleneck #3 (fleet vs value)
20M keys × ~2 exceptions/day = tens of millions of calls/day against a
single local 7B doing ~0.5 calls/sec — three orders of magnitude short.
The fix is economic, not hardware: **gate the LLM by decision value**
(high-margin / high-velocity SKUs first — `price`/`margin_pct` are already
in the catalog and currently unused), cascade small-model-first, keep the
15-min cache. At fleet scale each call must cost less than the stockout it
prevents, or finance kills the feature. The feedback loop (`04`, #1 panel)
is exactly the instrument for proving that per trigger.

### Postgres — fine with discipline, three landmines without it
~500 writes/sec average is comfortable; 5K/sec peaks need batching and
fewer statements per message. The landmines, all present today:
1. `processed` (exactly-once ids) and `events` (append-only log) grow
   forever — no pruning exists. At 40M events/day that pages someone in
   month two. Add TTL pruning first; it is the cheapest fix in this doc.
2. `sales_hist` / `lost_sales` at per-minute grain for 20M keys is
   billions of rows/year. Partition by month, downsample to hourly after
   N days, aggregate cold data out.
3. Persistent `llm_calls` history ("no prune at POC scale") needs a
   retention policy past pilot — keep labels + input snapshots, they are
   small; still, define it before millions of rows/day arrive.

### Already scales
- **Kafka transport:** 500/sec average is noise; add partitions with the
  store-keyed sharding above.
- **Reasoner service:** stateless HTTP, replicable behind a balancer —
  given GPUs (see economics above).
- **Dashboard per viewer:** fresh connections, no shared state. But it is
  a *single-store console*: 2,000 concurrent managers needs many
  Streamlit replicas or, more sensibly, a chain-level rollup/alerting UI
  instead of per-store cards.

### Explicitly out of scope: the sim
The publishers are a demo harness, not architecture. In production real
registers feed Kafka and the sim retires — it never needs to run 2,000
stores (and the single-store default in `03` already reflects that).

## Roadmap

- **Pilot (10–50 stores): config + hygiene.** More Kafka partitions,
  bigger Postgres, TTL pruning for `processed`/`events`, LLM concurrency
  limits + value gating pilot. Rules engine untouched.
- **Full fleet (2,000 stores): re-architecture of state and inference.**
  Sharded consumers with counter-based velocities; value-gated LLM fleet.
  Contracts, rules, and the feedback-loop design carry over intact —
  which is what this POC was built to de-risk.
