# 06 — Architecture + Tech Stack

## Decision: Docker Compose orchestrates everything, Postgres-only state

- All services run as containers via a single `infra/docker-compose.yml`. No bare-metal Python in demo mode.
- No Redis. Postgres is the only state store (live state + history). At POC scale (2–3 stores, 50 SKUs, ~1–5 events/sec) Postgres easily keeps up, and it removes an entire container + client library from the learning path.
- Redpanda is the Kafka transport (see "What is Redpanda?" below). Kafka protocol stays, so learnings transfer to real Kafka.

## Services (all in Compose, all Python except infra images)

```
sim-clock ──┐
shopper-sim (x N stores) ─┼─► redpanda (Kafka protocol) ─► decision-service ─► tasks topic ─► dashboard (Streamlit)
truck-sim ──┘                        ▲                              │  (async HTTP call)
                                     │                              ▼
                               postgres ◄──────────────────── llm-reasoner (FastAPI)
                          (shelf_state + history + llm_calls)
```

- `redpanda`: single-node `redpanda/redpanda` image, topics auto-created. Kafka-compatible: existing `kafka-python` / `confluent-kafka` clients work unchanged against `redpanda:9092`.
- `postgres`: single `postgres:18` container. Two roles in one DB:
  - `shelf_state (store_id, sku)` — one row per key, upserted on every event (live state for dashboard + decision).
  - `events / tasks / llm_calls` — append-only history for charts, replay, eval.
  - Dashboard polls `shelf_state` every 1s — fine at this scale. No LISTEN/NOTIFY needed for POC (add later if UI feels laggy).
- `decision-service`: Python asyncio Kafka consumer. Owns `02` rules + router + `04` LLM client. Reads/writes Postgres, emits to `restock_tasks`.
- `llm-reasoner`: FastAPI `POST /reason` (sync, called by decision-service). Separates prompt/config from stream logic so prompts iterate without touching consumer.
- `dashboard`: Streamlit. Publishes `restock_confirmations` + ad-hoc trucks/bursts (producer role) and reads Postgres.
- `sim publishers`: Python containers (`sim/shopper.py`, `sim/truck.py`, `sim/clock.py`) in same Compose network. Same code runnable standalone with `TRANSPORT=memory` for unit learning without Docker.

## Language per part: yes, all app code is Python 3.12

| Part | Python? | Key libs |
|------|---------|----------|
| `sim/` clock, shopper, truck, scenarios, catalog | Yes | `confluent-kafka`, `numpy`, `pyyaml` |
| `decision/` service, rules, router, state | Yes | `confluent-kafka`, `psycopg2`/`psycopg[binary]`, `pydantic`, `httpx` (to LLM) |
| `llm/` reasoner API, schemas, prompts | Yes | `fastapi`, `uvicorn`, `pydantic`, `openai` client → LM Studio `qwen2.5-7b-instruct-mlx` (`host.docker.internal:8081`), alt Ollama/hosted |
| `dashboard/` Streamlit app | Yes | `streamlit`, `psycopg2`, `pandas`, `confluent-kafka` (producer for confirmations/trucks/bursts) |
| `tests/` | Yes | `pytest` |
| `infra/` compose, config, Makefile | No — YAML/Make | images only: `redpandadata/redpanda` (C++), `postgres:18` (C) |

One language on purpose: single venv/requirements pattern, shared `pydantic` schemas from `01-data-contracts.md` across sim → decision → LLM → dashboard, fastest learning loop.

## Python env: uv (locked decision)
- Repo root `pyproject.toml` + `uv.lock` (committed). Per-service deps as optional groups or single shared env for POC speed — default single env.
- Local: `uv sync` → `uv run pytest` / `uv run streamlit run dashboard/app.py` / `uv run uvicorn llm.service:app`.
- Docker: `COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv` then `uv sync --frozen --no-dev`. Benefits: fast cached layers, reproducible.
- Python 3.12 pinned in `pyproject.toml` (`requires-python`).

## Repo layout (as built)
```
doc/                  # split requirements + this file
core/  config.py      # single config source (yaml + env)
sim/  clock.py shopper.py truck.py scenarios.py catalog.py loop.py runner.py
decision/  service.py rules.py state.py router.py bus.py pg.py
llm/  service.py prompts/v1.md schemas.py
dashboard/  app.py
config/ poc.yaml
infra/  docker-compose.yml Dockerfile postgres_init.sql
tests/  test_rules.py test_sim.py test_llm_service.py
.streamlit/ config.toml  (dark theme)
Makefile  (test, replay, health-llm, lint, up, down, logs)
```

## Config
Single `config/poc.yaml`: stores, SKUs, thresholds, speeds, seeds, model name. Env overrides: `KAFKA_BROKER` (points at Redpanda), `DATABASE_URL`, `LLM_MODEL`, `LLM_API_KEY`, `TRANSPORT`.

## Run modes (all via Compose except local learning loop)
1. `make sim-local` — no Docker: memory bus + SQLite (same schemas as Postgres), 1 store, Streamlit + sim in one process. Fastest learning loop.
2. `make up` — full Docker: Redpanda + Postgres + decision + LLM + dashboard + sims. Demo mode.
3. `make replay --seed 42` — deterministic headless run (Compose or local), asserts acceptance counts (CI-friendly).

## What is Redpanda?
Redpanda is a drop-in, Kafka-protocol-compatible streaming broker written in C++. No ZooKeeper/KRaft, single container, ~100ms startup, low RAM — ideal for local Docker. Your Python code uses standard Kafka clients and topics exactly as in `01-data-contracts.md`, so everything learned transfers 1:1 to real Apache Kafka later. If your org already runs Kafka, swap the image for `apache/kafka` and change only `KAFKA_BROKER` — no code changes.

## Why Postgres only (no Redis)?
Redis was originally for sub-ms hot-key state + pub/sub. We don't need it here: POC throughput is a few events/sec and dashboard polling at 1s is imperceptible. One Postgres gives us live state + history + LLM logs in one backup, one connection string, one container to learn. If we later need <100ms at 1000+ stores, add Redis as a read-through cache in front of `shelf_state` — the schema won't change.

## Observability (minimal but required)
- Structured JSON logs with `sim_ts, store, sku, source, latency_ms`.
- Counters: `tasks_rule, tasks_llm, suppressed_bulk, suppressed_promo, truck_zero_tasks, llm_timeouts, override_rate`.
- Dashboard footer + `/metrics` endpoint (Prometheus text) on decision + LLM services.
- Persist every LLM prompt/output pair to Postgres `llm_calls` table for later eval.

## Data lifecycle (does the DB grow forever?)

No. Day tables (`sales_hist`, `tasks`, `events`, `llm_calls`, `lost_sales`,
`shelf_state`, `processed`) are TRUNCATEd on every Restart/scenario; a busy
day is a few thousand rows total (~10 MB). Fixed tables: `shelf_state`
(20 rows, upserted), `sim_control` (1 row). `processed` (Kafka
idempotency ids) is wiped on restart too — safe because the epoch fence
rejects old-epoch redeliveries before they ever reach the claim check.
Kafka itself keeps 7 days (`log_retention_ms=604800000` cluster default);
daily traffic is kilobytes, the ~200 MB data dir is Redpanda fixed
overhead (controller/offsets/preallocated segments), not our data.

## Cost / perf notes
- Rules path p95 <5s wall. LLM path p95 <2min wall (async, non-blocking).
- Cache LLM by state bucket; POC budget <$5/day on hosted small model; Ollama fallback documented.
