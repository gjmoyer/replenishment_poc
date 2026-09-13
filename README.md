# Shelf Replenishment POC — LLM in the Mix

A working prototype that watches real-time grocery sales and decides what
needs restocking on the shelves — with a small local LLM reasoning over the
ambiguous cases that pure rules get wrong.

Deterministic rules handle ~90% of decisions (fast, exact, cheap). An LLM
handles the ~10% exceptions: promo items with endcap stock, bulk items that
thrash naive thresholds, silent out-of-stocks only a truck delivery can
rescue. Every LLM verdict passes back through the rules calculator, so the
model can advise and explain but never invent quantities.

## How it works

```
shoppers + trucks (sim) ──► Redpanda ──► decision service ──► dashboard
                                │              │  ▲                     ▲
                                │              ▼  │ LLM-only on          │ human
                                │           Postgres │ exceptions    restocks
                                │              │  ▼                     │
                                │         llm-reasoner ──► LM Studio (local, Mac)
```

- **Simulated stores** publish every sale, backroom receipt, and truck
  arrival to Kafka. A full 07:00–22:00 day plays in ~15 minutes at 60x.
- **Decision service** tracks estimated shelf quantity per item (shelves
  start full at opening), fires restock tasks with exact case counts, and
  routes only ambiguous cases to the LLM with a strict JSON contract.
- **Dashboard** shows per-store shelf levels, the task queue with
  rule-formula or LLM rationales, and lets you play the associate:
  restock, adjust, skip, dispatch trucks, force sale bursts, run scenarios.

## Quickstart

Prerequisites: Docker Desktop, `uv`, and [LM Studio](https://lmstudio.ai)
with `Qwen2.5-7B-Instruct-MLX-4bit` loaded and its server on `:8081`.

```bash
make up            # Redpanda + Postgres 18 + all services
# open http://localhost:8501/ — press Restart, then Play
make replay        # headless deterministic day (seed 42) + acceptance checks
make test          # unit + regression suite
make down          # stop everything
```

Five-minute tour: Restart → Play at 60x → watch morning depletion →
Restock a task → at ~13:00 eggs silently empties → 14:00 truck arrives →
`VERIFY DOCK` → receipt lands → restock task. That scene is the thesis.
Full operator instructions: [`doc/08-dashboard-guide.md`](doc/08-dashboard-guide.md).

## Repo layout

```
core/       single config source (config/poc.yaml + env)
sim/        demand publishers, truck sim, headless day loop, live runner
decision/   rules engine, LLM router, Kafka↔Postgres service
llm/        FastAPI reasoner + hardened prompt + schemas
dashboard/  Streamlit ops console (dark theme)
infra/      docker-compose.yml, Dockerfile, Postgres schema
tests/      rules (30+) + sim acceptance + LLM contract tests
doc/        split design docs, one topic each (see doc/README.md)
```

Python throughout (`uv` managed), one shared Postgres, Kafka-protocol
transport (Redpanda locally, swappable for real Kafka with one env var).

## Docs

Start with [`doc/00-overview.md`](doc/00-overview.md), then whatever your
task needs — each file is self-contained and sized for AI consumption:

| File | Covers |
|------|--------|
| `01-data-contracts.md` | Kafka topics, JSON schemas, product/store master |
| `02-rules-engine.md` | Shelf tracking, restock math, promo/bulk/truck rules |
| `03-simulation-publishers.md` | Shoppers, trucks, speedup, scenarios |
| `04-llm-reasoner.md` | Exception triggers, prompt, guardrails |
| `05-dashboard-associate.md` | UI spec |
| `06-architecture-techstack.md` | Services, Compose, run modes |
| `07-milestones-acceptance.md` | Build status, demo script |
| `08-dashboard-guide.md` | How to operate the monitor |
| `09-simulation-realism.md` | Real-vs-invented calibration status, retune worksheet |
| `10-session-log.md` | Session decisions, incidents, handoff notes |
| `11-future-work.md` | Session learnings + prioritized improvement ideas |
| `12-scalability.md` | 2,000-store × 10K-SKU evaluation, bottlenecks, pilot vs fleet path |

## Status

M1–M6 built and demo-verified live: rule tasks, LLM exception tasks,
manual restock/adjust/skip, truck rescue flow, scenarios, day-end summary,
PG-backed exactly-once processing and restart resume. See `07` for details.

MIT licensed. Built as a learning vehicle for LLM-in-the-loop retail ops.

## Dashboard

![Live ops console mid-day: shelf grid with velocities and cover, open restock tasks with rule rationales, event log with LLM repeat-task reviews, and the top-10-by-volume table.](doc/dashboard.png)

Mid-day at the Downtown store — milk running hot with an open fetch task,
eggs silently empty awaiting the 14:00 truck rescue, the event log showing
an LLM `repeat_task` review, and per-product fill rates in the Top 10 table.
