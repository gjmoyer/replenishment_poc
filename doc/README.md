# Replenishment POC — Docs Index

Split requirements for AI consumption. Read only what you need for the task.

| File | Purpose | When to load |
|------|---------|--------------|
| `00-overview.md` | Goals, non-goals, personas, store-day model | Starting any task, scoping |
| `01-data-contracts.md` | Kafka topics, JSON schemas, product/store master | Building publishers, consumers, state store |
| `02-rules-engine.md` | Deterministic shelf tracking + restock math | Building decision service, no LLM |
| `03-simulation-publishers.md` | Shopper sim, truck sim, time speedup, scenarios | Building simulation |
| `04-llm-reasoner.md` | Exception triggers, prompt, I/O schema, guardrails | Building LLM service |
| `05-dashboard-associate.md` | Dashboard, store selector, manual restock UX | Building UI |
| `06-architecture-techstack.md` | Services, Docker Compose, run modes, observability | Scaffolding repo, DevOps |
| `07-milestones-acceptance.md` | Build order, acceptance tests, demo script | Planning, verifying done |
| `08-dashboard-guide.md` | How to operate the monitor (bars, cards, queue, demo run) | Using the dashboard, giving a demo |
| `09-simulation-realism.md` | What is knowledge vs invented, calibration worksheet, retune rules | Changing demand, calibrating to real data |
| `10-session-log.md` | Decisions, incidents, critic history, landmines, resume steps | Resuming work cold, debugging déjà-vu |
| `11-future-work.md` | Session learnings + prioritized improvement ideas | Planning next work, avoiding re-learned lessons |
| `12-scalability.md` | 2,000-store × 10K-SKU evaluation, bottlenecks, pilot vs fleet path | Sizing, re-architecture planning |

## Conventions

- `(store_id, sku)` is the primary key for all state.
- Sim time vs wall time is always explicit. See `03-simulation-publishers.md`.
- Rules own quantity math. LLM never outputs final cases unchecked. See `02` + `04`.
- POC scale: 2–3 stores, 20–50 SKUs per store, 07:00–22:00 sim day.
