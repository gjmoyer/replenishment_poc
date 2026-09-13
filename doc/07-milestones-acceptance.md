# 07 — Milestones + Acceptance (status: M1–M6 built and demo-verified live)

Live stack: `make up` → Redpanda + Postgres 18 + decision + llm-reasoner
(LM Studio `qwen2.5-7b-instruct-mlx` on the Mac host) + sim runner +
Streamlit dashboard at :8501. Verified live: rule tasks with formula
rationales, LLM exception tasks with verbatim rationales, manual restock /
adjust / skip, truck-now, sale burst, 4 scenario restarts, silent-OOS
truck rescue (check → receipt → task), day-end summary, debounce caps
(bulk ≤3/day base, ≤4 under 3x thrash), PG-backed exactly-once
(processed table + manual offsets) and restart resume.

## Build order (each slice demoable)

### M1 — Contracts + rules unit tests (no Kafka, no UI)
- Implement `cases_needed()` + trigger rules from `02` + tests for normal/promo(1.5x)/bulk-debounce/truck-zero/clamp-to-BOH.
- Done when: `pytest tests/test_rules.py` green with ≥15 cases including dog-food thrash + promo endcap.

### M2 — Local sim loop (memory bus, CLI)
- `SimClock` + shopper + truck + decision in one process, print tasks to console.
- Done when: `make replay --seed 42` produces ≥3 normal tasks pre-noon, ≥1 promo, dog food ≤3/day, silent-OOS rescued only after truck.

### M3 — Kafka + state store
- Swap to Redpanda + Postgres in Compose, idempotency via `event_id`, one-open-task-per-key.
- Done when: kill/restart decision service mid-day → no duplicate tasks, state rebuilds from topics.

### M4 — Dashboard + manual restock
- Store selector, shelf grid, task queue with Done/Adjust/Reject, time controls, `Send truck now` + `Force sale burst`.
- Done when: user can play full day at 60x, trigger a burst to force a task, click Done, see shelf bar refill in <1s.

### M5 — LLM exceptions
- `llm/service.py` + router predicates from `04`, cache + timeout fallback, rationale in UI.
- Done when: with LLM on vs off, bulk tasks drop, promo false positives drop, every LLM task shows rationale + confidence, timeout never blocks rules path.

### M6 — Scenarios + eval
- Preset buttons + day-end summary + `llm_calls` review.
- Done when: each scenario in `03` runs one-click and day-end modal shows tasks/suppressed/LLM-calls/overrides.

## Demo script (5 min, seed 42, 60x, store-001)
1. Play → point out milk depleting, task fires (RULE), click Done → refills.
2. `Force sale burst` on soda (promo) → note 1.5x capacity math, task or LLM rationale.
3. Show dog food suppressed counter (bulk debounce working).
4. `Silent OOS` → shelf zero, no task → `Send truck now` → check task → Done after receipt.
5. Pause, show LLM vs rule badges + event log. End.

## Definition of done for POC
All M1–M5 acceptance true, docs in `doc/` match implementation, `make up` + `make replay` both pass from clean clone, total LLM spend per demo day <$1.
