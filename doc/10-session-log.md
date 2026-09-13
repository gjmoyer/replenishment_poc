# 10 — Session Log & Handoff Notes

Append-only. Purpose: preserve *why*, not what. A future AI (or human)
resuming this repo should read `README.md`, `09`, then this file.

## Resume boot sequence (2 minutes)

1. `docker compose -f infra/docker-compose.yml ps` — all six up?
   (`redpanda, postgres, decision, llm, sim, dashboard`).
2. LM Studio on the Mac host: `curl -s localhost:8081/v1/models` must list
   `qwen2.5-7b-instruct-mlx`. Containers reach it via
   `host.docker.internal:8081`. If the model/server is down, the system
   still runs — every LLM call degrades to `rule_fallback`, visible as
   `RULE-FB` badges and `fallback=true` in `llm_calls`.
3. `uv run pytest -q` (expect 54 passed, 1 live-skipped) and
   `uvx ruff check .` (expect clean).
4. Dashboard at `:8501`, sim clock in the header. If the day looks corrupt
   (shelves 0 with full BOH everywhere), see Landmines before touching PG.

## Architecture decisions (with rationale)

| Decision | Why | Date |
|----------|-----|------|
| Redpanda (`redpandadata/redpanda`, NOT `redpanda/redpanda` — that name 404s) | Single-container Kafka protocol, no ZK/KRaft; skills transfer to real Kafka via one env var | 2026-09-13 |
| Postgres 18 only, no Redis | POC throughput is a few events/sec; one DB = one backup, one conn string. Mount is `/var/lib/postgresql` (PG18 changed the convention; `/data` fails — verified, not theory) | 2026-09-13 |
| `uv` + `pyproject` + `uv.lock` | Reproducible envs locally and in Docker (`uv sync --frozen`) | 2026-09-13 |
| LM Studio native (Metal) over Ollama/Docker models | Docker-on-Mac has no GPU passthrough; native is fastest. Same OpenAI-compatible API, so provider is one config change | 2026-09-13 |
| MLX over GGUF on Apple Silicon | 10–30% faster via unified memory; identical API, config-only swap | 2026-09-13 |
| Pinned model `qwen2.5-7b-instruct-mlx` | General Instruct (not Coder) for natural rationales; best JSON discipline at 7B; live-verified schema-valid output | 2026-09-13 |
| Streamlit, not Next.js | Single-language POC speed; fragments + custom HTML/CSS reach "good enough" ops polish | 2026-09-13 |
| Rules own quantity math, always | LLM output passes through `cases_needed()` twice (llm/service clamp + router `final_cases`). The model advises/reduces/delays; it can never inflate | 2026-09-13 |
| Epoch protocol for restarts | Runner truncates day tables then bumps `sim_control.epoch`; decision follows. Order matters (truncate-before-bump) so the dashboard never sees an empty grid | 2026-09-13 |
| Claim-first idempotency + manual offsets | `processed(event_id)` PG table survives restarts; offsets commit only after clean batches. Crash replays land in the guard instead of double-applying or vanishing | 2026-09-13 |
| Router LLM timeout capped at 12s (`ROUTER_MAX_TIMEOUT_S`) | A 30s model stall must never trip the 5-min late-event window or freeze the consumer loop | 2026-09-13 |

## Production incidents (symptom → cause → fix)

1. **Phantom zero shelves (shelf 0, BOH full).** Cause: decision recreated
   mid-day ran `seed_opening()` (opening BOH) against live mid-day BOH;
   first absolute update fabricated giant sales. Fix: `load_or_seed()`
   resumes PG rows + open tasks + checks + velocities + trucks; control
   epoch adopted *before* loading so the poll doesn't wipe it
   (`decision/service.py`).
2. **Duplicate open tasks for one key.** Cause: repeat_task path emitted
   instead of refreshing. Fix: `refresh_open()` updates the row in place;
   `emit_task`/`emit_check` refuse duplicates (`decision/service.py`).
3. **Orphaned `open` rows never swept.** Cause: in-memory-only sweep lost
   entries across recreates. Fix: PG backstop abandons by query
   (`sweep_timeouts`), plus wall-clock sweep for idle/paused days.
4. **Restock clicks did nothing.** Cause: dashboard passed Kafka key/value
   positionally (swapped). Fix: keyword args (`dashboard/app.py`).
5. **Total pipeline silence, no errors.** Cause: `processed` table added to
   schema files but the live volume predated it; every handler crashed into
   the batch try/except. Fix: `Store.__init__` runs mirrored DDL at boot
   (`decision/pg.py SCHEMA`); live table created manually that once.
6. **Non-reproducible replays across processes.** Cause: builtin `hash()`
   is salted per process. Fix: sha256 seeding everywhere; cross-process
   test with `PYTHONHASHSEED=0` vs `1` (`tests/test_sim.py`).
7. **Wrong Redpanda image + PG18 mount failure.** See decisions table.
   Both were fixed by reading the actual error output, not docs.

## Critic history (AI subagents, this session — no human review yet)

- **Code reviewer** (harsh staff-engineer): 54 defects round one, 12
  blockers round two. All 12 fixed; most of the 54 fixed. Deliberate
  overrules, both documented: (a) PG18 volume mount — critic cited the
  PG16 convention, live evidence won; (b) full-async service + coverage
  gates — deferred as POC over-engineering.
- **Dashboard critic**: 18-requirement brief + 100-point rubric, then
  graded the build **41/100** with 20 defects. Drove: dark theme, dense
  cards, per-store footer, oldest-first queue, live OPEN badges,
  claim-guarded commands, no in-fragment `st.rerun()`. Not regraded after
  the final iteration — scores above reflect the critic's last look, and
  several fixes landed after.
- Standing rule both critics enforced and the repo keeps: ruff clean,
  exact assertions (no `in (...)` tuples, no tautology tests), no magic
  numbers without names, no silent drops.

## Open / deferred (not bugs, just not done)

- 50-SKU scale mode exists in catalog (`build_catalog`) but the dashboard
  at 50 cards/120x is untested; `Show all` toggle + cached sparklines are
  in place but unproven under load.
- Hosted-LLM fallback path (Haiku/GPT-4o-mini) is configured but never
  exercised; only LM Studio local has run live traffic.
- No mypy/coverage gates; the decision loop is still sync
  (mitigated by the 12s router cap).
- `doc/01` Kafka-vs-PG contract drift was partially reconciled
  (emit snapshots, actor); full alignment is still manual.
- No human has reviewed the dashboard aesthetics or `decision/service.py`.

## Landmines (learned the hard way)

- **Never truncate PG tables by hand.** Always Restart via the dashboard
  (runner path: truncate → reset memory → bump epoch). Manual truncates
  desync runner/decision BOH and manufacture phantom sales.
- **Kafka from the Mac host hangs.** Redpanda advertises `redpanda:9092`,
  unresolvable off-Docker. Probe Kafka from inside the network
  (`docker exec`), PG from the host (`localhost:5432` is forwarded).
- **`st.rerun()` inside fragments** causes full-page reruns and eaten
  clicks; fragments already refresh on interaction + timer. Don't re-add.
- **Streamlit widget `default=` + preset session key** logs policy warnings
  every second; use the `if key not in session: init` pattern instead.
- **`st.cache_data` takes `max_entries`**, not `maxsize` (crashed the app
  on load once).
- **Suppressions are transition-logged** (30-min per key+reason), so event
  counts understate tick-level suppressions — that is intentional, not loss.
- **Day-end zeros with full BOH and no task usually means LLM-suppressed**
  (stale_zero / endcap logic), not a bug. Check `llm_calls` + suppress
  events before debugging.
