# 04 — LLM Reasoner (Exceptions Only)

## Principle
Rules handle ~90% of tasks. LLM handles ~10% ambiguous cases and must explain itself. LLM is async (minutes latency OK) and never blocks the rules fast path.

## Route to LLM only if ANY true
1. `promo_ambiguous`: `is_promo` + shelf pct low but `BOH >= 1.2x effective_capacity` with no velocity spike (locked with `PROMO_BOH_COVER_FACTOR=1.2` in code — is endcap full? backroom?).
2. `bulk_candidate_suppressed`: bulk SKU suppressed by debounce but velocity rising (should we break debounce?).
3. `stale_zero`: `zero_flag` age > 60 sim-min with no truck yet (advise to expedite / transfer?).
4. `boh_anomaly`: BOH jump inconsistent with sales (e.g. +40 with no truck) or velocity > 3σ.
5. `repeat_task`: open task exists + continued sales dropping `shelf_est` further (merge / upsize?).

Otherwise: emit rule task directly, skip LLM (saves cost/latency).

## LLM input (assembled context, JSON)
```json
{
  "store_id": "store-001",
  "sku": "soda-12pk-101",
  "sim_ts": "...",
  "state": {"boh": 60, "shelf_est": 18, "effective_capacity": 72, "velocity_30m": 1.8},
  "product": {"is_promo": true, "case_size": 12, "threshold_pct": 0.35},
  "trigger": "promo_ambiguous",
  "recent_sales": [...last 10...],
  "open_task": null,
  "truck_eta": "14:00 sim"
}
```

## LLM output (strict schema, validated with Pydantic)
```json
{
  "needs_restock": true,
  "cases_override": null,
  "confidence": 0.82,
  "rationale": "<=2 sentences, associate-readable, references numbers>",
  "suppress_until_min": 0,
  "missing_data": []
}
```
- `cases_override` only to *reduce* or *delay*; final cases still pass through `cases_needed()` clamp in `02`.
- If `needs_restock=false`: emit `suppressed` with rationale + `suppress_until_min` (no new LLM call for that key until expiry).

## Prompt sketch (system)
> You are a grocery replenishment advisor. Shelves + endcap hold effective_capacity. Backroom BOH is not on shelf until associate restocks. Promo SKUs have 50% extra endcap stock. Bulk SKUs thrash if tasked too often. Return JSON only matching schema. Never invent BOH or sales. Prefer suppress + reason when uncertain. Keep rationale under 2 sentences with numbers.

Few-shots: (a) promo ambiguous → restock 2 cases, endcap likely empty given 3x velocity; (b) bulk suppressed → suppress 60 min, shelf fits <1 case; (c) truck-zero → restock now, dock manifest confirms.

## Model / infra for POC: local-first on Mac Studio M2 Max 64GB (LM Studio)
- Default: **LM Studio native on macOS** (Metal-accelerated), NOT in Docker. In LM Studio: load model → Developer tab → Start Server (this machine: `http://127.0.0.1:8081`, OpenAI-compatible `/v1/chat/completions`, JSON mode supported). Compose services reach it at `http://host.docker.internal:8081` — same API shape, so provider is one config change.
- Pinned for this POC (verified 2026-09-13): `qwen2.5-7b-instruct-mlx` @ `http://host.docker.internal:8081/v1` — live-tested, returns schema-valid JSON (~1–3s, 327 tokens for promo-ambiguous probe). Prefer **MLX builds** when available (e.g. `Qwen2.5-7B-Instruct-MLX-4bit`, `Llama-3.1-8B-Instruct-MLX-4bit`) — MLX is Apple-Silicon-native, uses unified memory without CPU/GPU copies, typically 10–30% faster than the same-model GGUF on M2 Max. Fallback to `GGUF Q4_K_M` when no MLX build exists (universal, portable to Ollama/Linux). Both serve identically over `/v1/chat/completions`, so switching is config-only. Step-up if rationales feel shallow: `Qwen2.5-14B` MLX/GGUF (~9GB) or `Qwen2.5-32B` (~20GB) — all comfortable in 64GB.
- `LLM_PROVIDER=lmstudio`, `LLM_MODEL=qwen2.5-7b-instruct-mlx`, `LLM_BASE_URL=http://host.docker.internal:8081/v1` in `config/poc.yaml`. Verify with `curl http://127.0.0.1:8081/v1/models` before starting Compose. Known-good probe: promo-ambiguous (shelf 18/72, BOH 60, 3x velocity) → `needs_restock=true, confidence=0.85`, valid JSON. Follow-up hardening needed: prompt must say units-vs-cases explicitly and require velocity reference — probe rationale said "18 cases" (actually units) and ignored velocity.
- Verified 2026-09-13 via `llm/service.py` (prompt v1, 1.4s): rationale now reads "Shelf holds ~18 of 72 units with a velocity of 1.8 u/min (~3x baseline), indicating the endcap is likely depleted." Note: this LM Studio build rejects `response_format: json_object` (400) — service auto-retries without it and still gets clean JSON thanks to the "JSON ONLY" prompt instruction.
- Alternatives via same client, no code change: Ollama native (`host.docker.internal:11434`), Docker Model Runner (`http://model-runner.docker.internal` — convenient but runs in Docker VM, slower than native Metal, model lifecycle tied to Docker; use only if you want zero native installs), hosted Haiku/GPT-4o-mini (fallback if local quality disappoints).
- Cache key `(store,sku,trigger,bucketed_state)` TTL 15 sim-min to avoid repeat calls.
- Timeout 30s wall (local first-token is the long pole) → fallback to rules decision + `source=rule_fallback`.
- Log: prompt version, model name, input hash, output, latency, override outcome (no API cost to track when local).

## Guardrails (hard)
- Reject output if `cases_override * case_size > BOH` → clamp + flag.
- Reject non-JSON / missing fields → fallback to rules.
- Never emit PII. Rationale must not contain instructions beyond fetch task.
- Eval: weekly review of `override_rate` (associate adjust/reject on LLM tasks should trend <20%).

## Learning exercises
1. Notebook: replay 20 logged exceptions, compare prompts.
2. Ablation: disable LLM → count extra bulk tasks + promo false positives (proves value).
3. Tune `suppress_until_min` from feedback.
