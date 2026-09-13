# 11 — Future Work: What We Learned + What Comes Next

Distilled from the Sept 2026 tuning + learning sessions. Each learning links
the evidence; each idea links the seam it plugs into.

## The reality gap (the highest-value open work)

Status: nothing here is validated against real data (`09` opens with "no
real POS data has been provided"). Every demand number — day curve,
popularities, case sizes, the 25-min associate delay — is invented or tuned
to look believable. 99.3% fill against invented demand proves the machinery,
not the policy. Ordered path to evidence:

1. **Offline replay of real history (highest value, no live risk).**
   Feed historical POS logs (timestamp, SKU, units — one store, one week
   suffices) through `evaluate()` + the sim loop; score fired tasks against
   actual staff restocks for precision/recall. Needs a CSV→events harness;
   no architecture changes, no operations risk.
2. **Calibration.** Fit demand curves, velocities, case sizes, lead times
   to the same data (`09` worksheet exists for this). Expect our pars and
   thresholds to move — that is the point.
3. **Shadow mode.** Decision service alongside real ops, tasks advisory
   only; the associate loop (`04` feedback panel) generates honest override
   labels with zero operational risk.
4. **Pilot in 1–2 stores**, with override <20% and falling regret as the
   go/no-go instrument. Only then do the fill-rate numbers mean anything.

## What we've learned

### 1. Threshold rules fail fast movers by construction (rules)
Baseline sim: 89.3% fill, 38% of shelf-gap loss arriving *after* the
associate's 25-min SLA (recoverable). A 35% threshold on milk (cap 24) is
~8 units ≈ 12–16 min of midday cover — the task fires, but physics wins.
Fix (`decision/rules.py`): fire when minutes-of-cover drops below
`associate_delay + buffer`, even above pct. Lesson: **thresholds must be
stated in time, not percent**, for anything with velocity. Bulk stays
exempt (floor + debounce ignore pct on purpose).

### 2. Absolute-BOH ranking starves fast movers (sim supply)
Receipt waves targeted lowest absolute BOH: milk at 177 looked "healthy"
next to dogfood at 30 while covering far fewer hours. Downtown milk demand
(~350/day) vs supply (~204) = systematic evening stockouts no shelf rule
can fix — *the goods must be in the building first*. Fix (`sim/loop.py`,
`sim/runner.py`): rank waves by days-of-supply (`BOH / popularity`) +
par levels covering peak demand + 1 case (`sim/catalog.py`). Lesson:
**replenishment has two halves (DC→backroom, backroom→shelf)** and all our
early tuning was on the wrong half.

### 3. The promo endcap guard needs a cover bypass (rules)
Suppressing while "stock is probably on endcap" drained soda to zero, then
paid a full 25-min stockout per `zero_fetch`. The sim models no separate
endcap buffer, so the guard's core assumption is unverifiable. Fix: when
cover goes critical, task anyway (still `llm_candidate` for live review).
Soda gap fell 87%. Lesson: **every suppression assumption should name the
condition that revokes it**.

### 4. Checks and tasks race for one refill (sim/service parity)
A truck `check` maturing 10 min before its sibling task stole the
confirmation; the orphaned task sat open until `abandoned`. Fix
(`sim/loop.py`): one physical refill closes both. Lesson: any "verify then
fetch" two-step needs joint completion semantics.

### 5. LLM calls are stateless; history must be built, not assumed
Each `/reason` call carries only the current snapshot + 3 frozen few-shots.
The `llm_calls` table was even in `DAY_TABLES` — every restart wiped the
training data. Built instead (`doc/04` feedback loop): persistent history,
online restock labels (`done/adjusted/rejected/abandoned`), day-end
suppress labels (`suppressed_ok/regret`), per-trigger override/regret
metrics. Lesson: **log rows are not training rows** until something joins
outcomes to them.

### 6. Retrieval should match regimes, not rows (#2)
Precedent matching is trigger-scoped, scale-free (`shelf_pct, boh_cover,
velocity_ratio`), diversity-first (closest success + closest mistake), and
temporal (time-of-day distance + soft same-weekday bonus). Store id is
deliberately absent — the only cross-store difference is the demand
multiplier, which surfaces through velocities. Lesson: **normalize away
identity, match on situation**.

### 7. Stores are independent — so N stores replicate, not interact
All logic is per `(store, sku)` with no shared DC, fleet, or transfers.
Second store added volume (2x LLM rows, demand heterogeneity) but no new
behavior — hence the single-store default (`config/poc.yaml`), with the
core staying N-store generic. Lesson: **multi-store is only as valuable
as the shared constraint** (see idea 1).

### 8. Dashboard fragments go stale in non-obvious ways
The store selector lived inside an auto-refreshing fragment: switching
stores reran only the header while every panel kept the old store (fixed
with `st.rerun()`). Separately, fragments can double-mount during startup
churn (transient duplicate buttons; refresh cures it). And app code is
baked into images — host edits never reach the browser without a rebuild.
Lesson: fragments are view caches with sharp edges; treat them as such.

## Future improvement ideas

Ordered by value × effort as judged today.

### A. Shared DC inventory across stores (makes multi-store real)
Today each SKU's backroom is infinite-ish. Give the DC a finite daily case
budget allocated across stores by need (lowest cover first). Suddenly store
count matters, and allocation-under-scarcity is a first-class LLM use case
with natural labels (stockout cost vs waste). Seam: receipt publisher +
a `dc_inventory` table. Pairs with idea B.

### B. Truck fleet routing + transshipments
One fleet, N stores, time windows: which truck serves which store when, and
should overstocked Suburb feed starved Downtown? The `truck_now` button and
`evaluate_truck` are the seams. LLM trigger candidate: `fleet_choice`.

### C. Adaptive suppression (bandit on `suppress_until_min`)
The model already emits this knob; today it's fire-and-forget. Per
(trigger × SKU-profile) bandit: shorten after regret, lengthen after
wasted-trip precision misses. Lives in `decision/router.py`, needs no
model change, evaluated by the existing panel.

### D. Confidence-gated fallback
`confidence` is logged and displayed but never gates anything. Route
low-confidence verdicts to rule fallback (or to human review) and measure
whether override/regret improve. One-line seam in `route()`.

### E. Prompt A/B via `prompt_version`
v1 vs v2 compares directly on override/regret today, but only by manual
SQL. Add the version split to the dashboard panel so prompt iterations
read as experiments with scoreboards.

### F. Sim date advances per epoch (real weekdays)
`SIM_DATE` is fixed, so the weekday bonus is dormant in sim. Advance the
sim date per epoch (or per scenario) to generate genuine weekday regimes —
prerequisite for the weekday machinery to prove itself before production.

### G. Partial-case fetch
`cases_needed` returns 0 when BOH covers no full case, so a shelf sits at 0
next to 4 loose units all evening. Real associates shelve the loose units.
Seam: `zero_partial` reason + capped `apply_confirmation`. Small, real fill.

### H. Store fetched-cases on tasks
"Cases in" sums *tasked* cases; associate adjusts (±1) make it approximate.
Add `fetched_cases` to the confirm path + tasks table for exact delivered
units, exact override magnitudes, and waste math.

### I. RAG scale-up
Pool 20 → larger with recency decay; embedding similarity instead of
hand features; per-SKU-profile candidate pools. Only after C–E show the
loop helps — retrieval quality is bounded by label quality.

### J. Dashboard responsiveness + weekday coverage view
Halve fragment poll rates (task queue at 1s is the prime suspect) and add
a history-coverage strip (calls per weekday) so cold weekdays are visible
before they cost decisions.

### K. Daily LLM post-mortem — process improvement (people/ops)
A scheduled LLM pass over each finished day, answering "how did the
*operation* run?" rather than "how did the *system* run?". Inputs already
exist: task shown→completed delays by hour/SKU (`tasks.emit/done_sim_min`),
skip/adjust rates, suppress regrets, lost sales, override clusters.
Output is an ops memo for the store manager, and crucially some findings
are NOT system fixes: evening restocks averaging 41 min vs a 25-min target
means add floor coverage 17–20h; a 60% skip rate on bulk tasks means
retrain on bulk policy (or fix the debounce); zero confirmations 12:00–
13:00 means a lunch coverage gap. Seam: `day_done` trigger → aggregate →
dedicated prompt (new file, e.g. `v-postmortem.md`) → persisted memo +
dashboard panel, plus week-over-week trends. This is where associate
speed and missing feedback become *communicated, actionable* training
needs instead of silent metric drift.

### L. Instrumentation analytics — system improvement (tech/cost)
The mirror image of K, for engineers instead of managers. Detailed
telemetry first (per-service latencies, Kafka consumer lag, PG write
times, fragment query times, LLM latency/cost per trigger and model,
cache hit rates, fallback rates) into a metrics store; then the LLM
analyzes *that* data for system suggestions: widen cache buckets on low
hit rates, trim prompt context on slow triggers, investigate fallback
spikes, right-size the reasoner fleet per cost-per-decision. Same
mechanism as K (scheduled analysis + memo), different data, different
audience, different backlog. Keep the two prompts and panels separate —
an ops manager should never have to read about GC pauses, and an engineer
should never have to read about shift coverage.

### M. DC replenishment — own the `boh_empty` branch
Everything today optimizes shelf→backroom flow; a truly empty building is
labeled "DC problem" and abandoned there. Close the loop with order
proposals to the DC: per-SKU reorder points from demand rate × lead time +
safety stock, order batching by delivery schedule, waste-aware caps for
perishables. The `lost_sales` `boh_empty` reason becomes its scoreboard,
mirroring how `shelf_gap` scores the shelf side. This is the largest
unbuilt half of replenishment.

### N. Perishables, waste, and shrink
Milk overstocked is milk poured away — the current math has no cost of
*too much* stock, only of too little. Add expiry-aware fetch caps
(don't fetch what can't sell by code date), waste tracking alongside lost
sales, and shrink reconciliation (register sales ≠ inventory movement)
feeding the correction path instead of polluting velocity. Grocery
without this is a dry-goods system wearing a grocery costume.

### O. Labor-capacity scheduling (the system side of staffing)
Priority tiers assume infinite associate bandwidth; K observes staffing
gaps but the scheduler doesn't *model* them. Next step: WIP limits per
associate, shift/speed profiles instead of one flat 25-min delay, and
pick-path ordering (one trip, several tasks, sensible aisle sequence).
Per-tier SLAs (doc/02) become capacity-feasible promises instead of hopes.

### P. Promo effectiveness + substitution effects
The system *executes* promos but never *evaluates* them: lift vs baseline,
cannibalization of sibling SKUs, endcap waste. Feed post-promo analysis
back into promo planning (which SKUs deserve the endcap row?) and into
demand modeling (a milk stockout measurably lifts bread — today's
independent-SKU demand misses that cross-elasticity entirely).

### Q. Alerting, experimentation, and people-data care
Three small but load-bearing gaps: (a) **escalation** — repeated regrets
or aging P0s should page a manager, not wait for a dashboard glance;
(b) **system-level A/B** — E covers prompts; rules/SLA variants need the
same guardrailed experiment harness; (c) **sensitivity** — K's memos rate
human performance by name-able shifts. That data needs access control and
aggregation floors before it exists, not after.

## Mission boundary (scope gate for everything above)
This system exists to **reduce missed sales opportunities** — shelf gaps,
late restocks, wasted trips. An idea belongs here iff it serves that
mission. Demand forecasting, calendar effects, and planogram compliance
are out of scope by design, not oversight: forecasting is a separate
system (velocities + cover triggers already carry the intraday signal),
and facings belong to merchandising, not replenishment.

## Explicit non-goals (for now)
- Fine-tuning the local model: RAG + feedback captures most of the gain
  at POC label volumes; revisit past ~1k labeled outcomes.
- Forecasting, calendar effects, planogram compliance: see mission above.
