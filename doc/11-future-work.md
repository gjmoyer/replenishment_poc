# 11 — Future Work: What We Learned + What Comes Next

Distilled from the Sept 2026 tuning + learning sessions. Each learning links
the evidence; each idea links the seam it plugs into.

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

## Explicit non-goals (for now)
- Fine-tuning the local model: RAG + feedback captures most of the gain
  at POC label volumes; revisit past ~1k labeled outcomes.
- Cross-day demand forecasting: velocities + cover triggers already carry
  the intraday signal; forecasting is a separate system.
- Real calendar effects (holidays, weather): no sim support, no labels.
