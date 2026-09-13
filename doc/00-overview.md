# 00 — Overview

## Goal
Learn how to architect and develop an LLM-in-the-mix workflow for grocery shelf replenishment, via a runnable multi-store simulation.

The learner (grocery retail domain, building from scratch) wants to see a full 07:00–22:00 store day play out sped-up, watch shelves deplete, watch restock tasks fire, and manually act as the associate to restock.

## What the system does
1. Simulates shoppers buying products (BOH decreases per sale, published to Kafka).
2. Simulates store receipts (BOH increases) and truck arrivals (including silent out-of-stock case with no sales events).
3. Tracks estimated shelf quantity per `(store_id, sku)` from opening-full assumption.
4. Decides deterministically if a shelf needs restocking and how many cases to fetch.
5. Calls an LLM only for ambiguous exceptions (promo/endcap ambiguity, bulk thrash, zero-shelf + truck).
6. Shows per-store dashboard with shelf status + task queue.
7. Lets a human click "Restock done" to simulate the associate fetching cases.

## Non-goals for POC
- No real POS integration, no real handheld devices.
- No DC ordering / replenishment to backroom — only shelf replenishment (sales floor).
- No planogram vision, no weight sensors.
- No perishable expiry / markdown logic (may add later as exception example).
- No auth, no multi-user, no production hardening.

## Personas
- **Associate (simulated by user):** sees task (SKU, location, cases), fetches from backroom, confirms done on dashboard.
- **Manager (future):** asks why / overrides. POC only logs rationale + override button.
- **Learner/Dev (you):** runs sim, tunes thresholds, iterates prompts, watches metrics.

## Store-day model
- Hours: 07:00–22:00 sim time (15 sim-hours).
- Speedup: configurable, default `60x` → 1 sim-minute = 1 wall-second → full day in ~15 wall-minutes. Also support `30x` (~30 min) and `120x` (~7.5 min) + pause/step.
- Opening assumption: all shelves fully stocked at 07:00. `shelf_est = shelf_capacity` (or `effective_capacity` for promo, see `02-rules-engine.md`).
- Sim clock is central: all publishers and dashboard use sim time, not wall time.

## Success definition
A reviewer can: pick a store on the dashboard, press Play, watch normal SKUs deplete, watch a promo SKU behave differently (endcap), watch a bulk SKU (dog food) not thrash, watch a zero-shelf SKU get revived by a truck event, click restock to clear tasks, and read LLM rationales for exception cases only.
