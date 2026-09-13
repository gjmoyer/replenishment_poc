-- Replenishment POC schema. Applied via docker-entrypoint-initdb.d.
-- Day tables are TRUNCATEd on sim restart (see sim/runner.py).
-- NOTE: decision/pg.py SCHEMA mirrors this file and runs at every boot,
-- so volumes created before a schema change still converge. Keep in sync.

CREATE TABLE IF NOT EXISTS shelf_state (
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  boh INT NOT NULL DEFAULT 0,
  shelf_est INT NOT NULL DEFAULT 0,
  effective_cap INT NOT NULL DEFAULT 0,
  case_size INT NOT NULL DEFAULT 1,
  is_promo BOOLEAN NOT NULL DEFAULT FALSE,
  is_bulk BOOLEAN NOT NULL DEFAULT FALSE,
  velocity_30m DOUBLE PRECISION NOT NULL DEFAULT 0,
  velocity_120m DOUBLE PRECISION NOT NULL DEFAULT 0,
  zero_flag BOOLEAN NOT NULL DEFAULT FALSE,
  zero_since_min INT NULL,
  last_task_min INT NULL,
  open_task JSONB NULL,
  updated_wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (store_id, sku)
);

CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  emit_sim_min INT NOT NULL,
  action TEXT NOT NULL,             -- task | check
  reason TEXT NOT NULL,
  cases INT NOT NULL DEFAULT 0,
  source TEXT NOT NULL,             -- rule | llm | rule_fallback
  rationale TEXT NOT NULL DEFAULT '',
  confidence DOUBLE PRECISION NULL,
  status TEXT NOT NULL DEFAULT 'open',  -- open | done | rejected | abandoned | superseded
  wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  shelf_at_emit INT NULL,           -- shelf units when the task fired
  boh_at_emit INT NULL,             -- BOH units when the task fired
  done_sim_min INT NULL,
  priority INT NOT NULL DEFAULT 2   -- 0 stockout / 1 imminent / 2 routine
);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS shelf_at_emit INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS boh_at_emit INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS done_sim_min INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 2;

-- Unmet demand. shelf_gap = shelf empty but backroom stocked (the
-- replenishment miss: faster restocking recovers it). boh_empty = store
-- truly out (ordering/DC problem, not shelf execution).
CREATE TABLE IF NOT EXISTS lost_sales (
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  sim_min INT NOT NULL,
  units INT NOT NULL,
  reason TEXT NOT NULL,  -- shelf_gap | boh_empty
  PRIMARY KEY (store_id, sku, sim_min, reason)
);
CREATE INDEX IF NOT EXISTS tasks_store_status ON tasks (store_id, status);

CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY,
  sim_min INT NOT NULL,
  wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  store_id TEXT NOT NULL,
  sku TEXT NULL,
  type TEXT NOT NULL,   -- sale_burst | receipt | truck | task | check | suppress | confirm | abandon | llm | control
  message TEXT NOT NULL,
  source TEXT NULL      -- rule | llm | rule_fallback
);

CREATE TABLE IF NOT EXISTS llm_calls (
  id BIGSERIAL PRIMARY KEY,
  wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  trigger TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  latency_ms INT NOT NULL,
  fallback BOOLEAN NOT NULL DEFAULT FALSE,
  output JSONB NOT NULL,
  sim_min INT NOT NULL DEFAULT 0,
  epoch INT NOT NULL DEFAULT 1,
  needs_restock BOOLEAN NOT NULL DEFAULT FALSE,
  task_id TEXT NULL,
  input JSONB NULL,
  outcome TEXT NULL,          -- done | adjusted | rejected | abandoned | suppressed_ok | suppressed_regret
  outcome_sim_min INT NULL,
  outcome_units INT NULL      -- regret lost-units for suppress verdicts
);
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS sim_min INT NOT NULL DEFAULT 0;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS epoch INT NOT NULL DEFAULT 1;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS needs_restock BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS task_id TEXT NULL;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS input JSONB NULL;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS outcome TEXT NULL;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS outcome_sim_min INT NULL;
ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS outcome_units INT NULL;
CREATE INDEX IF NOT EXISTS llm_calls_task_id ON llm_calls (task_id);
CREATE INDEX IF NOT EXISTS llm_calls_trigger_wall ON llm_calls (trigger, wall);

CREATE TABLE IF NOT EXISTS sales_hist (
  store_id TEXT NOT NULL,
  sku TEXT NOT NULL,
  sim_min INT NOT NULL,
  units INT NOT NULL,
  PRIMARY KEY (store_id, sku, sim_min)
);

CREATE TABLE IF NOT EXISTS sim_control (
  id INT PRIMARY KEY CHECK (id = 1),
  sim_min INT NOT NULL DEFAULT 420,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  speed INT NOT NULL DEFAULT 60,
  seed INT NOT NULL DEFAULT 42,
  epoch INT NOT NULL DEFAULT 1,
  cmd TEXT NULL,          -- truck_now | burst | restart | step | scenario
  cmd_arg JSONB NULL,
  day_done BOOLEAN NOT NULL DEFAULT FALSE,
  flags JSONB NOT NULL DEFAULT '{"silent_oos": true}'::jsonb
);
INSERT INTO sim_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Exactly-once guard: every consumed Kafka event claims its event_id here.
-- Survives restarts (unlike in-memory sets) so redeliveries can't double-apply.
CREATE TABLE IF NOT EXISTS processed (
  event_id TEXT PRIMARY KEY,
  wall TIMESTAMPTZ NOT NULL DEFAULT now()
);
