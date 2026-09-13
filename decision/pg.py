"""Postgres persistence for the decision service + dashboard reads."""
from __future__ import annotations

import json
import logging
import time
from typing import Any

log = logging.getLogger("decision.pg")

DAY_TABLES = ("sales_hist", "tasks", "events", "llm_calls", "shelf_state")

SCHEMA = """
CREATE TABLE IF NOT EXISTS shelf_state (
  store_id TEXT NOT NULL, sku TEXT NOT NULL,
  boh INT NOT NULL DEFAULT 0, shelf_est INT NOT NULL DEFAULT 0,
  effective_cap INT NOT NULL DEFAULT 0, case_size INT NOT NULL DEFAULT 1,
  is_promo BOOLEAN NOT NULL DEFAULT FALSE, is_bulk BOOLEAN NOT NULL DEFAULT FALSE,
  velocity_30m DOUBLE PRECISION NOT NULL DEFAULT 0,
  velocity_120m DOUBLE PRECISION NOT NULL DEFAULT 0,
  zero_flag BOOLEAN NOT NULL DEFAULT FALSE, zero_since_min INT NULL,
  last_task_min INT NULL, open_task JSONB NULL,
  updated_wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (store_id, sku)
);
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, sku TEXT NOT NULL,
  emit_sim_min INT NOT NULL, action TEXT NOT NULL, reason TEXT NOT NULL,
  cases INT NOT NULL DEFAULT 0, source TEXT NOT NULL,
  rationale TEXT NOT NULL DEFAULT '', confidence DOUBLE PRECISION NULL,
  status TEXT NOT NULL DEFAULT 'open', wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  shelf_at_emit INT NULL, boh_at_emit INT NULL
);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS shelf_at_emit INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS boh_at_emit INT NULL;
CREATE INDEX IF NOT EXISTS tasks_store_status ON tasks (store_id, status);
CREATE TABLE IF NOT EXISTS events (
  id BIGSERIAL PRIMARY KEY, sim_min INT NOT NULL,
  wall TIMESTAMPTZ NOT NULL DEFAULT now(), store_id TEXT NOT NULL, sku TEXT NULL,
  type TEXT NOT NULL, message TEXT NOT NULL, source TEXT NULL
);
CREATE TABLE IF NOT EXISTS llm_calls (
  id BIGSERIAL PRIMARY KEY, wall TIMESTAMPTZ NOT NULL DEFAULT now(),
  store_id TEXT NOT NULL, sku TEXT NOT NULL, trigger TEXT NOT NULL,
  model TEXT NOT NULL, prompt_version TEXT NOT NULL, input_hash TEXT NOT NULL,
  latency_ms INT NOT NULL, fallback BOOLEAN NOT NULL DEFAULT FALSE, output JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS sales_hist (
  store_id TEXT NOT NULL, sku TEXT NOT NULL, sim_min INT NOT NULL, units INT NOT NULL,
  PRIMARY KEY (store_id, sku, sim_min)
);
CREATE TABLE IF NOT EXISTS sim_control (
  id INT PRIMARY KEY CHECK (id = 1), sim_min INT NOT NULL DEFAULT 420,
  paused BOOLEAN NOT NULL DEFAULT FALSE, speed INT NOT NULL DEFAULT 60,
  seed INT NOT NULL DEFAULT 42, epoch INT NOT NULL DEFAULT 1,
  cmd TEXT NULL, cmd_arg JSONB NULL, day_done BOOLEAN NOT NULL DEFAULT FALSE,
  flags JSONB NOT NULL DEFAULT '{"silent_oos": true}'::jsonb
);
INSERT INTO sim_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
CREATE TABLE IF NOT EXISTS processed (
  event_id TEXT PRIMARY KEY, wall TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def wait_pg(url: str, timeout_s: float = 60.0) -> Any:
    import psycopg

    deadline = time.time() + timeout_s
    while True:
        try:
            conn = psycopg.connect(url, autocommit=True)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except Exception as e:
            if time.time() > deadline:
                raise TimeoutError(f"postgres unreachable: {e!r}") from e
            log.info("waiting for postgres ...")
            time.sleep(2)


class Store:
    def __init__(self, conn: Any) -> None:
        self.conn = conn
        with conn.cursor() as cur:  # boot-time ensure: volumes outlive init.sql
            cur.execute(SCHEMA)

    def upsert_shelf(self, row: dict) -> None:
        open_json = json.dumps(row["open_task"]) if row.get("open_task") else None
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO shelf_state
                   (store_id, sku, boh, shelf_est, effective_cap, case_size,
                    is_promo, is_bulk, velocity_30m, velocity_120m,
                    zero_flag, zero_since_min, last_task_min, open_task)
                   VALUES (%(store_id)s, %(sku)s, %(boh)s, %(shelf_est)s,
                           %(effective_cap)s, %(case_size)s, %(is_promo)s, %(is_bulk)s,
                           %(velocity_30m)s, %(velocity_120m)s,
                           %(zero_flag)s, %(zero_since_min)s, %(last_task_min)s,
                           %(open_task)s)
                   ON CONFLICT (store_id, sku) DO UPDATE SET
                     boh=EXCLUDED.boh, shelf_est=EXCLUDED.shelf_est,
                     effective_cap=EXCLUDED.effective_cap, case_size=EXCLUDED.case_size,
                     is_promo=EXCLUDED.is_promo, is_bulk=EXCLUDED.is_bulk,
                     velocity_30m=EXCLUDED.velocity_30m, velocity_120m=EXCLUDED.velocity_120m,
                     zero_flag=EXCLUDED.zero_flag, zero_since_min=EXCLUDED.zero_since_min,
                     last_task_min=EXCLUDED.last_task_min, open_task=EXCLUDED.open_task,
                     updated_wall=now()""",
                {**row, "open_task": open_json},
            )

    def add_sale_hist(self, store_id: str, sku: str, sim_min: int, units: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sales_hist (store_id, sku, sim_min, units)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (store_id, sku, sim_min)
                   DO UPDATE SET units = sales_hist.units + EXCLUDED.units""",
                (store_id, sku, sim_min, units),
            )

    def add_task(self, task: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO tasks
                   (task_id, store_id, sku, emit_sim_min, action, reason,
                    cases, source, rationale, confidence, status,
                    shelf_at_emit, boh_at_emit)
                   VALUES (%(task_id)s, %(store_id)s, %(sku)s, %(emit_sim_min)s,
                           %(action)s, %(reason)s, %(cases)s, %(source)s,
                           %(rationale)s, %(confidence)s, %(status)s,
                           %(shelf_at_emit)s, %(boh_at_emit)s)
                   ON CONFLICT (task_id) DO NOTHING""",
                task,
            )

    def set_task_status(self, task_id: str, status: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status=%s WHERE task_id=%s", (status, task_id))

    def log_event(
        self,
        sim_min: int,
        store_id: str,
        sku: str | None,
        type: str,
        message: str,
        source: str | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO events (sim_min, store_id, sku, type, message, source)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                (sim_min, store_id, sku, type, message, source),
            )

    def add_llm_call(self, call: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO llm_calls
                   (store_id, sku, trigger, model, prompt_version,
                    input_hash, latency_ms, fallback, output)
                   VALUES (%(store_id)s, %(sku)s, %(trigger)s, %(model)s,
                           %(prompt_version)s, %(input_hash)s,
                           %(latency_ms)s, %(fallback)s, %(output)s)""",
                {**call, "output": json.dumps(call["output"])},
            )

    def claim_event(self, event_id: str | None) -> bool:
        """True if this event is new (claimed now). False = redelivery, skip.

        Single cross-restart idempotency guard for all consumers.
        """
        if not event_id:
            return True  # legacy/adhoc message without id: process once
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed(event_id) VALUES (%s)"
                " ON CONFLICT DO NOTHING RETURNING event_id",
                (event_id,),
            )
            return cur.fetchone() is not None

    def read_control(self) -> dict:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT sim_min, paused, speed, seed, epoch, cmd, cmd_arg,"
                " day_done, flags FROM sim_control WHERE id=1"
            )
            r = cur.fetchone()
            keys = ("sim_min", "paused", "speed", "seed", "epoch", "cmd",
                    "cmd_arg", "day_done", "flags")
            return dict(zip(keys, r, strict=True))

    def truncate_day(self) -> None:
        with self.conn.cursor() as cur:
            for t in DAY_TABLES:
                cur.execute(f"TRUNCATE {t}")
