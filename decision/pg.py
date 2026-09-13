"""Postgres persistence for the decision service + dashboard reads."""
from __future__ import annotations

import json
import logging
import time
from typing import Any

log = logging.getLogger("decision.pg")

DAY_TABLES = ("sales_hist", "tasks", "events", "shelf_state", "lost_sales",
              # llm_calls is deliberately NOT a day table: it is the persistent
              # feedback-loop history (doc/04). ~10s of rows/day, so no prune
              # job at POC scale. Outcomes are materialized into each row by
              # finalize_llm_outcomes() before the day tables are truncated.
              # processed is safe to wipe on restart: the epoch fence runs BEFORE
              # claim_event, so redelivered old-epoch messages are dropped without
              # needing their ids; same-day recreates never truncate.
              "processed")

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
   shelf_at_emit INT NULL, boh_at_emit INT NULL, done_sim_min INT NULL,
   priority INT NOT NULL DEFAULT 2
);
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS shelf_at_emit INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS boh_at_emit INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS done_sim_min INT NULL;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 2;
CREATE TABLE IF NOT EXISTS lost_sales (
  store_id TEXT NOT NULL, sku TEXT NOT NULL, sim_min INT NOT NULL,
  units INT NOT NULL, reason TEXT NOT NULL,
  PRIMARY KEY (store_id, sku, sim_min, reason)
);
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
  latency_ms INT NOT NULL, fallback BOOLEAN NOT NULL DEFAULT FALSE, output JSONB NOT NULL,
  sim_min INT NOT NULL DEFAULT 0, epoch INT NOT NULL DEFAULT 1,
  needs_restock BOOLEAN NOT NULL DEFAULT FALSE, task_id TEXT NULL,
  input JSONB NULL, outcome TEXT NULL, outcome_sim_min INT NULL,
  outcome_units INT NULL
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
                    shelf_at_emit, boh_at_emit, priority)
                   VALUES (%(task_id)s, %(store_id)s, %(sku)s, %(emit_sim_min)s,
                           %(action)s, %(reason)s, %(cases)s, %(source)s,
                           %(rationale)s, %(confidence)s, %(status)s,
                           %(shelf_at_emit)s, %(boh_at_emit)s,
                           %(priority)s)
                   ON CONFLICT (task_id) DO NOTHING""",
                {"priority": 2, **task},
            )

    def set_task_status(self, task_id: str, status: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status=%s WHERE task_id=%s", (status, task_id))

    def mark_done(self, task_id: str, sim_min: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status='done', done_sim_min=%s WHERE task_id=%s",
                (sim_min, task_id),
            )

    def add_loss(self, store_id: str, sku: str, sim_min: int, units: int, reason: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO lost_sales (store_id, sku, sim_min, units, reason)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (store_id, sku, sim_min, reason)
                   DO UPDATE SET units = lost_sales.units + EXCLUDED.units""",
                (store_id, sku, sim_min, units, reason),
            )

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

    def add_llm_call(self, call: dict) -> int | None:
        """Persist one reasoner call. Returns the row id (for task linking)."""
        row = {
            "sim_min": 0, "epoch": 1, "needs_restock": False,
            "task_id": None, "input": None,
            "outcome": None, "outcome_sim_min": None,
            "outcome_units": None,
            **call,
        }
        row["output"] = json.dumps(row["output"])
        row["input"] = json.dumps(row["input"]) if row["input"] is not None else None
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO llm_calls
                   (store_id, sku, trigger, model, prompt_version,
                    input_hash, latency_ms, fallback, output,
                    sim_min, epoch, needs_restock, task_id, input,
                    outcome, outcome_sim_min, outcome_units)
                   VALUES (%(store_id)s, %(sku)s, %(trigger)s, %(model)s,
                           %(prompt_version)s, %(input_hash)s,
                           %(latency_ms)s, %(fallback)s, %(output)s,
                           %(sim_min)s, %(epoch)s, %(needs_restock)s,
                           %(task_id)s, %(input)s,
                           %(outcome)s, %(outcome_sim_min)s, %(outcome_units)s)
                   RETURNING id""",
                row,
            )
            fetched = cur.fetchone()
            return fetched[0] if fetched else None

    def link_llm_task(self, call_id: int, task_id: str) -> None:
        """Attach the emitted task to its LLM call (restock path)."""
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE llm_calls SET task_id=%s WHERE id=%s",
                (task_id, call_id),
            )

    def recent_labeled_calls(self, trigger: str, limit: int = 20) -> list[dict]:
        """Labeled precedent for outcome-aware few-shots (doc/04 #2).

        Most recent labeled, non-fallback same-trigger rows. The caller
        (decision/history.py) scores them against the current state.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT input, needs_restock, outcome, sim_min,
                          output->>'rationale' AS rationale
                   FROM llm_calls
                   WHERE trigger=%s AND fallback=FALSE
                     AND outcome IS NOT NULL AND sim_min > 0
                     AND input IS NOT NULL
                   ORDER BY id DESC LIMIT %s""",
                (trigger, limit),
            )
            cols = [d[0] for d in cur.description]
            out = []
            for r in cur.fetchall():
                row = dict(zip(cols, r, strict=True))
                snap = row.pop("input") or {}
                if isinstance(snap, str):  # non-psycopg driver returned text
                    try:
                        snap = json.loads(snap)
                    except ValueError:
                        snap = {}
                out.append({**snap, "needs_restock": row["needs_restock"],
                            "outcome": row["outcome"],
                            "sim_min": row["sim_min"],
                            "rationale": row["rationale"] or ""})
            return out

    def set_llm_outcome(
        self, task_id: str, outcome: str, sim_min: int,
        units: int | None = None,
    ) -> None:
        """Label a call by its linked task. First label wins (terminal)."""
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE llm_calls SET outcome=%s, outcome_sim_min=%s,
                          outcome_units=COALESCE(%s, outcome_units)
                   WHERE task_id=%s AND outcome IS NULL""",
                (outcome, sim_min, units, task_id),
            )

    def finalize_llm_outcomes(self) -> None:
        """Materialize labels for the ending day BEFORE day tables truncate.

        Only rows with sim_min > 0 (post-feature rows of the live day) can
        still join tasks/lost_sales — older days were finalized by their own
        truncation, legacy rows stay NULL and are excluded from metrics.
        """
        from decision.feedback import REGRET_WINDOW_MAX, REGRET_WINDOW_MIN
        with self.conn.cursor() as cur:
            # Restock path: adopt the terminal task status. done/rejected
            # were set online with finer labels (done vs adjusted); anything
            # still NULL here never confirmed — the day ended on it.
            cur.execute(
                """UPDATE llm_calls c SET outcome=CASE
                       WHEN t.status='rejected' THEN 'rejected'
                       ELSE 'abandoned' END,
                     outcome_sim_min=COALESCE(t.done_sim_min, c.sim_min)
                   FROM tasks t
                   WHERE c.task_id = t.task_id
                     AND c.outcome IS NULL AND c.sim_min > 0"""
            )
            # Suppress path: regret iff lost sales landed in-window. Two steps
            # (a scalar subquery can't be referenced twice in one SET).
            cur.execute(
                """UPDATE llm_calls c
                   SET outcome_units = (
                         SELECT COALESCE(SUM(x.units), 0) FROM lost_sales x
                         WHERE x.store_id = c.store_id AND x.sku = c.sku
                           AND x.sim_min BETWEEN c.sim_min AND c.sim_min
                             + LEAST(GREATEST(COALESCE(
                                 (c.output->>'suppress_until_min')::int, 0),
                                 %s), %s)),
                       outcome_sim_min = c.sim_min + LEAST(GREATEST(COALESCE(
                         (c.output->>'suppress_until_min')::int, 0), %s), %s)
                   WHERE c.task_id IS NULL AND c.outcome IS NULL
                     AND c.sim_min > 0""",
                (REGRET_WINDOW_MIN, REGRET_WINDOW_MAX,
                 REGRET_WINDOW_MIN, REGRET_WINDOW_MAX),
            )
            cur.execute(
                """UPDATE llm_calls
                   SET outcome=CASE WHEN outcome_units > 0
                                      THEN 'suppressed_regret'
                                    ELSE 'suppressed_ok' END
                   WHERE task_id IS NULL AND outcome IS NULL AND sim_min > 0"""
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
        self.finalize_llm_outcomes()
        with self.conn.cursor() as cur:
            for t in DAY_TABLES:
                cur.execute(f"TRUNCATE {t}")
