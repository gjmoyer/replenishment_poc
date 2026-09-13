"""sim runner: live multi-store demand publisher (M3).

Owns the sim clock. Each tick it advances sim time per the control table
(speed/paused), generates shopper sales + scheduled receipts + trucks, and
publishes boh_updates / truck_arrivals to Redpanda. It tracks BOH per key
(opening + receipts - sales) so demand can never sell air — shelf estimation
lives in the decision service, not here.

Dashboard commands arrive via sim_control.cmd: truck_now | burst |
restart | step | scenario. Executed once, then cleared.

Run: python -m sim.runner
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass

from core.config import get_config
from decision.bus import Producer, wait_broker
from decision.pg import Store, wait_pg
from sim.catalog import Store as StoreDef
from sim.catalog import build_catalog, stores_from_config
from sim.clock import to_iso
from sim.scenarios import ScenarioFlags
from sim.shopper import ShopperSim
from sim.truck import TruckSim

log = logging.getLogger("sim.runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RECEIPT_WAVES_MIN = (8 * 60, 15 * 60)
RECEIPT_SPIKE_MIN = 12 * 60
RECEIPT_WAVE_SIZE = 4


@dataclass
class KeyState:
    boh: int


class Runner:
    def __init__(self) -> None:
        self.cfg = get_config()
        wait_broker(self.cfg.kafka.broker)
        self.store = Store(wait_pg(self.cfg.pg.url))
        self.producer = Producer(self.cfg.kafka.broker)
        self.flags = ScenarioFlags()
        self.stores: tuple[StoreDef, ...] = stores_from_config(self.cfg.sim.stores)
        self.skus = {s.sku: s for s in build_catalog()}
        self.boh: dict[tuple[str, str], KeyState] = {}
        self.shoppers: dict[str, ShopperSim] = {}
        self.trucks: dict[str, TruckSim] = {}
        self.pending_receipts: list[tuple[int, str, tuple[str, ...]]] = []
        self.epoch = 1
        self.reset_state()

    def reset_state(self) -> None:
        ctl = self.store.read_control()
        seed = ctl["seed"] or self.cfg.sim.seed
        flags = ctl["flags"] or {}
        self.flags = ScenarioFlags(
            promo_rush=bool(flags.get("promo_rush")),
            bulk_thrash=bool(flags.get("bulk_thrash")),
            silent_oos=bool(flags.get("silent_oos", True)),
            receipt_spike=bool(flags.get("receipt_spike")),
            silent_sku=str(flags.get("silent_sku", "eggs-12ct-005")),
        )
        self.boh = {
            (st.store_id, s.sku): KeyState(s.opening_boh)
            for st in self.stores for s in self.skus.values()
        }
        # Plain recreate (no epoch bump) must NOT reseed to opening while the
        # day is mid-flight — adopt live PG BOH so the next delta is real.
        try:
            with self.store.conn.cursor() as cur:
                cur.execute("SELECT store_id, sku, boh FROM shelf_state")
                for sid, sku, boh in cur.fetchall():
                    if (sid, sku) in self.boh:
                        self.boh[(sid, sku)].boh = boh
        except Exception as e:
            log.warning("boh resume failed, using openings: %r", e)
        self.shoppers = {
            st.store_id: ShopperSim(st, seed, self.flags, tuple(self.skus.values()))
            for st in self.stores
        }
        self.trucks = {
            st.store_id: TruckSim(
                store_id=st.store_id, schedule_min=self.cfg.sim.trucks_min, seed=seed)
            for st in self.stores
        }
        self.pending_receipts = []
        self.epoch = ctl["epoch"]

    # -- publishers --------------------------------------------------------

    def pub_boh(self, store_id: str, sku: str, sim_min: int, new_boh: int,
                reason: str) -> None:
        old = self.boh[(store_id, sku)].boh
        self.boh[(store_id, sku)].boh = new_boh
        self.producer.send("boh_updates", f"{store_id}:{sku}", {
            "store_id": store_id, "sku": sku, "sim_ts": to_iso(sim_min),
            "wall_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "boh": new_boh, "delta": new_boh - old, "reason": reason,
            "epoch": self.epoch,
            "event_id": uuid.uuid4().hex,
        })

    def pub_truck(self, store_id: str, sim_min: int, manifest: list[str]) -> None:
        self.producer.send("truck_arrivals", store_id, {
            "store_id": store_id, "sim_ts": to_iso(sim_min),
            "wall_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "truck_id": f"truck-{sim_min}",
            "manifest_skus": manifest,
            "epoch": self.epoch,
            "event_id": uuid.uuid4().hex,
        })

    # -- tick stages --------------------------------------------------------

    def shelf_snapshot(self) -> dict[tuple[str, str], int]:
        """Decision-service shelf estimates, for shelf-aware demand.

        Missing row = decision hasn't initialized this key yet: callers must
        fall back to BOH-only clamping, never record phantom losses.
        """
        try:
            with self.store.conn.cursor() as cur:
                cur.execute("SELECT store_id, sku, shelf_est FROM shelf_state")
                return {(r[0], r[1]): r[2] for r in cur.fetchall()}
        except Exception as e:
            log.warning("shelf snapshot failed: %r", e)
            return {}

    def stage_sales(self, now: int) -> None:
        shelf = self.shelf_snapshot()
        for st in self.stores:
            for sku, units in self.shoppers[st.store_id].gen_sales(now).items():
                key = (st.store_id, sku)
                if key not in shelf:
                    sellable = min(units, self.boh[key].boh)
                    if sellable > 0:
                        self.pub_boh(st.store_id, sku, now, self.boh[key].boh - sellable, "sale")
                    continue
                # A shelf can't hand over more than it holds; the remainder
                # is unmet demand, split by root cause (see lost_sales).
                sellable = min(units, self.boh[key].boh, shelf[key])
                lost = units - sellable
                if lost > 0:
                    reason = "shelf_gap" if self.boh[key].boh > 0 else "boh_empty"
                    self.store.add_loss(st.store_id, sku, now, lost, reason)
                if sellable <= 0:
                    continue
                self.pub_boh(st.store_id, sku, now, self.boh[key].boh - sellable, "sale")

    def receipt(self, store_id: str, sku: str, now: int) -> None:
        key = (store_id, sku)
        add = self.skus[sku].case_size_units * self.cfg.sim.receipt_cases
        self.pub_boh(store_id, sku, now, self.boh[key].boh + add, "receipt")

    def stage_receipts(self, now: int) -> None:
        if now in RECEIPT_WAVES_MIN or (self.flags.receipt_spike and now == RECEIPT_SPIKE_MIN):
            for st in self.stores:
                # Same days-of-supply ranking as sim/loop.py (parity): waves
                # target lowest BOH/popularity, not lowest absolute BOH.
                by_boh = sorted(self.skus.values(),
                                key=lambda s: self.boh[(st.store_id, s.sku)].boh
                                / max(s.popularity, 0.1))
                for s in by_boh[:RECEIPT_WAVE_SIZE]:
                    self.receipt(st.store_id, s.sku, now)
        for _, store_id, skus in [p for p in self.pending_receipts if p[0] <= now]:
            for sku in skus:
                self.receipt(store_id, sku, now)
        self.pending_receipts = [p for p in self.pending_receipts if p[0] > now]

    def _need_goods_skus(self, store_id: str) -> list[str]:
        """Manifest candidates: SKUs whose backroom can't cover one case.

        Shelf-zero with stock is deliberately EXCLUDED — it needs an
        associate fetch, not truck goods (the arrival itself re-checks
        those as a wake-up call)."""
        try:
            with self.store.conn.cursor() as cur:
                cur.execute(
                    "SELECT sku FROM shelf_state WHERE store_id=%s AND boh < case_size",
                    (store_id,),
                )
                return [r[0] for r in cur.fetchall()]
        except Exception as e:
            log.warning("goods query failed, falling back to BOH: %r", e)
            return [sku for (sid, sku), ks in self.boh.items()
                    if sid == store_id and ks.boh < self.skus[sku].case_size_units]

    def stage_trucks(self, now: int) -> None:
        for st in self.stores:
            need_goods = self._need_goods_skus(st.store_id)
            manifest = self.trucks[st.store_id].manifest_at(now, need_goods, list(self.skus))
            if manifest is None:
                continue
            self.pub_truck(st.store_id, now, manifest)
            rmin = self.trucks[st.store_id].receipt_min(now)
            self.pending_receipts.append((rmin, st.store_id, tuple(manifest)))

    def stage_silent_drain(self, now: int) -> None:
        # Silent OOS is shelf-empty. The runner models it as a recount
        # correction (reason="correction", NOT a sale): the decision service
        # drops the shelf to zero without polluting velocity/sales history.
        if self.flags.silent_oos and now == self.flags.silent_drain_min:
            for st in self.stores:
                key = (st.store_id, self.flags.silent_sku)
                if self.boh[key].boh > 0:
                    self.pub_boh(st.store_id, self.flags.silent_sku, now, 0, "correction")

    def tick(self, now: int) -> None:
        self.stage_sales(now)
        self.stage_receipts(now)
        self.stage_trucks(now)
        self.stage_silent_drain(now)
        self.producer.flush()

    # -- commands ------------------------------------------------------------

    def run_command(self, cmd: str, arg: dict, now: int) -> None:
        if cmd == "truck_now":
            store_id = arg.get("store_id", "store-001")
            skus = tuple(arg.get("skus", []))
            self.trucks[store_id].send_now(now, skus)
        elif cmd == "burst":
            store_id, sku = arg.get("store_id", "store-001"), arg.get("sku", "")
            if sku not in self.skus:
                return
            key = (store_id, sku)
            want = int(arg.get("units", 10))
            shelf = self.shelf_snapshot().get(key)
            cap = self.boh[key].boh if shelf is None else min(self.boh[key].boh, shelf)
            units = min(want, cap)
            if want > units and shelf is not None:
                reason = "shelf_gap" if self.boh[key].boh > 0 else "boh_empty"
                self.store.add_loss(store_id, sku, now, want - units, reason)
            if units > 0:
                self.pub_boh(store_id, sku, now, self.boh[key].boh - units, "sale")
                self.producer.flush()
        elif cmd == "scenario":
            flags = dict(arg.get("flags", {}))
            # TRUNCATE before the epoch bump: if the decision service polls
            # between the two, it must see EITHER old-epoch rows OR nothing
            # (it skips unknown keys) — never a half-seeded day.
            self.store.truncate_day()
            with self.store.conn.cursor() as cur:
                cur.execute(
                    "UPDATE sim_control SET flags=%s, sim_min=%s,"
                    " day_done=false, epoch=epoch+1, cmd=NULL WHERE id=1",
                    (json.dumps({**self.flags.__dict__, **flags}), self.cfg.sim.open_min),
                )
            self.reset_state()
            log.info("scenario restart with flags %s (epoch %s)", flags, self.epoch)
        elif cmd == "step":
            n = max(int(arg.get("n", 15)), 0)
            now = self.store.read_control()["sim_min"]
            for i in range(n):
                if now + i >= self.cfg.sim.close_min:
                    break
                self.tick(now + i)
            with self.store.conn.cursor() as cur:
                cur.execute(
                    "UPDATE sim_control SET sim_min=LEAST(%s, %s) WHERE id=1",
                    (now + n, self.cfg.sim.close_min),
                )
        elif cmd == "restart":
            self.store.truncate_day()
            with self.store.conn.cursor() as cur:
                cur.execute(
                    "UPDATE sim_control SET sim_min=%s, day_done=false,"
                    " epoch=epoch+1, cmd=NULL WHERE id=1",
                    (self.cfg.sim.open_min,),
                )
            self.reset_state()
            log.info("day restarted (epoch %s)", self.epoch)
        # 'step' handled by the main loop via sim_min update.

    def clear_cmd(self) -> None:
        with self.store.conn.cursor() as cur:
            cur.execute("UPDATE sim_control SET cmd=NULL, cmd_arg=NULL WHERE id=1")

    # -- main loop -------------------------------------------------------------

    def run(self) -> None:
        log.info("sim runner live")
        while True:
            ctl = self.store.read_control()
            if ctl["epoch"] != self.epoch:
                self.reset_state()
                continue
            if ctl["cmd"]:
                try:
                    self.run_command(ctl["cmd"], ctl["cmd_arg"] or {}, ctl["sim_min"])
                except Exception:
                    log.exception("command %s failed", ctl["cmd"])
                self.clear_cmd()
                continue
            if ctl["day_done"] or ctl["sim_min"] >= self.cfg.sim.close_min:
                if not ctl["day_done"]:
                    with self.store.conn.cursor() as cur:
                        cur.execute("UPDATE sim_control SET day_done=true WHERE id=1")
                    log.info("day complete")
                time.sleep(1)
                continue
            if ctl["paused"]:
                time.sleep(0.5)
                continue
            now = ctl["sim_min"]
            self.tick(now)
            with self.store.conn.cursor() as cur:
                cur.execute("UPDATE sim_control SET sim_min=%s WHERE id=1", (now + 1,))
            interval = 60.0 / max(ctl["speed"], 1)
            time.sleep(min(interval, 5.0))


def main() -> None:
    Runner().run()


if __name__ == "__main__":
    main()
