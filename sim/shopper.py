"""ShopperSim: per-store Poisson demand. Deterministic via sha256 seeds.

Emits batched (sku -> units) sales per sim-minute tick. Rates follow the
doc/03 day curve, scaled down ~5x from real-store volumes so a 15-min demo
shows a believable handful of restocks instead of hundreds.
"""
from __future__ import annotations

import math
import random
from collections import Counter
from hashlib import sha256

from .catalog import Sku, Store, build_catalog
from .scenarios import ScenarioFlags, muted, profile_mult

# (start_min, baskets_per_min) day curve, store_mult applied on top.
DAY_CURVE: tuple[tuple[int, float], ...] = (
    (7 * 60, 0.30),
    (9 * 60, 0.60),
    (12 * 60, 0.90),
    (14 * 60, 0.70),
    (17 * 60, 1.10),
    (19 * 60, 0.50),
)

MAX_LINES_BASE = 1  # every basket has at least 1 line
P_SECOND_LINE = 0.3
P_THIRD_LINE = 0.1
P_FOURTH_LINE = 0.03  # doc/03: baskets pick 1-4 SKUs


class ShopperSim:
    def __init__(
        self,
        store: Store,
        seed: int,
        flags: ScenarioFlags,
        skus: tuple[Sku, ...] | None = None,
    ) -> None:
        self.store = store
        # NOTE: builtin hash() is salted per process (str) — sha256 keeps
        # --seed 42 replayable across processes/machines.
        digest = sha256(f"{seed}:{store.store_id}".encode()).digest()
        self.rng = random.Random(int.from_bytes(digest[:8], "big"))
        self.flags = flags
        self.skus: tuple[Sku, ...] = skus if skus is not None else build_catalog()

    def rate(self, minute: int) -> float:
        base = DAY_CURVE[0][1]
        for start, r in DAY_CURVE:
            if minute >= start:
                base = r
        return base * self.store.rate_mult

    def _poisson(self, lam: float) -> int:
        # Knuth; lam <= ~1.5 so this is cheap.
        if lam <= 0:
            return 0
        cutoff = math.exp(-lam)
        k, prob = 0, 1.0
        while prob > cutoff:
            k += 1
            prob *= self.rng.random()
        return k - 1

    def _pick_sku(self, minute: int) -> Sku | None:
        weights = [
            0.0 if muted(s.sku, minute, self.flags)
            else s.popularity * profile_mult(s.profile, minute, self.flags, s.sku)
            for s in self.skus
        ]
        total = sum(weights)
        if total <= 0:
            return None  # everything muted — no sale (never sell a muted SKU)
        x = self.rng.random() * total
        acc = 0.0
        for s, w in zip(self.skus, weights, strict=True):
            acc += w
            if x < acc:
                return s
        return self.skus[-1]

    def gen_sales(self, minute: int) -> Counter:
        """Return Counter {sku: units sold} for one sim-minute."""
        out: Counter = Counter()
        baskets = self._poisson(self.rate(minute))
        for _ in range(baskets):
            lines = (
                MAX_LINES_BASE
                + (1 if self.rng.random() < P_SECOND_LINE else 0)
                + (1 if self.rng.random() < P_THIRD_LINE else 0)
                + (1 if self.rng.random() < P_FOURTH_LINE else 0)
            )
            for _ in range(lines):
                s = self._pick_sku(minute)
                if s is None:
                    continue
                units = 2 if self.rng.random() < s.multi_unit_prob else 1
                out[s.sku] += units
        return out
