"""TruckSim: scheduled manifests + dock-to-backroom receipt delay.

Manifest always includes zero-shelf SKUs (silent-OOS rescue demo) plus
2 seeded filler SKUs. Seeds mix (seed, store_id, minute) so stores diverge.
Receipts for manifest SKUs land +5..15 sim-min later — the loop schedules
them so truck_zero tasks can upgrade from `check` to real cases. Ad-hoc
`send_now` is the dashboard button path; entries are consumed once then
pruned, and unknown SKUs are dropped at manifest build.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from hashlib import sha256


def _seeded(*parts: object) -> random.Random:
    # Stable across processes (builtin hash() is salted for str).
    digest = sha256(":".join(map(str, parts)).encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


@dataclass
class TruckSim:
    store_id: str = "store-001"
    schedule_min: tuple[int, ...] = (10 * 60 + 30, 14 * 60)
    receipt_delay_lo: int = 5
    receipt_delay_hi: int = 15
    seed: int = 42
    _adhoc: list[tuple[int, tuple[str, ...]]] = field(default_factory=list)

    def send_now(self, minute: int, skus: tuple[str, ...]) -> None:
        if minute < 0:
            raise ValueError(f"minute must be >= 0, got {minute}")
        self._adhoc.append((minute, skus))

    def manifest_at(
        self, minute: int, zero_skus: list[str], all_skus: list[str]
    ) -> list[str] | None:
        scheduled = minute in self.schedule_min
        adhoc_now = [skus for m, skus in self._adhoc if m == minute]
        self._adhoc = [(m, skus) for m, skus in self._adhoc if m > minute]
        if not scheduled and not adhoc_now:
            return None
        known = set(all_skus)
        manifest = [s for s in zero_skus if s in known]
        rng = _seeded(self.seed, self.store_id, minute)
        pool = [s for s in all_skus if s not in manifest]
        manifest += rng.sample(pool, min(2, len(pool)))
        for skus in adhoc_now:
            manifest += [s for s in skus if s in known and s not in manifest]
        return manifest

    def receipt_min(self, manifest_min: int) -> int:
        rng = _seeded(self.seed, self.store_id, "receipt", manifest_min)
        return manifest_min + rng.randint(self.receipt_delay_lo, self.receipt_delay_hi)
