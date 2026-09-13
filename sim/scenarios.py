"""Scenario flags: one-click demo modifiers (doc/03). Composable; all default off
except silent_oos, which the M2 replay needs for the truck-rescue assertion."""
from __future__ import annotations

from dataclasses import dataclass

MORNING_BOOST = 2.5
MORNING_OFF = 0.4
MORNING_CUTOFF_MIN = 10 * 60
EVENING_BOOST = 2.0
EVENING_OFF = 0.7
EVENING_START_MIN = 17 * 60
SPIKY_BOOST = 2.2
SPIKY_START_MIN = 17 * 60
SPIKY_END_MIN = 19 * 60
PROMO_RUSH_MULT = 3.0
PROMO_RUSH_START_MIN = 18 * 60
PROMO_RUSH_END_MIN = 20 * 60
BULK_THRASH_MULT = 3.0
BULK_THRASH_START_MIN = 10 * 60
BULK_THRASH_END_MIN = 12 * 60
SILENT_DRAIN_MIN = 13 * 60


@dataclass
class ScenarioFlags:
    promo_rush: bool = False  # 18:00-20:00 promo weight 3x
    bulk_thrash: bool = False  # dogfood weight 3x for 2h from 10:00
    silent_oos: bool = True  # drain eggs at 13:00, mute its demand after
    receipt_spike: bool = False  # extra receipt wave at 12:00

    silent_sku: str = "eggs-12ct-005"
    silent_drain_min: int = SILENT_DRAIN_MIN


def profile_mult(profile: str, minute: int, flags: ScenarioFlags,
                 is_promo: bool = False, is_bulk: bool = False) -> float:
    """Time-of-day weight multiplier for a SKU profile."""
    if profile == "morning":
        m = MORNING_BOOST if minute < MORNING_CUTOFF_MIN else MORNING_OFF
    elif profile == "evening":
        m = EVENING_BOOST if minute >= EVENING_START_MIN else EVENING_OFF
    elif profile == "spiky":
        m = SPIKY_BOOST if SPIKY_START_MIN <= minute < SPIKY_END_MIN else 1.0
    else:
        m = 1.0
    rush = PROMO_RUSH_START_MIN <= minute < PROMO_RUSH_END_MIN
    # Rush hour lifts the whole promo endcap row, not one SKU (doc/03).
    if flags.promo_rush and is_promo and rush:
        m *= PROMO_RUSH_MULT
    thrash = BULK_THRASH_START_MIN <= minute < BULK_THRASH_END_MIN
    if flags.bulk_thrash and is_bulk and thrash:
        m *= BULK_THRASH_MULT
    return m


def muted(sku: str, minute: int, flags: ScenarioFlags) -> bool:
    """Silent-OOS SKU gets no demand after its drain minute."""
    return (
        flags.silent_oos
        and sku == flags.silent_sku
        and minute >= flags.silent_drain_min
    )
