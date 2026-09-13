"""Catalog: SKUs + stores. Static data, no RNG — determinism comes from seeds
in the demand model. Mirrors doc/01-data-contracts.md product_master.

Filler SKUs scale via build_catalog(n_fillers): default 10-SKU catalog keeps
the M2 replay tuned; the dashboard scale mode raises fillers for the 50-card
perf test (weights renormalize through relative popularity).
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from hashlib import sha256

from decision.rules import DEFAULT_THRESHOLD_PCT


@dataclass(frozen=True)
class Sku:
    sku: str
    name: str
    shelf_capacity_units: int
    case_size_units: int
    is_bulk: bool = False
    is_promo: bool = False
    threshold_pct: float = DEFAULT_THRESHOLD_PCT
    popularity: float = 10.0  # relative line-pick weight
    profile: str = "steady"  # steady | morning | evening | spiky
    opening_boh: int = 60
    multi_unit_prob: float = 0.0  # P(line rings 2 units instead of 1)
    pack: str = "unit"  # consumer-facing unit, e.g. "1 gal jug"
    price: float = 3.99  # retail price per consumer unit (USD)
    margin_pct: float = 0.25  # gross margin fraction (profit = price * margin)


CORE_SKUS: tuple[Sku, ...] = (
    Sku("milk-1gal-001", "Milk 1gal", 24, 6, popularity=20, opening_boh=180,
        multi_unit_prob=0.15, pack="1 gal jug", price=4.29, margin_pct=0.18),
    Sku("soda-12pk-101", "Soda 12-pack (PROMO)", 48, 12, is_promo=True,
        popularity=14, profile="spiky", opening_boh=225, multi_unit_prob=0.15,
        pack="12-pack", price=8.99, margin_pct=0.25),
    Sku("dogfood-40lb-007", "Bulk Dog Food 40lb", 6, 2, is_bulk=True,
        popularity=0.8, opening_boh=30, pack="40 lb bag", price=54.99, margin_pct=0.32),
    Sku("bread-loaf-003", "Bread Loaf", 30, 10, popularity=12,
        profile="morning", opening_boh=135, pack="20 oz loaf", price=3.49, margin_pct=0.30),
    Sku("eggs-12ct-005", "Eggs Dozen", 36, 12, popularity=12,
        profile="morning", opening_boh=162, pack="12 ct carton", price=5.49, margin_pct=0.18),
)

BASE_FILLERS: tuple[Sku, ...] = (
    Sku("cereal-001", "Cereal", 24, 6, popularity=8,
        profile="morning", opening_boh=108, pack="18 oz box", price=4.99, margin_pct=0.28),
    Sku("pasta-001", "Pasta", 30, 10, popularity=6, opening_boh=90,
        pack="16 oz box", price=1.79, margin_pct=0.30),
    Sku("beans-001", "Canned Beans", 36, 12, popularity=5, opening_boh=108,
        pack="15 oz can", price=1.29, margin_pct=0.32),
    Sku("chips-001", "Chips", 30, 6, popularity=7,
        profile="evening", opening_boh=90, pack="13 oz bag", price=5.49, margin_pct=0.28),
    Sku("coffee-001", "Coffee", 20, 5, popularity=9,
        profile="morning", opening_boh=90, pack="12 oz bag", price=11.99, margin_pct=0.30),
)

ALL_SKUS: tuple[Sku, ...] = CORE_SKUS + BASE_FILLERS

_FILLER_NAMES = (
    "Soup", "Rice", "Oats", "Peanut Butter", "Jam", "Tuna", "Crackers",
    "Cookies", "Tea", "Sugar", "Flour", "Oil", "Vinegar", "Salsa", "Yogurt",
    "Cheese", "Butter", "Juice", "Apples", "Bananas", "Carrots", "Onions",
    "Potatoes", "Tomatoes", "Lettuce", "Chicken", "Beef", "Pork", "Fish",
    "Shrimp", "Ice Cream", "Frozen Peas", "Pizza", "Detergent", "Soap",
    "Shampoo", "Toothpaste", "Paper Towels", "Napkins", "Trash Bags",
)


def build_catalog(n_fillers: int = 5) -> tuple[Sku, ...]:
    """Catalog with generated filler SKUs (deterministic, seed 7).

    Generated fillers are deliberately light (low popularity, mid caps) so
    scale mode exercises render/perf paths without drowning core-SKU dynamics.
    """
    if n_fillers <= len(BASE_FILLERS):
        return CORE_SKUS + BASE_FILLERS[:n_fillers]
    rng = random.Random(int.from_bytes(sha256(b"filler-catalog-7").digest()[:8], "big"))
    extra: list[Sku] = []
    for i, name in enumerate(_FILLER_NAMES[: n_fillers - len(BASE_FILLERS)]):
        slug = name.lower().replace(" ", "-")
        extra.append(
            Sku(
                sku=f"gen-{slug}-{i:02d}",
                name=name,
                shelf_capacity_units=rng.choice([20, 24, 30, 36]),
                case_size_units=rng.choice([5, 6, 10, 12]),
                popularity=round(rng.uniform(2.0, 5.0), 1),
                profile=rng.choice(["steady", "steady", "morning", "evening"]),
                opening_boh=rng.choice([48, 60, 72]),
            )
        )
    return CORE_SKUS + BASE_FILLERS + tuple(extra)


@dataclass(frozen=True)
class Store:
    store_id: str
    name: str
    rate_mult: float = 1.0


STORES: tuple[Store, ...] = (
    Store("store-001", "Downtown", 1.2),
    Store("store-002", "Suburb", 0.9),
)


def stores_from_config(entries: tuple[tuple[str, str, float], ...]) -> tuple[Store, ...]:
    """Config-driven store list (config/poc.yaml sim.stores)."""
    return tuple(Store(store_id=sid, name=name, rate_mult=mult) for sid, name, mult in entries)
