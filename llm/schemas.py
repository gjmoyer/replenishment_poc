"""Pydantic contracts for the LLM reasoner. Mirrors doc/04-llm-reasoner.md."""
from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

Trigger = Literal[
    "promo_ambiguous",
    "bulk_candidate_suppressed",
    "stale_zero",
    "boh_anomaly",
    "repeat_task",
]


class StateSnapshot(BaseModel):
    boh: int = Field(ge=0, description="Backroom+floor units (UNITS, not cases)")
    shelf_est: int = Field(ge=0, description="Estimated units on shelf+endcap (UNITS)")
    effective_capacity: int = Field(gt=0, description="shelf + endcap in UNITS")
    velocity_30m: float = Field(ge=0, description="Units per sim-minute, trailing 30m")
    velocity_120m: float = Field(ge=0, description="Baseline u/min, trailing 120m")


class ProductInfo(BaseModel):
    is_promo: bool = False
    is_bulk: bool = False
    case_size: int = Field(gt=0, description="Units per case")
    threshold_pct: float = Field(default=0.35, gt=0, lt=1)


class OpenTask(BaseModel):
    task_id: str = Field(min_length=1)
    cases: int = Field(ge=0)
    emit_min: int = Field(ge=0, description="sim-minutes since midnight")


SaleUnit = Annotated[int, Field(ge=0)]


class ReasonRequest(BaseModel):
    store_id: str = Field(min_length=1)
    sku: str = Field(min_length=1)
    sim_ts: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
    state: StateSnapshot
    product: ProductInfo
    trigger: Trigger
    recent_sales: list[SaleUnit] = Field(default_factory=list, max_length=10)
    open_task: OpenTask | None = None
    truck_eta: str | None = None


class ReasonDecision(BaseModel):
    """Strict LLM output. cases_override is in CASES; everything else is UNITS."""

    needs_restock: bool
    cases_override: int | None = Field(default=None, ge=0)
    confidence: float = Field(ge=0, le=1)
    rationale: str = Field(min_length=1, max_length=500)
    suppress_until_min: int = Field(default=0, ge=0, le=180)
    missing_data: list[str] = Field(default_factory=list, max_length=10)

    @field_validator("suppress_until_min", mode="before")
    @classmethod
    def _null_suppress_to_zero(cls, v: object) -> object:
        # Qwen probe returned null; treat as 0 (see doc/04).
        return 0 if v is None else v


class ReasonMeta(BaseModel):
    model: str
    prompt_version: str
    latency_ms: int
    fallback: bool = False
    fallback_reason: str | None = None
    clamped: bool = False


class ReasonResponse(BaseModel):
    decision: ReasonDecision
    meta: ReasonMeta
