"""Unit tests for llm schemas + prompt. Live-model test hits LM Studio :8081.

Run:  uv run pytest -q
Live: LMSTUDIO_LIVE=1 uv run pytest -q  (requires LM Studio server up)
"""

import json
import os

import pytest

from llm.schemas import ReasonDecision, ReasonRequest

PROMO_CTX = {
    "store_id": "store-001",
    "sku": "soda-12pk-101",
    "sim_ts": "2026-01-05T18:20",
    "state": {
        "boh": 60,
        "shelf_est": 18,
        "effective_capacity": 72,
        "velocity_30m": 1.8,
        "velocity_120m": 0.6,
    },
    "product": {"is_promo": True, "is_bulk": False, "case_size": 12, "threshold_pct": 0.35},
    "trigger": "promo_ambiguous",
    "recent_sales": [2, 1, 3, 2, 1],
    "open_task": None,
    "truck_eta": None,
}


def test_request_schema_validates():
    req = ReasonRequest.model_validate(PROMO_CTX)
    assert req.state.boh == 60
    assert req.product.case_size == 12


def test_null_suppress_coerced_to_zero():
    # Qwen probe returned "suppress_until_min": null — must not 422.
    d = ReasonDecision.model_validate(
        {
            "needs_restock": True,
            "cases_override": None,
            "confidence": 0.85,
            "rationale": "Shelf holds ~18 of 72 units.",
            "suppress_until_min": None,
            "missing_data": [],
        }
    )
    assert d.suppress_until_min == 0


def test_prompt_hardens_units_language():
    text = open("llm/prompts/v1.md").read()
    assert "UNITS" in text and "cases_override" in text
    assert "velocity_30m" in text  # promo trigger must cite velocity


def test_clamp_math():
    # mirrors service guardrail: 60 BOH / 12 per case = max 5 cases
    assert 60 // 12 == 5


def test_extract_json_tolerates_fences_and_prose():
    from llm.service import extract_json

    assert json.loads(extract_json('```json\n{"a": 1}\n```')) == {"a": 1}
    assert json.loads(extract_json('Here is the JSON: {"a": 1} done')) == {"a": 1}
    with pytest.raises(ValueError):
        extract_json("no object here")


def test_clamp_decision_caps_to_boh_and_calculator():
    from llm.service import clamp_decision

    req = ReasonRequest.model_validate(PROMO_CTX)
    # BOH 60 / 12 = 5 max; calculator wants 5. Override 9 must clamp to 5.
    d = ReasonDecision.model_validate(
        {
            "needs_restock": True,
            "cases_override": 9,
            "confidence": 0.9,
            "rationale": "x",
            "suppress_until_min": 0,
            "missing_data": [],
        }
    )
    out, clamped = clamp_decision(d, req)
    assert clamped is True
    assert out.cases_override == 5


def test_clamp_decision_flips_restock_when_boh_empty():
    from llm.service import clamp_decision

    ctx = dict(PROMO_CTX)
    ctx["state"] = dict(PROMO_CTX["state"], boh=0)
    req = ReasonRequest.model_validate(ctx)
    d = ReasonDecision.model_validate(
        {
            "needs_restock": True,
            "cases_override": None,
            "confidence": 0.9,
            "rationale": "x",
            "suppress_until_min": 0,
            "missing_data": [],
        }
    )
    out, clamped = clamp_decision(d, req)
    assert clamped is True
    assert out.needs_restock is False


def test_schema_rejects_oversized_and_negative_inputs():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ReasonRequest.model_validate({**PROMO_CTX, "recent_sales": [1] * 11})
    with pytest.raises(ValidationError):
        ReasonRequest.model_validate({**PROMO_CTX, "store_id": ""})
    with pytest.raises(ValidationError):
        ReasonRequest.model_validate({**PROMO_CTX, "sim_ts": "yesterday"})


def test_cache_bucket_separates_velocities():
    from decision.router import LlmCache, ReasonContext

    def ctx(v30, v120):
        return ReasonContext(
            store_id="s",
            sku="k",
            sim_ts="2026-01-05T10:00",
            boh=60,
            shelf_est=18,
            effective_cap=72,
            case_size=12,
            is_promo=True,
            is_bulk=False,
            threshold_pct=0.35,
            velocity_30m=v30,
            velocity_120m=v120,
            trigger="promo_ambiguous",
            recent_sales=[],
            open_task=None,
            truck_eta=None,
        )

    assert LlmCache.bucket(ctx(1.8, 0.6)) != LlmCache.bucket(ctx(0.5, 0.6))


@pytest.mark.skipif(os.getenv("LMSTUDIO_LIVE") != "1", reason="needs LM Studio on :8081")
def test_live_reason_endpoint():
    from fastapi.testclient import TestClient

    from llm.service import app

    c = TestClient(app)
    r = c.post("/reason", json=PROMO_CTX)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "decision" in body and "meta" in body
    rationale = body["decision"]["rationale"].lower()
    assert "unit" in rationale, f"rationale must say units, got: {rationale!r}"
    assert "u/min" in rationale or "velocity" in rationale, (
        f"promo rationale must cite velocity, got: {rationale!r}"
    )
    assert body["decision"]["suppress_until_min"] is not None
