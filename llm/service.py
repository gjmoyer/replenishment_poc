"""llm-reasoner service: async FastAPI wrapper over LM Studio (OpenAI-compatible).

POST /reason  {ReasonRequest} -> {ReasonResponse envelope}
GET  /health  -> {status, model, prompt_version}

Config: core.config (config/poc.yaml + LLM_* env). Pure helpers
(extract_json, clamp_decision) are unit-tested without a model.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI
from pydantic import ValidationError

from core.config import get_config
from decision.rules import cases_needed

from .schemas import ReasonDecision, ReasonMeta, ReasonRequest, ReasonResponse

FALLBACK_CONFIDENCE = 0.5
FALLBACK_SUPPRESS_MIN = 15

log = logging.getLogger("llm-reasoner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parents[1]
app = FastAPI(title="llm-reasoner")


def _prompts_dir() -> Path:
    return ROOT / "llm" / "prompts"


@lru_cache(maxsize=8)
def load_system_prompt(version: str | None = None) -> str:
    ver = version or get_config().llm.prompt_version
    return (_prompts_dir() / f"{ver}.md").read_text(encoding="utf-8")


def extract_json(text: str) -> str:
    """Pull the first {...} object out of model output (fences/prose tolerant)."""
    t = text.strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t[3:]
        if "```" in t:
            t = t.rsplit("```", 1)[0]
    start, end = t.find("{"), t.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON object in model output")
    return t[start : end + 1]


def clamp_decision(decision: ReasonDecision, req: ReasonRequest) -> tuple[ReasonDecision, bool]:
    """Enforce doc/04: final quantity passes through cases_needed() + BOH.

    Returns (decision, clamped). A restock BOH cannot fill flips to suppress.
    """
    cap = min(
        req.state.boh // req.product.case_size,
        cases_needed(
            req.state.shelf_est,
            req.state.effective_capacity,
            req.product.case_size,
            req.state.boh,
        ),
    )
    clamped = False
    if decision.cases_override is not None and decision.cases_override > cap:
        decision = decision.model_copy(update={"cases_override": cap})
        clamped = True
    if decision.needs_restock and cap == 0:
        decision = decision.model_copy(
            update={"needs_restock": False, "cases_override": None, "suppress_until_min": 15}
        )
        clamped = True
    if decision.suppress_until_min > 90:
        decision = decision.model_copy(update={"suppress_until_min": 90})
        clamped = True
    return decision, clamped


def _fallback(reason: str, latency_ms: int) -> ReasonResponse:
    cfg = get_config()
    return ReasonResponse(
        decision=ReasonDecision(
            needs_restock=False,
            cases_override=None,
            confidence=FALLBACK_CONFIDENCE,
            rationale="LLM unavailable; defer to deterministic rules.",
            suppress_until_min=FALLBACK_SUPPRESS_MIN,
            missing_data=["llm_unavailable"],
        ),
        meta=ReasonMeta(
            model=cfg.llm.model,
            prompt_version=cfg.llm.prompt_version,
            latency_ms=latency_ms,
            fallback=True,
            fallback_reason=reason,
        ),
    )


def _log_call(req: ReasonRequest, resp: ReasonResponse, input_hash: str) -> None:
    log.info(
        json.dumps(
            {
                "event": "llm_call",
                "input_hash": input_hash,
                "store": req.store_id,
                "sku": req.sku,
                "trigger": req.trigger,
                "model": resp.meta.model,
                "prompt": resp.meta.prompt_version,
                "latency_ms": resp.meta.latency_ms,
                "fallback": resp.meta.fallback,
                "needs_restock": resp.decision.needs_restock,
                "confidence": resp.decision.confidence,
            }
        )
    )


@app.get("/health")
async def health() -> dict:
    cfg = get_config()
    return {"status": "ok", "model": cfg.llm.model, "prompt_version": cfg.llm.prompt_version}


@app.post("/reason", response_model=ReasonResponse)
async def reason(req: ReasonRequest) -> ReasonResponse:
    cfg = get_config()
    started = time.perf_counter()

    def elapsed_ms() -> int:
        return int((time.perf_counter() - started) * 1000)

    input_hash = hashlib.sha256(req.model_dump_json().encode()).hexdigest()[:12]
    try:
        system = load_system_prompt()
    except OSError as e:
        log.warning("prompt load failed: %r", e)
        resp = _fallback(f"prompt load failed: {type(e).__name__}", elapsed_ms())
        _log_call(req, resp, input_hash)
        return resp

    payload: dict[str, Any] = {
        "model": cfg.llm.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": req.model_dump_json()},
        ],
        "temperature": cfg.llm.temperature,
        "max_tokens": cfg.llm.max_tokens,
        "response_format": {"type": "json_object"},
    }
    try:
        async with httpx.AsyncClient(timeout=cfg.llm.timeout_s) as client:
            r = await client.post(f"{cfg.llm.base_url}/chat/completions", json=payload)
            if r.status_code == 400:  # e.g. LM Studio rejects response_format
                payload.pop("response_format", None)
                r = await client.post(f"{cfg.llm.base_url}/chat/completions", json=payload)
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as e:
        log.warning("llm call failed: %r", e)
        resp = _fallback(f"llm call failed: {type(e).__name__}", elapsed_ms())
        _log_call(req, resp, input_hash)
        return resp

    if not isinstance(content, str) or not content.strip():
        resp = _fallback("empty model content", elapsed_ms())
        _log_call(req, resp, input_hash)
        return resp
    try:
        decision = ReasonDecision.model_validate(json.loads(extract_json(content)))
    except (ValueError, ValidationError) as e:
        log.warning("llm output invalid: %r | raw=%.200s", e, content)
        resp = _fallback(f"invalid llm output: {type(e).__name__}", elapsed_ms())
        _log_call(req, resp, input_hash)
        return resp

    decision, clamped = clamp_decision(decision, req)
    resp = ReasonResponse(
        decision=decision,
        meta=ReasonMeta(
            model=cfg.llm.model,
            prompt_version=cfg.llm.prompt_version,
            latency_ms=elapsed_ms(),
            fallback=False,
            clamped=clamped,
        ),
    )
    _log_call(req, resp, input_hash)
    return resp
