"""Single config source. config/poc.yaml + env overrides, validated at
startup. No module may hardcode seed/day/trucks/delays/URLs — import here.

Env overrides: LLM_BASE_URL, LLM_MODEL, LLM_TIMEOUT_S, KAFKA_BROKER,
DATABASE_URL, SIM_SEED.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml

log = logging.getLogger("core.config")

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LlmSettings:
    provider: str = "lmstudio"
    base_url: str = "http://127.0.0.1:8081/v1"
    model: str = "qwen2.5-7b-instruct-mlx"
    timeout_s: float = 30.0
    temperature: float = 0.2
    max_tokens: int = 300
    prompt_version: str = "v1"
    # #2 outcome-aware few-shots: retrieved precedent per reason call.
    # 0 disables (pure v1 behavior). Pool bounds the retrieval SELECT.
    history_cases: int = 2
    history_pool: int = 20


@dataclass(frozen=True)
class SimSettings:
    seed: int = 42
    open_min: int = 7 * 60
    close_min: int = 22 * 60
    trucks_min: tuple[int, ...] = (10 * 60 + 30, 14 * 60)
    associate_delay_min: int = 25
    open_timeout_min: int = 120
    receipt_cases: int = 4
    stores: tuple[tuple[str, str, float], ...] = (("store-001", "Downtown", 1.2),)


@dataclass(frozen=True)
class KafkaSettings:
    broker: str = "localhost:9092"
    topics: tuple[str, ...] = (
        "boh_updates",
        "truck_arrivals",
        "restock_tasks",
        "restock_confirmations",
    )


@dataclass(frozen=True)
class PgSettings:
    url: str = "postgresql://poc:poc@localhost:5432/poc"


@dataclass(frozen=True)
class AppConfig:
    llm: LlmSettings = field(default_factory=LlmSettings)
    sim: SimSettings = field(default_factory=SimSettings)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    pg: PgSettings = field(default_factory=PgSettings)


def _parse_hhmm(value: str) -> int:
    h, m = value.split(":")
    return int(h) * 60 + int(m)


def _read_yaml() -> dict:
    path = ROOT / "config" / "poc.yaml"
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        log.info("no config/poc.yaml, using builtins")
        return {}
    except (yaml.YAMLError, OSError) as e:
        log.warning("unreadable config/poc.yaml (%r), using builtins", e)
        return {}
    if not isinstance(data, dict):
        log.warning("config/poc.yaml root is not a mapping, using builtins")
        return {}
    return data


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        log.warning("bad %s=%r, using %s", name, raw, default)
        return default


def load() -> AppConfig:
    """Build validated config. Fails fast on nonsense, never at import."""
    y = _read_yaml()
    yl, ys = y.get("llm", {}), y.get("sim", {})
    day = ys.get("day") or {}
    trucks = ys.get("trucks") or ["10:30", "14:00"]

    timeout = _get_float("LLM_TIMEOUT_S", float(yl.get("timeout_s", 30)))
    if timeout <= 0:
        raise ValueError(f"llm.timeout_s must be > 0, got {timeout}")
    history_cases = int(os.getenv("LLM_HISTORY_CASES", yl.get("history_cases", 2)))
    if history_cases < 0 or history_cases > 3:
        raise ValueError(f"llm.history_cases must be in [0, 3], got {history_cases}")
    history_pool = int(yl.get("history_pool", 20))
    if history_pool <= 0:
        raise ValueError(f"llm.history_pool must be > 0, got {history_pool}")

    seed = int(os.getenv("SIM_SEED", ys.get("seed", 42)))
    stores = (
        tuple(
            (s["store_id"], s.get("name", s["store_id"]), float(s.get("rate_mult", 1.0)))
            for s in (ys.get("stores") or [])
        )
        or SimSettings.stores
    )
    return AppConfig(
        llm=LlmSettings(
            provider=str(yl.get("provider", "lmstudio")),
            base_url=os.getenv("LLM_BASE_URL", yl.get("base_url", LlmSettings.base_url)).rstrip(
                "/"
            ),
            model=os.getenv("LLM_MODEL", yl.get("model", LlmSettings.model)),
            timeout_s=timeout,
            temperature=float(yl.get("temperature", 0.2)),
            max_tokens=int(yl.get("max_tokens", 300)),
            prompt_version=str(yl.get("prompt_version", "v1")),
            history_cases=history_cases,
            history_pool=history_pool,
        ),
        sim=SimSettings(
            seed=seed,
            open_min=_parse_hhmm(day.get("open", "07:00")),
            close_min=_parse_hhmm(day.get("close", "22:00")),
            trucks_min=tuple(_parse_hhmm(t) for t in trucks),
            associate_delay_min=int(ys.get("associate_delay_min", 25)),
            open_timeout_min=120,
            receipt_cases=4,
            stores=stores,
        ),
        kafka=KafkaSettings(
            broker=os.getenv("KAFKA_BROKER", "localhost:9092"),
        ),
        pg=PgSettings(
            url=os.getenv("DATABASE_URL", "postgresql://poc:poc@localhost:5432/poc"),
        ),
    )


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    return load()
