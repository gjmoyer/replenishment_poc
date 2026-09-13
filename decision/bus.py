"""Kafka bus helpers. JSON everywhere; keys keep (store,sku) ordering."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

log = logging.getLogger("decision.bus")


def _kafka() -> Any:
    from kafka import KafkaConsumer, KafkaProducer  # kafka-python-ng

    return KafkaConsumer, KafkaProducer


def wait_broker(broker: str, timeout_s: float = 60.0) -> None:
    KafkaConsumer, _ = _kafka()
    deadline = time.time() + timeout_s
    while True:
        try:
            c = KafkaConsumer(bootstrap_servers=broker, request_timeout_ms=3000)
            c.close()
            return
        except Exception as e:
            if time.time() > deadline:
                raise TimeoutError(f"broker {broker} unreachable: {e!r}") from e
            log.info("waiting for broker %s ...", broker)
            time.sleep(2)


class Producer:
    def __init__(self, broker: str) -> None:
        _, KafkaProducer = _kafka()
        self._p = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: (k or "").encode(),
            linger_ms=20,
        )

    def send(self, topic: str, key: str, value: dict) -> None:
        self._p.send(topic, key=key, value=value)

    def flush(self, timeout: float = 5.0) -> None:
        self._p.flush(timeout)


class Consumer:
    def __init__(self, broker: str, topics: list[str], group: str) -> None:
        KafkaConsumer, _ = _kafka()
        self._c = KafkaConsumer(
            *topics,
            bootstrap_servers=broker,
            group_id=group,
            value_deserializer=lambda b: json.loads(b.decode()),
            key_deserializer=lambda b: b.decode() if b else "",
            auto_offset_reset="earliest",
            # Manual commit: offsets advance only after PG persisted, so a
            # crash replays into the processed() guard instead of losing data.
            enable_auto_commit=False,
        )

    def poll(self, timeout_ms: int = 500) -> Any:
        return self._c.poll(timeout_ms=timeout_ms)

    def commit(self) -> None:
        self._c.commit()

    def close(self) -> None:
        self._c.close()
