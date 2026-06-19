"""Kafka producer wrapper for the transform service.

Wraps confluent-kafka Producer with delivery confirmation so we can
guarantee that the offset commit only happens after a successful produce.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

log = logging.getLogger(__name__)


class TransformProducer:
    def __init__(self, bootstrap_servers: str, **extra: Any) -> None:
        config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "lz4",
        }
        config.update(extra)
        self._producer = Producer(config)
        self._delivery_errors = 0

    @property
    def delivery_errors(self) -> int:
        return self._delivery_errors

    def _delivery_cb(
        self,
        err: KafkaError | None,
        msg: Any,
    ) -> None:
        if err is not None:
            self._delivery_errors += 1
            log.error(
                "delivery failed: topic=%s key=%s err=%s",
                msg.topic() if msg else "?",
                msg.key() if msg else "?",
                err,
            )
        else:
            log.debug("delivered: topic=%s partition=%s offset=%s",
                      msg.topic(), msg.partition(), msg.offset())

    def produce(
        self,
        topic: str,
        value: bytes | None,
        key: bytes | None = None,
        headers: list[tuple[str, str | bytes | None]] | None = None,
    ) -> None:
        self._producer.produce(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            on_delivery=self._delivery_cb,
        )

    def poll(self, timeout: float = 0.0) -> int:
        return self._producer.poll(timeout)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        remaining = self.flush(timeout=10.0)
        if remaining > 0:
            log.warning("producer closed with %d un-flushed messages", remaining)


def build_producer(bootstrap_servers: str) -> TransformProducer:
    return TransformProducer(bootstrap_servers=bootstrap_servers)


DeliveryCallback = Callable[[KafkaError | None, Any], None]


def raise_on_error(err: KafkaError | None, _msg: Any) -> None:
    if err is not None:
        raise KafkaException(err)
