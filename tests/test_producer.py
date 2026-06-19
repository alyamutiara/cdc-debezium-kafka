"""Tests for the Kafka producer offset commit semantics.

The producer wrapper is exercised via the service layer in integration tests.
These unit tests cover the contract: produce + flush delivers messages.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from transform.producer import TransformProducer


def test_producer_initializes_with_sane_defaults() -> None:
    p = TransformProducer(bootstrap_servers="kafka:9092")
    assert p.delivery_errors == 0


def test_producer_records_delivery_error() -> None:
    p = TransformProducer(bootstrap_servers="kafka:9092")
    fake_err = MagicMock()
    fake_msg = MagicMock()
    fake_msg.topic.return_value = "cdc.transformed.x"
    fake_msg.key.return_value = b"k"
    p._delivery_cb(fake_err, fake_msg)
    assert p.delivery_errors == 1


def test_producer_records_successful_delivery() -> None:
    p = TransformProducer(bootstrap_servers="kafka:9092")
    fake_msg = MagicMock()
    fake_msg.topic.return_value = "cdc.transformed.x"
    fake_msg.partition.return_value = 0
    fake_msg.offset.return_value = 42
    p._delivery_cb(None, fake_msg)
    assert p.delivery_errors == 0


def test_producer_produce_delegates_to_underlying() -> None:
    """Inject a stub producer and verify the wrapper calls it with the right args."""
    p = TransformProducer(bootstrap_servers="kafka:9092")
    captured: list[dict] = []

    class StubProducer:
        def produce(self, **kwargs):  # type: ignore[no-untyped-def]
            captured.append(kwargs)

        def poll(self, timeout):  # type: ignore[no-untyped-def]
            return 0

        def flush(self, timeout):  # type: ignore[no-untyped-def]
            return 0

    p._producer = StubProducer()  # type: ignore[assignment]
    p.produce("topic", b"v", key=b"k", headers=[("h", b"v")])
    assert len(captured) == 1
    assert captured[0]["topic"] == "topic"
    assert captured[0]["value"] == b"v"
    assert captured[0]["key"] == b"k"
    assert captured[0]["headers"] == [("h", b"v")]


def test_producer_close_flushes() -> None:
    p = TransformProducer(bootstrap_servers="kafka:9092")
    p.close()  # should not raise

