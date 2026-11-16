from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from transform.config import FilterRule, TableTransform, TransformConfig
from transform.metrics import render_latest
from transform.service import (
    TransformService,
    _encode_connect_record,
    _transform_key,
)


class _StubConsumer:
    def __init__(self) -> None:
        self.committed: list[Any] = []

    def commit(self, message: Any = None, asynchronous: bool = False) -> None:
        self.committed.append((message, asynchronous))


class _StubProducer:
    def __init__(self, *, fail_delivery: bool = False, flush_remaining: int = 0) -> None:
        self.produced: list[dict[str, Any]] = []
        self.delivery_errors = 0
        self.flush_calls = 0
        self._fail_delivery = fail_delivery
        self._flush_remaining = flush_remaining

    def produce(
        self,
        topic: str,
        value: bytes | None,
        key: bytes | None = None,
        headers: list[tuple[str, str | bytes | None]] | None = None,
    ) -> None:
        self.produced.append({
            "topic": topic,
            "value": value,
            "key": key,
            "headers": headers,
        })

    def flush(self, timeout: float = 10.0) -> int:
        self.flush_calls += 1
        if self._fail_delivery:
            self.delivery_errors += 1
        return self._flush_remaining


def _make_msg(topic: str, value: bytes | None, key: bytes | None = None) -> MagicMock:
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.value.return_value = value
    msg.key.return_value = key
    return msg


def _make_service(
    producer: _StubProducer | None = None,
    consumer: _StubConsumer | None = None,
) -> TransformService:
    cfg = TransformConfig(
        source_topics=["cdc.source.orders"],
        output_topic_prefix="cdc.transformed",
        transforms={
            "orders": TableTransform(
                column_map={"id": "order_id", "customer_id": "cust_id"},
                drop_columns=["internal_notes"],
                filters=[FilterRule(field="status", op="=", value="deleted")],
            )
        },
    )
    svc = TransformService(cfg, bootstrap_servers="kafka:9092", group_id="test-group")
    svc._producer = producer or _StubProducer()  # type: ignore[assignment]
    svc._consumer = consumer or _StubConsumer()  # type: ignore[assignment]
    return svc


def test_encode_connect_record_wraps_payload_with_schema() -> None:
    record = json.loads(_encode_connect_record(
        {"order_id": 1, "status": "pending", "active": True},
        schema_name="cdc.transformed.orders.Value",
    ).decode("utf-8"))

    assert record["payload"] == {"order_id": 1, "status": "pending", "active": True}
    assert record["schema"]["name"] == "cdc.transformed.orders.Value"
    assert record["schema"]["fields"] == [
        {"type": "int32", "optional": False, "field": "order_id"},
        {"type": "string", "optional": False, "field": "status"},
        {"type": "boolean", "optional": False, "field": "active"},
    ]


def test_transform_key_applies_column_mapping_to_schemaful_key() -> None:
    key = json.dumps({
        "schema": {
            "type": "struct",
            "fields": [{"type": "int64", "optional": False, "field": "id"}],
            "optional": False,
            "name": "cdc.source.public.orders.Key",
        },
        "payload": {"id": 7},
    }).encode("utf-8")

    transformed = json.loads(_transform_key(key, {"id": "order_id"}).decode("utf-8"))

    assert transformed["payload"] == {"order_id": 7}
    assert transformed["schema"]["name"] == "cdc.source.public.orders.Key"
    assert transformed["schema"]["fields"] == [
        {"type": "int64", "optional": False, "field": "order_id"},
    ]


def test_consumer_disables_auto_commit(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    class _FakeConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            captured.update(conf)

    monkeypatch.setattr("transform.service.Consumer", _FakeConsumer)
    svc = _make_service()
    svc._build_consumer()
    assert captured["enable.auto.commit"] is False


def test_handle_message_skips_upstream_tombstone_without_looping() -> None:
    producer = _StubProducer()
    consumer = _StubConsumer()
    svc = _make_service(producer=producer, consumer=consumer)
    msg = _make_msg("cdc.source.orders", value=None, key=b'{"payload":{"id":1}}')

    svc._handle_message(msg)

    assert producer.produced == []
    assert len(consumer.committed) == 1


def test_handle_message_preserves_schema_and_flushes_before_commit() -> None:
    producer = _StubProducer()
    consumer = _StubConsumer()
    svc = _make_service(producer=producer, consumer=consumer)

    envelope = json.dumps({
        "schema": {
            "type": "struct",
            "fields": [
                {
                    "type": "struct",
                    "optional": True,
                    "field": "after",
                    "fields": [
                        {"type": "int32", "optional": False, "field": "id"},
                        {"type": "int32", "optional": False, "field": "customer_id"},
                        {"type": "int32", "optional": True, "field": "discount_cents"},
                        {
                            "type": "int64",
                            "optional": False,
                            "name": "org.apache.kafka.connect.data.Timestamp",
                            "version": 1,
                            "field": "created_at",
                        },
                        {"type": "string", "optional": True, "field": "internal_notes"},
                        {"type": "string", "optional": False, "field": "status"},
                    ],
                }
            ],
        },
        "payload": {
            "op": "c",
            "before": None,
            "after": {
                "id": 42,
                "customer_id": 7,
                "discount_cents": None,
                "created_at": 1700000000000,
                "internal_notes": "secret",
                "status": "pending",
            },
            "source": {"db": "ecommerce", "schema": "public", "table": "orders"},
        },
    }).encode("utf-8")
    key = json.dumps({
        "schema": {"type": "struct", "fields": [{"type": "int32", "optional": False, "field": "id"}]},
        "payload": {"id": 42},
    }).encode("utf-8")

    svc._handle_message(_make_msg("cdc.source.orders", envelope, key=key))

    assert len(producer.produced) == 1
    assert producer.flush_calls == 1
    assert len(consumer.committed) == 1
    out = producer.produced[0]
    assert out["topic"] == "cdc.transformed.orders"
    record = json.loads(out["value"].decode("utf-8"))
    assert record["payload"] == {
        "order_id": 42,
        "cust_id": 7,
        "discount_cents": None,
        "created_at": 1700000000000,
        "status": "pending",
    }
    fields_by_name = {f["field"]: f for f in record["schema"]["fields"]}
    assert fields_by_name["discount_cents"] == {"type": "int32", "optional": True, "field": "discount_cents"}
    assert fields_by_name["created_at"]["name"] == "org.apache.kafka.connect.data.Timestamp"
    assert "internal_notes" not in fields_by_name


def test_handle_message_does_not_commit_on_delivery_failure() -> None:
    producer = _StubProducer(fail_delivery=True)
    consumer = _StubConsumer()
    svc = _make_service(producer=producer, consumer=consumer)

    envelope = json.dumps({
        "payload": {
            "op": "c",
            "after": {"id": 1, "customer_id": 2, "status": "pending"},
            "source": {"schema": "public", "table": "orders"},
        }
    }).encode("utf-8")

    svc._handle_message(_make_msg("cdc.source.orders", envelope))

    assert len(producer.produced) == 1
    assert producer.flush_calls == 1
    assert consumer.committed == []


def test_handle_message_does_not_commit_on_processing_exception(monkeypatch: Any) -> None:
    producer = _StubProducer()
    consumer = _StubConsumer()
    svc = _make_service(producer=producer, consumer=consumer)

    def _boom(*_args: Any, **_kwargs: Any) -> str | None:
        raise RuntimeError("boom")

    monkeypatch.setattr(svc, "_process_data_event", _boom)
    envelope = json.dumps({
        "payload": {
            "op": "c",
            "after": {"id": 1, "status": "pending"},
            "source": {"schema": "public", "table": "orders"},
        }
    }).encode("utf-8")

    svc._handle_message(_make_msg("cdc.source.orders", envelope))

    assert consumer.committed == []


def test_handle_message_delete_produces_mapped_tombstone_and_filtered_skips() -> None:
    producer = _StubProducer()
    consumer = _StubConsumer()
    svc = _make_service(producer=producer, consumer=consumer)

    key = json.dumps({
        "schema": {"type": "struct", "fields": [{"type": "int32", "optional": False, "field": "id"}]},
        "payload": {"id": 9},
    }).encode("utf-8")
    delete_env = json.dumps({
        "payload": {
            "op": "d",
            "before": {"id": 9, "customer_id": 1, "status": "cancelled"},
            "after": None,
            "source": {"schema": "public", "table": "orders"},
        }
    }).encode("utf-8")
    filtered_delete_env = json.dumps({
        "payload": {
            "op": "d",
            "before": {"id": 10, "customer_id": 1, "status": "deleted"},
            "after": None,
            "source": {"schema": "public", "table": "orders"},
        }
    }).encode("utf-8")

    svc._handle_message(_make_msg("cdc.source.orders", delete_env, key=key))
    svc._handle_message(_make_msg("cdc.source.orders", filtered_delete_env, key=key))

    assert len(producer.produced) == 1
    assert producer.produced[0]["topic"] == "cdc.transformed.orders"
    assert producer.produced[0]["value"] is None
    out_key = json.loads(producer.produced[0]["key"].decode("utf-8"))
    assert out_key["payload"] == {"order_id": 9}
    assert len(consumer.committed) == 2


def test_handle_message_ddl_records_kind_and_action_labels() -> None:
    producer = _StubProducer()
    consumer = _StubConsumer()
    svc = _make_service(producer=producer, consumer=consumer)

    ddl_env = json.dumps({
        "payload": {
            "ddl": "ALTER TABLE public.orders ADD COLUMN tag TEXT",
            "source": {"db": "ecommerce", "schema": "public", "table": "orders"},
        }
    }).encode("utf-8")

    svc._handle_message(_make_msg("cdc.source.orders", ddl_env))

    assert len(consumer.committed) == 1
    payload, _ = render_latest()
    metrics_text = payload.decode("utf-8")
    assert 'ddl_events_total{action="evolve",kind="ALTER"}' in metrics_text


def test_health_endpoint_returns_503_when_unhealthy() -> None:
    svc = _make_service()
    svc._healthy = False
    svc._last_error = "consumer crashed"
    client = TestClient(svc.app)
    resp = client.get("/health")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unhealthy"
    assert body["error"] == "consumer crashed"
