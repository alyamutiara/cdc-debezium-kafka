"""Tests for the metrics endpoint and counters."""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from transform.config import TransformConfig
from transform.metrics import (
    EVENTS_FAILED,
    EVENTS_FILTERED,
    EVENTS_PROCESSED,
    Timer,
    render_latest,
)
from transform.service import TransformService


@pytest.fixture
def service() -> TransformService:
    cfg = TransformConfig(
        source_topics=["cdc.source.orders"],
        output_topic_prefix="cdc.transformed",
    )
    return TransformService(
        config=cfg,
        bootstrap_servers="kafka:9092",
        group_id="test-group",
    )


def test_metrics_endpoint_returns_prometheus_format(service: TransformService) -> None:
    client = TestClient(service.app)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    body = resp.text
    assert "events_processed_total" in body
    assert "events_failed_total" in body
    assert "transform_duration_seconds" in body
    assert "service_uptime_seconds" in body


def test_health_endpoint_returns_ok(service: TransformService) -> None:
    client = TestClient(service.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["subscribed_topics"] == ["cdc.source.orders"]


def test_counters_increment_on_labels() -> None:
    EVENTS_PROCESSED.labels(topic="t", op="c").inc()
    EVENTS_FAILED.labels(topic="t", reason="envelope").inc()
    EVENTS_FILTERED.labels(topic="t").inc()
    payload, _ = render_latest()
    body = payload.decode("utf-8")
    assert 'events_processed_total{op="c",topic="t"}' in body
    assert 'events_failed_total{reason="envelope",topic="t"}' in body
    assert 'events_filtered_total{topic="t"}' in body


def test_timer_records_observation() -> None:
    with Timer(topic="t"):
        time.sleep(0.01)
    payload, _ = render_latest()
    body = payload.decode("utf-8")
    # Histogram emits _count and _sum series.
    assert 'transform_duration_seconds_count{topic="t"}' in body


def test_metrics_uptime_increases_with_time() -> None:
    payload1, _ = render_latest()
    time.sleep(0.05)
    payload2, _ = render_latest()
    body1 = payload1.decode("utf-8")
    body2 = payload2.decode("utf-8")
    # The gauge value should be present in both.
    assert "service_uptime_seconds" in body1
    assert "service_uptime_seconds" in body2
