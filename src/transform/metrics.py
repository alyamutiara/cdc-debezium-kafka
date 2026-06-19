"""Prometheus metrics for the transform service."""
from __future__ import annotations

import time
from collections.abc import Iterator

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

REGISTRY = CollectorRegistry()

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Total CDC events processed successfully",
    labelnames=("topic", "op"),
    registry=REGISTRY,
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Total CDC events that failed processing",
    labelnames=("topic", "reason"),
    registry=REGISTRY,
)

EVENTS_FILTERED = Counter(
    "events_filtered_total",
    "Total CDC events dropped by filter rules",
    labelnames=("topic",),
    registry=REGISTRY,
)

DDL_EVENTS = Counter(
    "ddl_events_total",
    "Total DDL events observed",
    labelnames=("kind", "action"),
    registry=REGISTRY,
)

TRANSFORM_DURATION = Histogram(
    "transform_duration_seconds",
    "Time spent applying transforms to a single event",
    labelnames=("topic",),
    registry=REGISTRY,
)

SERVICE_UPTIME = Gauge(
    "service_uptime_seconds",
    "Service uptime in seconds",
    registry=REGISTRY,
)

START_TIME = time.monotonic()


def record_uptime() -> None:
    SERVICE_UPTIME.set(time.monotonic() - START_TIME)


def render_latest() -> tuple[bytes, str]:
    record_uptime()
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


class Timer:
    """Context manager for timing transform work."""

    def __init__(self, topic: str) -> None:
        self._topic = topic
        self._start: float = 0.0
        self.duration: float = 0.0

    def __enter__(self) -> Timer:
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.duration = time.perf_counter() - self._start
        TRANSFORM_DURATION.labels(topic=self._topic).observe(self.duration)


def iter_metrics() -> Iterator[bytes]:
    record_uptime()
    yield from generate_latest(REGISTRY).splitlines(keepends=True)
