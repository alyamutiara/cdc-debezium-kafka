"""Transform service entrypoint.

Consumes from `cdc.source.*` topics, applies per-table transforms, handles
DDL events, and produces to `cdc.transformed.*` topics. Offset commits happen
after a successful produce, giving at-least-once delivery semantics.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import threading
from typing import Any

import uvicorn
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, Response

from .config import TransformConfig, load_config
from .ddl_handler import DdlDecision, DdlHandler
from .envelope import DataEvent, EnvelopeError, Op, parse_event
from .metrics import (
    EVENTS_FAILED,
    EVENTS_FILTERED,
    EVENTS_PROCESSED,
    Timer,
    render_latest,
)
from .producer import TransformProducer
from .transform import FilteredOutError, transform_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
log = logging.getLogger("transform.service")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "cdc-transform-service")
TRANSFORM_CONFIG_PATH = os.environ.get("TRANSFORM_CONFIG_PATH", "/config/transform.yaml")
METRICS_HOST = os.environ.get("METRICS_HOST", "0.0.0.0")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))


class TransformService:
    def __init__(self, config: TransformConfig, bootstrap_servers: str, group_id: str) -> None:
        self._config = config
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._stop_event = threading.Event()
        self._consumer: Consumer | None = None
        self._producer: TransformProducer | None = None
        self._ddl_handler = DdlHandler(on_ddl=self._on_ddl)
        self._table_specs = {table: spec for table, spec in config.transforms.items()}
        self._app = self._build_app()

    # ---------- public ----------

    def run(self) -> None:
        self._consumer = self._build_consumer()
        self._producer = TransformProducer(bootstrap_servers=self._bootstrap_servers)
        self._consumer.subscribe(self._config.source_topics)
        log.info(
            "subscribed to topics=%s group=%s",
            self._config.source_topics, self._group_id,
        )
        self._install_signal_handlers()
        self._consume_loop()

    def stop(self) -> None:
        log.info("stopping transform service")
        self._stop_event.set()

    @property
    def app(self) -> FastAPI:
        return self._app

    # ---------- internals ----------

    def _build_consumer(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": self._group_id,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        })

    def _build_app(self) -> FastAPI:
        app = FastAPI(title="cdc-transform-service", version="0.1.0")

        @app.get("/health")
        def health() -> dict[str, Any]:
            return {
                "status": "ok",
                "subscribed_topics": self._config.source_topics,
                "ddl_stats": {
                    "add_columns": self._ddl_handler.stats.add_columns,
                    "drop_columns": self._ddl_handler.stats.drop_columns,
                    "other_ddl": self._ddl_handler.stats.other_ddl,
                },
            }

        @app.get("/metrics")
        def metrics() -> Response:
            payload, content_type = render_latest()
            return Response(content=payload, media_type=content_type)

        return app

    def _install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, lambda *_: self.stop())
            except (ValueError, OSError):
                # Not the main thread (uvicorn worker) — skip.
                pass

    def _consume_loop(self) -> None:
        assert self._consumer is not None and self._producer is not None
        try:
            while not self._stop_event.is_set():
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                err = msg.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("consumer error: %s", err)
                    EVENTS_FAILED.labels(topic=msg.topic() or "unknown", reason="consumer").inc()
                    continue
                self._handle_message(msg)
                self._producer.poll(0)
        finally:
            self._shutdown()

    def _handle_message(self, msg: Any) -> None:
        assert self._producer is not None
        topic = msg.topic()
        key = msg.key()
        raw_value = msg.value()

        with Timer(topic=topic):
            try:
                parsed = parse_event(raw_value) if raw_value is not None else None
            except EnvelopeError as exc:
                log.warning("envelope parse error on %s: %s", topic, exc)
                EVENTS_FAILED.labels(topic=topic, reason="envelope").inc()
                if self._consumer is not None:
                    self._consumer.commit(message=msg, asynchronous=False)
                return

            if parsed is None:
                # Tombstone from upstream — pass through.
                self._produce_tombstone(topic, key)
                if self._consumer is not None:
                    self._consumer.commit(message=msg, asynchronous=False)
                return

            if parsed.ddl is not None:
                self._ddl_handler.handle(parsed.ddl)
                # Commit immediately so DDL ordering is preserved; downstream
                # schema evolution happens on the JDBC sink side.
                if self._consumer is not None:
                    self._consumer.commit(message=msg, asynchronous=False)
                return

            assert parsed.data is not None
            event: DataEvent = parsed.data
            try:
                self._process_data_event(event, key=key)
            except Exception:
                log.exception("unhandled error processing %s op=%s", topic, event.op)
                EVENTS_FAILED.labels(topic=topic, reason="unhandled").inc()
                # Don't commit on unhandled error — let it be retried.
                return

            if self._consumer is not None:
                self._consumer.commit(message=msg, asynchronous=False)

    def _process_data_event(self, event: DataEvent, key: bytes | None) -> None:
        assert self._producer is not None
        spec = self._table_specs.get(event.table)
        output_topic = self._config.topic_for_table(f"cdc.source.{event.schema}.{event.table}")
        if spec is None:
            # No transform configured for this table — pass through.
            self._produce_data(output_topic, event, key)
            EVENTS_PROCESSED.labels(topic=output_topic, op=event.op.value).inc()
            return

        try:
            result = transform_event(event, spec)
        except FilteredOutError as exc:
            log.info("filtered out: table=%s reason=%s", event.table, exc)
            EVENTS_FILTERED.labels(topic=f"cdc.source.{event.table}").inc()
            return

        output_key = _transform_key(key, spec.column_map)
        if result.is_tombstone or result.row is None:
            self._produce_tombstone(output_topic, output_key)
            EVENTS_PROCESSED.labels(topic=output_topic, op=event.op.value).inc()
            return

        payload = _encode_connect_record(result.row, schema_name=f"{output_topic}.Value")
        self._producer.produce(
            topic=output_topic,
            value=payload,
            key=output_key,
            headers=[("op", event.op.value.encode("utf-8"))],
        )
        EVENTS_PROCESSED.labels(topic=output_topic, op=event.op.value).inc()
        log.debug("produced: %s op=%s row=%s", output_topic, event.op.value, result.row)

    def _produce_data(self, topic: str, event: DataEvent, key: bytes | None) -> None:
        assert self._producer is not None
        if event.op == Op.DELETE:
            self._produce_tombstone(topic, key)
            return
        row = event.after or {}
        payload = _encode_connect_record(row, schema_name=f"{topic}.Value")
        self._producer.produce(
            topic=topic,
            value=payload,
            key=key,
            headers=[("op", event.op.value.encode("utf-8"))],
        )

    def _produce_tombstone(self, topic: str, key: bytes | None) -> None:
        assert self._producer is not None
        # Null value with same key = tombstone. JDBC sink uses this to issue DELETE.
        self._producer.produce(topic=topic, value=None, key=key)

    def _on_ddl(self, decision: DdlDecision) -> None:
        # Counter for Prometheus visibility.
        from .metrics import DDL_EVENTS
        DDL_EVENTS.labels(kind=decision.action, action=decision.action).inc()
        # Logged in ddl_handler.handle already.

    def _shutdown(self) -> None:
        log.info("shutting down")
        if self._producer is not None:
            self._producer.close()
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                log.exception("error closing consumer")
        log.info("shutdown complete")


def _json_default(o: Any) -> Any:
    # Handle datetime, Decimal, etc. that may appear in Debezium payloads.
    if hasattr(o, "isoformat"):
        return o.isoformat()
    if hasattr(o, "__str__"):
        return str(o)
    raise TypeError(f"not json-serializable: {type(o).__name__}")


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_json_value(v) for v in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return _json_default(value)


def _field_schema(field: str, value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        field_type = "boolean"
        optional = False
    elif isinstance(value, int):
        field_type = "int32" if -(2**31) <= value <= (2**31 - 1) else "int64"
        optional = False
    elif isinstance(value, float):
        field_type = "float64"
        optional = False
    elif value is None:
        field_type = "string"
        optional = True
    else:
        field_type = "string"
        optional = False
    return {"type": field_type, "optional": optional, "field": field}


def _encode_connect_record(payload: dict[str, Any], schema_name: str) -> bytes:
    normalized = {k: _normalize_json_value(v) for k, v in payload.items()}
    document = {
        "schema": {
            "type": "struct",
            "fields": [_field_schema(field, value) for field, value in normalized.items()],
            "optional": False,
            "name": schema_name,
        },
        "payload": normalized,
    }
    return json.dumps(document, separators=(",", ":")).encode("utf-8")


def _transform_key(key: bytes | None, mapping: dict[str, str]) -> bytes | None:
    if key is None or not mapping:
        return key
    try:
        raw = json.loads(key.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return key
    if not isinstance(raw, dict):
        return key

    payload = raw.get("payload", raw)
    if not isinstance(payload, dict):
        return key

    transformed = {
        mapping.get(str(k), str(k)): _normalize_json_value(v)
        for k, v in payload.items()
    }
    schema_name = "transformed.Key"
    schema = raw.get("schema")
    if isinstance(schema, dict):
        existing_name = schema.get("name")
        if isinstance(existing_name, str) and existing_name:
            schema_name = existing_name
    return _encode_connect_record(transformed, schema_name=schema_name)


def main() -> None:
    config = load_config(TRANSFORM_CONFIG_PATH)
    log.info("loaded config: source_topics=%s tables=%s",
             config.source_topics, list(config.transforms.keys()))
    service = TransformService(
        config=config,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
    )

    # Run the FastAPI metrics server in a background thread alongside the
    # Kafka consumer loop. This keeps the deployment simple: one process.
    def _start_api() -> None:
        uvicorn.run(
            service.app,
            host=METRICS_HOST,
            port=METRICS_PORT,
            log_level="warning",
        )

    api_thread = threading.Thread(target=_start_api, daemon=True, name="metrics-api")
    api_thread.start()
    log.info("metrics endpoint listening on %s:%d", METRICS_HOST, METRICS_PORT)

    service.run()


if __name__ == "__main__":
    main()
