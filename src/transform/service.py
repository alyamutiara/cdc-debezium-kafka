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
    DDL_EVENTS,
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
        self._healthy = True
        self._last_error: str | None = None
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
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        })

    def _build_app(self) -> FastAPI:
        app = FastAPI(title="cdc-transform-service", version="0.1.0")

        @app.get("/health")
        def health(response: Response) -> dict[str, Any]:
            if not self._healthy:
                response.status_code = 503
            body: dict[str, Any] = {
                "status": "ok" if self._healthy else "unhealthy",
                "subscribed_topics": self._config.source_topics,
                "ddl_stats": {
                    "add_columns": self._ddl_handler.stats.add_columns,
                    "drop_columns": self._ddl_handler.stats.drop_columns,
                    "other_ddl": self._ddl_handler.stats.other_ddl,
                },
            }
            if self._last_error is not None:
                body["error"] = self._last_error
            return body

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
        except Exception as exc:
            self._healthy = False
            self._last_error = str(exc)
            log.exception("consume loop failed")
            raise
        finally:
            if not self._stop_event.is_set():
                self._healthy = False
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
                # Upstream Debezium tombstones follow op="d" events, which already
                # emit a mapped tombstone to the output topic. Skip without looping.
                log.debug("skipping upstream tombstone on %s", topic)
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
            errors_before = self._producer.delivery_errors
            try:
                output_topic = self._process_data_event(event, key=key)
                if output_topic is not None:
                    remaining = self._producer.flush(timeout=10.0)
                    if remaining > 0 or self._producer.delivery_errors > errors_before:
                        raise RuntimeError(
                            f"kafka delivery failed: remaining={remaining} "
                            f"errors={self._producer.delivery_errors - errors_before}"
                        )
                    EVENTS_PROCESSED.labels(topic=output_topic, op=event.op.value).inc()
            except Exception:
                log.exception("unhandled error processing %s op=%s", topic, event.op)
                EVENTS_FAILED.labels(topic=topic, reason="unhandled").inc()
                # Don't commit on unhandled error — let it be retried.
                return

            if self._consumer is not None:
                self._consumer.commit(message=msg, asynchronous=False)

    def _process_data_event(self, event: DataEvent, key: bytes | None) -> str | None:
        assert self._producer is not None
        spec = self._table_specs.get(event.table)
        output_topic = self._config.topic_for_table(f"cdc.source.{event.schema}.{event.table}")
        if spec is None:
            # No transform configured for this table — pass through.
            self._produce_data(output_topic, event, key)
            return output_topic

        try:
            result = transform_event(event, spec)
        except FilteredOutError as exc:
            log.info("filtered out: table=%s reason=%s", event.table, exc)
            EVENTS_FILTERED.labels(topic=f"cdc.source.{event.table}").inc()
            return None

        output_key = _transform_key(key, spec.column_map)
        if result.is_tombstone or result.row is None:
            self._produce_tombstone(output_topic, output_key)
            return output_topic

        field_schemas = _map_field_schemas(event.row_schema, spec.column_map)
        payload = _encode_connect_record(
            result.row,
            schema_name=f"{output_topic}.Value",
            field_schemas=field_schemas,
        )
        self._producer.produce(
            topic=output_topic,
            value=payload,
            key=output_key,
            headers=[("op", event.op.value.encode("utf-8"))],
        )
        log.debug("produced: %s op=%s row=%s", output_topic, event.op.value, result.row)
        return output_topic

    def _produce_data(self, topic: str, event: DataEvent, key: bytes | None) -> None:
        assert self._producer is not None
        if event.op == Op.DELETE:
            self._produce_tombstone(topic, key)
            return
        row = event.after or {}
        payload = _encode_connect_record(
            row,
            schema_name=f"{topic}.Value",
            field_schemas=event.row_schema,
        )
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
        DDL_EVENTS.labels(kind=decision.kind, action=decision.action).inc()
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


def _map_field_schemas(
    row_schema: dict[str, dict[str, Any]] | None,
    mapping: dict[str, str],
) -> dict[str, dict[str, Any]] | None:
    if not row_schema:
        return None
    mapped: dict[str, dict[str, Any]] = {}
    for src_field, field_schema in row_schema.items():
        dst_field = mapping.get(src_field, src_field)
        copied = dict(field_schema)
        copied["field"] = dst_field
        mapped[dst_field] = copied
    return mapped


def _encode_connect_record(
    payload: dict[str, Any],
    schema_name: str,
    field_schemas: dict[str, dict[str, Any]] | None = None,
) -> bytes:
    normalized = {k: _normalize_json_value(v) for k, v in payload.items()}
    fields = []
    for field, value in normalized.items():
        if field_schemas is not None and field in field_schemas:
            schema_entry = dict(field_schemas[field])
            schema_entry["field"] = field
            fields.append(schema_entry)
        else:
            fields.append(_field_schema(field, value))
    document = {
        "schema": {
            "type": "struct",
            "fields": fields,
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
    field_schemas: dict[str, dict[str, Any]] | None = None
    schema = raw.get("schema")
    if isinstance(schema, dict):
        existing_name = schema.get("name")
        if isinstance(existing_name, str) and existing_name:
            schema_name = existing_name
        raw_fields = schema.get("fields")
        if isinstance(raw_fields, list):
            raw_key_schemas = {
                entry["field"]: dict(entry)
                for entry in raw_fields
                if isinstance(entry, dict) and isinstance(entry.get("field"), str)
            }
            field_schemas = _map_field_schemas(raw_key_schemas, mapping)
    return _encode_connect_record(
        transformed,
        schema_name=schema_name,
        field_schemas=field_schemas,
    )


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
