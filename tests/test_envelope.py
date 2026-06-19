"""Tests for the Debezium envelope parser."""
from __future__ import annotations

import json

import pytest

from transform.envelope import EnvelopeError, Op, parse_event


def _wrap(op: str, before: dict | None = None, after: dict | None = None) -> bytes:
    return json.dumps({
        "payload": {
            "op": op,
            "before": before,
            "after": after,
            "source": {"db": "ecommerce", "schema": "public", "table": "orders"},
            "ts_ms": 1700000000000,
        }
    }).encode("utf-8")


def test_parse_insert_event() -> None:
    parsed = parse_event(_wrap("c", after={"id": 1, "status": "pending"}))
    assert parsed.ddl is None
    assert parsed.data is not None
    assert parsed.data.op == Op.CREATE
    assert parsed.data.after == {"id": 1, "status": "pending"}
    assert parsed.data.before is None


def test_parse_update_event() -> None:
    parsed = parse_event(_wrap("u", before={"id": 1}, after={"id": 1, "status": "shipped"}))
    assert parsed.data is not None
    assert parsed.data.op == Op.UPDATE
    assert parsed.data.before == {"id": 1}
    assert parsed.data.after == {"id": 1, "status": "shipped"}


def test_parse_delete_event() -> None:
    parsed = parse_event(_wrap("d", before={"id": 1, "status": "deleted"}))
    assert parsed.data is not None
    assert parsed.data.op == Op.DELETE
    assert parsed.data.row == {"id": 1, "status": "deleted"}


def test_parse_read_snapshot_event() -> None:
    parsed = parse_event(_wrap("r", after={"id": 1}))
    assert parsed.data is not None
    assert parsed.data.op == Op.READ


def test_parse_unknown_op_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(_wrap("z"))


def test_parse_delete_without_before_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(_wrap("d", before=None))


def test_parse_insert_without_after_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(_wrap("c", after=None))


def test_parse_malformed_json_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(b"not json{")


def test_parse_non_object_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(b'"just a string"')


def test_parse_missing_payload_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(json.dumps({"no_payload": True}).encode())


def test_parse_missing_source_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(json.dumps({"payload": {"op": "c", "after": {}}}).encode())


def test_parse_ddl_event() -> None:
    raw = {
        "payload": {
            "op": "r",
            "source": {
                "db": "ecommerce",
                "schema": "public",
                "table": "orders",
                "ddl": "ALTER TABLE public.orders ADD COLUMN discount_cents INTEGER",
            },
        }
    }
    parsed = parse_event(json.dumps(raw).encode())
    assert parsed.data is None
    assert parsed.ddl is not None
    assert parsed.ddl.kind == "ALTER"
    assert "ADD COLUMN" in parsed.ddl.ddl


def test_parse_ddl_create_table() -> None:
    raw = {
        "payload": {
            "op": "r",
            "source": {
                "db": "ecommerce",
                "schema": "public",
                "table": "new_table",
                "ddl": "CREATE TABLE new_table (id INTEGER)",
            },
        }
    }
    parsed = parse_event(json.dumps(raw).encode())
    assert parsed.ddl is not None
    assert parsed.ddl.kind == "CREATE"


def test_parse_ddl_drop_table() -> None:
    raw = {
        "payload": {
            "op": "r",
            "source": {
                "db": "ecommerce",
                "schema": "public",
                "table": "old_table",
                "ddl": "DROP TABLE old_table",
            },
        }
    }
    parsed = parse_event(json.dumps(raw).encode())
    assert parsed.ddl is not None
    assert parsed.ddl.kind == "DROP"


def test_parse_non_utf8_raises() -> None:
    with pytest.raises(EnvelopeError):
        parse_event(b"\xff\xfe\x00bad")


def test_parse_string_input() -> None:
    parsed = parse_event('{"payload":{"op":"c","after":{},"source":{"table":"x"}}}')
    assert parsed.data is not None
    assert parsed.data.op == Op.CREATE


def test_source_table_and_schema() -> None:
    parsed = parse_event(_wrap("c", after={"id": 1}))
    assert parsed.data is not None
    assert parsed.data.table == "orders"
    assert parsed.data.schema == "public"
