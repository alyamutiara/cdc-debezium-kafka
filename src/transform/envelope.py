"""Debezium JSON envelope parsing.

The transform service consumes the standard Debezium JSON format:
- `payload.op` is one of "c" (create/insert), "u" (update), "d" (delete), "r" (read/snapshot)
- `payload.before` is the row state before the change (null for inserts)
- `payload.after` is the row state after the change (null for deletes)
- Schema-change events carry a `ddl` field on `payload` (or `payload.source`
  in legacy/custom envelopes).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

log = logging.getLogger(__name__)


class Op(StrEnum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"

    @property
    def is_data(self) -> bool:
        return self in (Op.CREATE, Op.UPDATE, Op.DELETE, Op.READ)

    @property
    def is_delete(self) -> bool:
        return self is Op.DELETE


class EnvelopeError(ValueError):
    """Raised when an event cannot be parsed as a Debezium envelope."""


@dataclass(frozen=True)
class DataEvent:
    op: Op
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: dict[str, Any]
    ts_ms: int | None = None
    row_schema: dict[str, dict[str, Any]] | None = None

    @property
    def row(self) -> dict[str, Any] | None:
        """The row payload to transform. `after` for c/u/r, `before` for d."""
        return self.after if not self.op.is_delete else self.before

    @property
    def table(self) -> str:
        return str(self.source.get("table", ""))

    @property
    def schema(self) -> str:
        return str(self.source.get("schema", ""))


@dataclass(frozen=True)
class DdlEvent:
    ddl: str
    database: str
    schema: str
    table: str
    kind: str  # "CREATE", "ALTER", "DROP", "OTHER"
    ts_ms: int | None = None


@dataclass(frozen=True)
class ParsedEvent:
    """Discriminated union of data and DDL events."""

    data: DataEvent | None
    ddl: DdlEvent | None
    raw: dict[str, Any]


def parse_event(raw_value: bytes | str) -> ParsedEvent:
    """Parse a Kafka message value as a Debezium envelope.

    Raises EnvelopeError on malformed input.
    """
    if isinstance(raw_value, bytes):
        try:
            raw = json.loads(raw_value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EnvelopeError(f"value is not valid utf-8/json: {exc}") from exc
    else:
        try:
            raw = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise EnvelopeError(f"value is not valid json: {exc}") from exc

    if not isinstance(raw, dict):
        raise EnvelopeError(f"expected json object, got {type(raw).__name__}")

    payload = raw.get("payload")
    if not isinstance(payload, dict):
        raise EnvelopeError("missing or non-object 'payload' field")

    source = payload.get("source")
    if not isinstance(source, dict):
        raise EnvelopeError("missing or non-object 'payload.source' field")

    # Schema-change events: Debezium places `ddl` at payload.ddl (or legacy payload.source.ddl).
    ddl_sql = payload.get("ddl") if isinstance(payload.get("ddl"), str) else source.get("ddl")
    if isinstance(ddl_sql, str):
        kind = _classify_ddl(ddl_sql)
        return ParsedEvent(
            data=None,
            ddl=DdlEvent(
                ddl=ddl_sql,
                database=source.get("db", ""),
                schema=source.get("schema", ""),
                table=source.get("table", ""),
                kind=kind,
                ts_ms=payload.get("ts_ms"),
            ),
            raw=raw,
        )

    op_raw = payload.get("op")
    if not isinstance(op_raw, str):
        raise EnvelopeError(f"op field is not a string: {op_raw!r}")
    try:
        op = Op(op_raw)
    except ValueError as exc:
        raise EnvelopeError(f"unknown op: {op_raw!r}") from exc

    before = payload.get("before")
    after = payload.get("after")
    if op.is_data and op.is_delete and before is None:
        raise EnvelopeError("delete event missing 'before'")
    if op.is_data and not op.is_delete and after is None:
        raise EnvelopeError(f"op={op.value} event missing 'after'")

    return ParsedEvent(
        data=DataEvent(
            op=op,
            before=before if isinstance(before, dict) else None,
            after=after if isinstance(after, dict) else None,
            source=source,
            ts_ms=payload.get("ts_ms"),
            row_schema=_extract_row_schema(
                raw.get("schema"),
                prefer="before" if op.is_delete else "after",
            ),
        ),
        ddl=None,
        raw=raw,
    )


def _extract_row_schema(
    envelope_schema: Any,
    prefer: str = "after",
) -> dict[str, dict[str, Any]] | None:
    """Extract per-column Connect schemas from Debezium's envelope schema."""
    if not isinstance(envelope_schema, dict):
        return None
    fields = envelope_schema.get("fields")
    if not isinstance(fields, list):
        return None

    fallback = "after" if prefer == "before" else "before"
    for target in (prefer, fallback):
        for entry in fields:
            if not isinstance(entry, dict) or entry.get("field") != target:
                continue
            sub_fields = entry.get("fields")
            if not isinstance(sub_fields, list):
                continue
            result: dict[str, dict[str, Any]] = {}
            for col in sub_fields:
                if isinstance(col, dict) and isinstance(col.get("field"), str):
                    result[col["field"]] = dict(col)
            if result:
                return result
    return None


def _classify_ddl(ddl: str) -> str:
    """Coarse classification of a DDL statement."""
    upper = ddl.strip().upper()
    if upper.startswith(("CREATE TABLE", "CREATE INDEX", "CREATE VIEW", "CREATE SCHEMA")):
        return "CREATE"
    if upper.startswith(("DROP TABLE", "DROP INDEX", "DROP VIEW", "DROP SCHEMA")):
        return "DROP"
    if upper.startswith("ALTER"):
        return "ALTER"
    return "OTHER"
