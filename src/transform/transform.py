"""Transform logic: column mapping and filtering.

A transform is applied per-table. The pipeline:
1. Apply column_map (rename source columns)
2. Apply drop_columns (remove sensitive or unwanted fields)
3. Evaluate filter rules; if any filter matches, drop the event
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from .config import FilterRule, TableTransform
from .envelope import DataEvent, Op

log = logging.getLogger(__name__)


class FilteredOutError(Exception):
    """Raised when an event is dropped by a filter rule."""


@dataclass
class TransformResult:
    row: dict[str, Any] | None  # None for tombstones (delete events with no before)
    is_tombstone: bool


def apply_column_map(row: dict[str, Any], mapping: dict[str, str]) -> dict[str, Any]:
    """Rename columns in `row` per `mapping` (src -> dst)."""
    if not mapping:
        return row
    return {(mapping.get(k, k)): v for k, v in row.items()}


def apply_drop_columns(row: dict[str, Any], drop: list[str]) -> dict[str, Any]:
    if not drop:
        return row
    return {k: v for k, v in row.items() if k not in drop}


def evaluate_filter(value: Any, op: str, target: Any) -> bool:
    """Return True if (value op target) matches."""
    if op == "=":
        return bool(value == target)
    if op == "!=":
        return bool(value != target)
    if op == "<":
        return value is not None and bool(value < target)
    if op == "<=":
        return value is not None and bool(value <= target)
    if op == ">":
        return value is not None and bool(value > target)
    if op == ">=":
        return value is not None and bool(value >= target)
    if op == "in":
        return value in (target or [])
    if op == "not in":
        return value not in (target or [])
    raise ValueError(f"unsupported op: {op}")


def apply_filters(row: dict[str, Any], filters: list[FilterRule]) -> None:
    """Raise FilteredOutError if any filter rule matches the row.

    Multiple rules are OR-combined: the event is dropped on the first match.
    """
    if not filters:
        return
    for f in filters:
        value = row.get(f.field)
        if evaluate_filter(value, f.op, f.value):
            log.debug("filter matched: field=%s op=%s value=%r", f.field, f.op, value)
            raise FilteredOutError(f"filter matched: {f.field} {f.op} {f.value!r}")


def transform_event(event: DataEvent, spec: TableTransform) -> TransformResult:
    """Apply the per-table transform to a data event.

    Returns the resulting row (or None for tombstones). Raises FilteredOutError
    if the event is dropped by a filter rule.
    """
    if event.op.is_delete:
        # For deletes we emit a tombstone (no row) so JDBC sink can apply DELETE.
        # Still subject to filters — a configured filter on status='deleted' should
        # drop the delete entirely.
        if event.before is None:
            return TransformResult(row=None, is_tombstone=True)
        row = dict(event.before)
        row = apply_column_map(row, spec.column_map)
        row = apply_drop_columns(row, spec.drop_columns)
        apply_filters(row, spec.filters)
        # Tombstone: no row payload, but we still want to signal delete.
        return TransformResult(row=None, is_tombstone=True)

    if event.after is None:
        return TransformResult(row=None, is_tombstone=True)

    row = dict(event.after)
    row = apply_column_map(row, spec.column_map)
    row = apply_drop_columns(row, spec.drop_columns)
    apply_filters(row, spec.filters)
    return TransformResult(row=row, is_tombstone=False)


def is_tombstone_for_op(op: Op) -> bool:
    return op == Op.DELETE
