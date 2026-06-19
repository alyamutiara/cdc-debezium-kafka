"""Tests for the column mapping and filtering transform."""
from __future__ import annotations

import pytest

from transform.config import FilterRule, TableTransform
from transform.envelope import DataEvent, Op
from transform.transform import (
    FilteredOutError,
    apply_column_map,
    apply_drop_columns,
    evaluate_filter,
    transform_event,
)


def _data_event(after: dict, before: dict | None = None, op: Op = Op.CREATE) -> DataEvent:
    return DataEvent(
        op=op,
        before=before,
        after=after,
        source={"db": "x", "schema": "public", "table": "orders"},
    )


def test_column_map_renames() -> None:
    out = apply_column_map({"id": 1, "customer_id": 2}, {"id": "order_id"})
    assert out == {"order_id": 1, "customer_id": 2}


def test_column_map_empty_passthrough() -> None:
    row = {"a": 1, "b": 2}
    assert apply_column_map(row, {}) == row


def test_drop_columns_removes() -> None:
    assert apply_drop_columns({"a": 1, "b": 2, "c": 3}, ["b"]) == {"a": 1, "c": 3}


def test_drop_columns_empty_passthrough() -> None:
    row = {"a": 1}
    assert apply_drop_columns(row, []) == row


def test_evaluate_eq() -> None:
    assert evaluate_filter("a", "=", "a") is True
    assert evaluate_filter("a", "=", "b") is False


def test_evaluate_neq() -> None:
    assert evaluate_filter("a", "!=", "b") is True
    assert evaluate_filter("a", "!=", "a") is False


def test_evaluate_lt() -> None:
    assert evaluate_filter(1, "<", 2) is True
    assert evaluate_filter(2, "<", 1) is False
    assert evaluate_filter(None, "<", 1) is False


def test_evaluate_in() -> None:
    assert evaluate_filter("a", "in", ["a", "b"]) is True
    assert evaluate_filter("c", "in", ["a", "b"]) is False


def test_evaluate_not_in() -> None:
    assert evaluate_filter("c", "not in", ["a", "b"]) is True


def test_transform_renames_and_drops() -> None:
    spec = TableTransform(
        column_map={"id": "order_id"},
        drop_columns=["internal_notes"],
    )
    event = _data_event({"id": 1, "internal_notes": "secret", "status": "pending"})
    result = transform_event(event, spec)
    assert result.row == {"order_id": 1, "status": "pending"}
    assert result.is_tombstone is False


def test_transform_passthrough_with_no_spec() -> None:
    spec = TableTransform()
    event = _data_event({"id": 1, "status": "pending"})
    result = transform_event(event, spec)
    assert result.row == {"id": 1, "status": "pending"}


def test_transform_filter_drops_event() -> None:
    # Filter rule: drop events where status equals "deleted".
    spec = TableTransform(filters=[FilterRule(field="status", op="=", value="deleted")])
    event = _data_event({"id": 1, "status": "deleted"})
    with pytest.raises(FilteredOutError):
        transform_event(event, spec)


def test_transform_filter_passes_non_matching() -> None:
    spec = TableTransform(filters=[FilterRule(field="status", op="=", value="deleted")])
    event = _data_event({"id": 1, "status": "pending"})
    result = transform_event(event, spec)
    assert result.row == {"id": 1, "status": "pending"}


def test_transform_no_filters_passes() -> None:
    spec = TableTransform()
    event = _data_event({"id": 1, "status": "deleted"})
    result = transform_event(event, spec)
    assert result.row == {"id": 1, "status": "deleted"}


def test_transform_delete_emits_tombstone() -> None:
    spec = TableTransform()
    event = _data_event(after=None, before={"id": 1, "status": "pending"}, op=Op.DELETE)
    result = transform_event(event, spec)
    assert result.is_tombstone is True
    assert result.row is None


def test_transform_delete_with_filter_drops() -> None:
    spec = TableTransform(filters=[FilterRule(field="status", op="=", value="deleted")])
    event = _data_event(after=None, before={"id": 1, "status": "deleted"}, op=Op.DELETE)
    with pytest.raises(FilteredOutError):
        transform_event(event, spec)


def test_transform_combines_rename_drop_filter() -> None:
    spec = TableTransform(
        column_map={"id": "order_id"},
        drop_columns=["internal_notes"],
        filters=[FilterRule(field="status", op="=", value="cancelled")],
    )
    event = _data_event(
        {"id": 1, "internal_notes": "x", "status": "cancelled"},
    )
    with pytest.raises(FilteredOutError):
        transform_event(event, spec)


def test_transform_update_applies_to_after() -> None:
    spec = TableTransform(column_map={"id": "order_id"})
    event = DataEvent(
        op=Op.UPDATE,
        before={"id": 1, "status": "pending"},
        after={"id": 1, "status": "shipped"},
        source={"db": "x", "schema": "public", "table": "orders"},
    )
    result = transform_event(event, spec)
    assert result.row == {"order_id": 1, "status": "shipped"}
