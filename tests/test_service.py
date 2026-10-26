from __future__ import annotations

import json

from transform.service import _encode_connect_record, _transform_key


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
            "fields": [{"type": "int32", "optional": False, "field": "id"}],
            "optional": False,
            "name": "cdc.source.public.orders.Key",
        },
        "payload": {"id": 7},
    }).encode("utf-8")

    transformed = json.loads(_transform_key(key, {"id": "order_id"}).decode("utf-8"))

    assert transformed["payload"] == {"order_id": 7}
    assert transformed["schema"]["name"] == "cdc.source.public.orders.Key"
    assert transformed["schema"]["fields"] == [
        {"type": "int32", "optional": False, "field": "order_id"},
    ]
