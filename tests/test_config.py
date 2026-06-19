"""Tests for the Pydantic config loader."""
from __future__ import annotations

from pathlib import Path

import pytest

from transform.config import FilterRule, TableTransform, TransformConfig, load_config


def test_load_valid_config(tmp_path: Path, sample_config_yaml: str) -> None:
    cfg_path = tmp_path / "transform.yaml"
    cfg_path.write_text(sample_config_yaml)
    cfg = load_config(cfg_path)
    assert cfg.source_topics == ["cdc.source.orders", "cdc.source.customers"]
    assert cfg.output_topic_prefix == "cdc.transformed"
    assert "orders" in cfg.transforms
    assert cfg.transforms["orders"].column_map == {"id": "order_id", "customer_id": "cust_id"}
    assert cfg.transforms["orders"].drop_columns == ["internal_notes"]
    assert len(cfg.transforms["orders"].filters) == 1


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yaml")


def test_empty_yaml_missing_source_topics_raises(tmp_path: Path) -> None:
    cfg_path = tmp_path / "transform.yaml"
    cfg_path.write_text("")
    with pytest.raises(Exception):  # ValidationError
        load_config(cfg_path)


def test_empty_yaml_file_raises(tmp_path: Path) -> None:
    """An empty file yields an empty mapping, which fails source_topics validation."""
    cfg_path = tmp_path / "transform.yaml"
    cfg_path.write_text("")
    with pytest.raises(Exception):  # ValidationError
        load_config(cfg_path)


def test_invalid_field_name_in_filter_rejected() -> None:
    with pytest.raises(Exception):
        FilterRule(field="bad-name", op="=", value=1)


def test_unsupported_op_rejected() -> None:
    with pytest.raises(Exception):
        FilterRule(field="x", op="~~", value=1)


def test_in_op_requires_list() -> None:
    with pytest.raises(Exception):
        FilterRule(field="x", op="in", value="notalist")


def test_in_op_with_list_ok() -> None:
    rule = FilterRule(field="x", op="in", value=[1, 2, 3])
    assert rule.value == [1, 2, 3]


def test_invalid_column_map_key_rejected() -> None:
    with pytest.raises(Exception):
        TableTransform(column_map={"bad-key": "good_key"})


def test_invalid_column_map_value_rejected() -> None:
    with pytest.raises(Exception):
        TableTransform(column_map={"good_key": "bad-key"})


def test_invalid_drop_column_rejected() -> None:
    with pytest.raises(Exception):
        TableTransform(drop_columns=["bad-column"])


def test_topic_requires_dot_separator() -> None:
    with pytest.raises(Exception):
        TransformConfig(source_topics=["no_dot_here"])


def test_topic_prefix_validation() -> None:
    with pytest.raises(Exception):
        TransformConfig(
            source_topics=["cdc.source.orders"],
            output_topic_prefix="bad prefix with spaces",
        )


def test_topic_for_table_strips_schema() -> None:
    cfg = TransformConfig(source_topics=["cdc.source.orders"])
    assert cfg.topic_for_table("cdc.source.orders") == "cdc.transformed.orders"


def test_topic_for_table_with_schema() -> None:
    cfg = TransformConfig(source_topics=["cdc.source.public.orders"])
    assert cfg.topic_for_table("cdc.source.public.orders") == "cdc.transformed.orders"


def test_load_config_rejects_non_mapping_root(tmp_path: Path) -> None:
    cfg_path = tmp_path / "transform.yaml"
    cfg_path.write_text("- just\n- a\n- list\n")
    with pytest.raises(ValueError):
        load_config(cfg_path)
