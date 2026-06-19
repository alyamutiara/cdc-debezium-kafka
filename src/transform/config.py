"""Pydantic config model for the transform service.

Validates the YAML config at startup so typos and unsupported constructs are
rejected with a clear error before the service accepts traffic.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

SUPPORTED_OPS = {"=", "!=", "<", "<=", ">", ">=", "in", "not in"}
VALID_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class FilterRule(BaseModel):
    """A predicate that drops events when it matches.

    `field` is a column in the Debezium `after` payload.
    `op` is one of SUPPORTED_OPS.
    `value` is compared against the field value.
    """

    field: str
    op: str
    value: Any

    @field_validator("field")
    @classmethod
    def _field_is_ident(cls, v: str) -> str:
        if not VALID_IDENT.match(v):
            raise ValueError(f"invalid field name: {v!r}")
        return v

    @field_validator("op")
    @classmethod
    def _op_supported(cls, v: str) -> str:
        if v not in SUPPORTED_OPS:
            raise ValueError(
                f"unsupported filter op {v!r}; expected one of {sorted(SUPPORTED_OPS)}"
            )
        return v

    @model_validator(mode="after")
    def _value_type_ok(self) -> FilterRule:
        if self.op in ("in", "not in") and not isinstance(self.value, list):
            raise ValueError(f"op {self.op!r} requires list value")
        return self


class TableTransform(BaseModel):
    """Per-table transform config: column map, dropped columns, filters."""

    column_map: dict[str, str] = Field(default_factory=dict)
    drop_columns: list[str] = Field(default_factory=list)
    filters: list[FilterRule] = Field(default_factory=list)

    @field_validator("column_map")
    @classmethod
    def _map_keys_and_values_are_idents(cls, v: dict[str, str]) -> dict[str, str]:
        for src, dst in v.items():
            if not VALID_IDENT.match(src):
                raise ValueError(f"invalid column_map key: {src!r}")
            if not VALID_IDENT.match(dst):
                raise ValueError(f"invalid column_map value: {dst!r}")
        return v

    @field_validator("drop_columns")
    @classmethod
    def _drop_columns_are_idents(cls, v: list[str]) -> list[str]:
        for c in v:
            if not VALID_IDENT.match(c):
                raise ValueError(f"invalid drop_columns entry: {c!r}")
        return v


class TransformConfig(BaseModel):
    """Top-level transform service config."""

    source_topics: list[str] = Field(min_length=1)
    output_topic_prefix: str = "cdc.transformed"
    transforms: dict[str, TableTransform] = Field(default_factory=dict)

    @field_validator("source_topics")
    @classmethod
    def _topics_well_formed(cls, v: list[str]) -> list[str]:
        for t in v:
            if not t or not isinstance(t, str):
                raise ValueError(f"invalid topic: {t!r}")
            if "." not in t:
                raise ValueError(
                    f"topic {t!r} should follow cdc.source.{{schema}}.{{table}} convention"
                )
        return v

    @field_validator("output_topic_prefix")
    @classmethod
    def _prefix_nonempty(cls, v: str) -> str:
        if not v or not v.replace(".", "").replace("_", "").isalnum():
            raise ValueError(f"invalid output_topic_prefix: {v!r}")
        return v

    def topic_for_table(self, source_topic: str) -> str:
        """Map a source topic like cdc.source.public.orders to cdc.transformed.orders."""
        # Source topics are routed by the Debezium RegexRouter to cdc.source.{table}.
        parts = source_topic.split(".")
        table = parts[-1]
        return f"{self.output_topic_prefix}.{table}"


def load_config(path: str | Path) -> TransformConfig:
    """Load and validate a transform config YAML file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"config not found: {p}")
    raw = yaml.safe_load(p.read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError("config root must be a mapping")
    return TransformConfig.model_validate(raw)
