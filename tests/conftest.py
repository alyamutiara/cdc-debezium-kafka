"""Pytest fixtures shared across unit and integration tests."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_config_yaml() -> str:
    return """
source_topics:
  - cdc.source.orders
  - cdc.source.customers
output_topic_prefix: cdc.transformed
transforms:
  orders:
    column_map:
      id: order_id
      customer_id: cust_id
    drop_columns:
      - internal_notes
    filters:
      - field: status
        op: "!="
        value: "deleted"
  customers:
    drop_columns:
      - password_hash
"""
