"""Integration test: ALTER TABLE ADD COLUMN auto-evolves the target table.

Requires the full docker-compose stack running.
"""
from __future__ import annotations

import os
import time

import psycopg
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("INTEGRATION") != "1",
    reason="integration tests require INTEGRATION=1 and a running docker-compose stack",
)

SOURCE_DSN = os.environ.get(
    "SOURCE_DSN",
    "host=localhost port=5433 dbname=ecommerce user=source password=source",
)
TARGET_DSN = os.environ.get(
    "TARGET_DSN",
    "host=localhost port=5434 dbname=analytics user=target password=target",
)


def test_ddl_add_column_auto_evolves() -> None:
    col_name = f"ddl_test_col_{int(time.time())}"

    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE orders ADD COLUMN {col_name} TEXT DEFAULT 'ddl-default'")
            cur.execute(
                f"INSERT INTO orders (customer_id, status, total_cents, {col_name}) "
                "SELECT id, 'pending', 1234, 'from-ddl' FROM customers LIMIT 1 RETURNING id",
            )
            order_id = cur.fetchone()[0]

    try:
        with psycopg.connect(TARGET_DSN) as conn:
            with conn.cursor() as cur:
                # Wait for the target table to gain the column.
                deadline = time.time() + 60
                evolved = False
                while time.time() < deadline:
                    cur.execute(
                        "SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name='orders' AND column_name=%s",
                        (col_name,),
                    )
                    if cur.fetchone() is not None:
                        evolved = True
                        break
                    time.sleep(1.0)
                assert evolved, f"column {col_name} did not evolve in target"

                # Wait for the new row to land with the new column populated.
                deadline = time.time() + 30
                found_value = None
                while time.time() < deadline:
                    cur.execute(
                        f"SELECT {col_name} FROM public.orders WHERE order_id = %s",
                        (order_id,),
                    )
                    row = cur.fetchone()
                    if row is not None:
                        found_value = row[0]
                        if found_value == "from-ddl":
                            return
                    time.sleep(1.0)
                pytest.fail(
                    f"new column data did not flow through (got {found_value!r})"
                )
    finally:
        with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE orders DROP COLUMN {col_name}")
                cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))
