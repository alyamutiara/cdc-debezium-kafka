"""End-to-end integration test: insert/update/delete on source lands in target.

Requires the full docker-compose stack running. Skipped if `INTEGRATION=1`
is not set, so unit-test runs stay fast and offline.

Run with:
    INTEGRATION=1 pytest tests/test_e2e.py -v
"""
from __future__ import annotations

import os
import time
import uuid

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
E2E_TIMEOUT_S = 30


def _query_target_count(table: str) -> int:
    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public.{table}')
            return cur.fetchone()[0]


def _wait_for_count(table: str, expected: int, timeout: float = E2E_TIMEOUT_S) -> int:
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        last = _query_target_count(table)
        if last >= expected:
            return last
        time.sleep(1.0)
    return last


def test_e2e_insert_lands_in_target() -> None:
    marker = f"e2e-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "pending", 4242, marker),
            )
            order_id = cur.fetchone()[0]

    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + E2E_TIMEOUT_S
            found = False
            while time.time() < deadline:
                cur.execute("SELECT order_id, status, total_cents FROM public.orders WHERE order_id = %s", (order_id,))
                row = cur.fetchone()
                if row is not None:
                    # Note: `id` is renamed to `order_id` by the transform config.
                    assert row[1] == "pending"
                    assert row[2] == 4242
                    found = True
                    break
                time.sleep(1.0)
            assert found, f"order {order_id} never appeared in target"


def test_e2e_filter_drops_deleted_status() -> None:
    # The transform config drops events with status='deleted'.
    # Insert a row with status='deleted' on source; it should NOT land in target.
    marker = f"filtered-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "deleted", 999, marker),
            )
            order_id = cur.fetchone()[0]

    time.sleep(10)  # wait long enough for CDC to flow
    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM public.orders WHERE order_id = %s", (order_id,))
            row = cur.fetchone()
            assert row is None, f"filtered event {order_id} should not be in target"


def test_e2e_column_rename_applied() -> None:
    marker = f"rename-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "pending", 1111, marker),
            )
            order_id = cur.fetchone()[0]

    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + E2E_TIMEOUT_S
            found = False
            while time.time() < deadline:
                # The transform renames `id` -> `order_id` and `customer_id` -> `cust_id`.
                cur.execute(
                    "SELECT order_id, cust_id FROM public.orders WHERE order_id = %s",
                    (order_id,),
                )
                row = cur.fetchone()
                if row is not None:
                    assert row[0] == order_id
                    assert row[1] == cust_id
                    found = True
                    break
                time.sleep(1.0)
            assert found


def test_e2e_drop_column_excluded() -> None:
    marker = f"dropcol-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "pending", 555, marker),
            )
            order_id = cur.fetchone()[0]

    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + E2E_TIMEOUT_S
            while time.time() < deadline:
                cur.execute(
                    "SELECT internal_notes FROM public.orders WHERE order_id = %s",
                    (order_id,),
                )
                row = cur.fetchone()
                if row is not None:
                    assert row[0] is None
                    return
                time.sleep(1.0)
            pytest.fail("order did not appear in target")


def test_e2e_delete_lands_as_tombstone() -> None:
    # Insert a row, wait for it to land, then delete it and verify it disappears.
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "pending", 7777, None),
            )
            order_id = cur.fetchone()[0]

    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + E2E_TIMEOUT_S
            while time.time() < deadline:
                cur.execute("SELECT 1 FROM public.orders WHERE order_id = %s", (order_id,))
                if cur.fetchone() is not None:
                    break
                time.sleep(1.0)
            else:
                pytest.fail("insert never appeared in target")

    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM orders WHERE id = %s", (order_id,))

    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + E2E_TIMEOUT_S
            while time.time() < deadline:
                cur.execute("SELECT 1 FROM public.orders WHERE order_id = %s", (order_id,))
                if cur.fetchone() is None:
                    return
                time.sleep(1.0)
            pytest.fail("delete never propagated to target")
