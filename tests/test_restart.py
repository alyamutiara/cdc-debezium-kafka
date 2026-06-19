"""Integration test: pipeline resumes from last committed offset after restart.

Requires the full docker-compose stack running.
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


def test_restart_resumes_from_committed_offset() -> None:
    marker = f"restart-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "pending", 2222, marker),
            )
            order_id = cur.fetchone()[0]

    # Wait for the row to land in target pre-restart.
    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + 30
            while time.time() < deadline:
                cur.execute("SELECT 1 FROM public.orders WHERE order_id = %s", (order_id,))
                if cur.fetchone() is not None:
                    break
                time.sleep(1.0)
            else:
                pytest.fail("row did not land pre-restart")

    # Restart just the transform service. The consumer group should
    # resume from the last committed offset.
    import subprocess

    subprocess.run(
        ["docker", "compose", "restart", "transform-service"],
        check=True,
        capture_output=True,
    )
    time.sleep(15)  # allow consumer to rejoin group

    # Insert another row — it should land in target within the standard window.
    with psycopg.connect(SOURCE_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers LIMIT 1")
            cust_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (cust_id, "pending", 3333, marker + "-post"),
            )
            post_id = cur.fetchone()[0]

    with psycopg.connect(TARGET_DSN) as conn:
        with conn.cursor() as cur:
            deadline = time.time() + 30
            while time.time() < deadline:
                cur.execute("SELECT 1 FROM public.orders WHERE order_id = %s", (post_id,))
                if cur.fetchone() is not None:
                    return
                time.sleep(1.0)
            pytest.fail("post-restart row did not land")
