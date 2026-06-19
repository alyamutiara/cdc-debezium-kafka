"""Data generator: simulates live traffic on the source Postgres.

Generates INSERTs, UPDATEs, and DELETEs on a configurable interval to
demonstrate end-to-end CDC flow.
"""
from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime, timezone

import psycopg
from psycopg import errors as pg_errors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("data-generator")

PG_HOST = os.environ["PG_HOST"]
PG_PORT = int(os.environ["PG_PORT"])
PG_DB = os.environ["PG_DB"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

INSERT_INTERVAL = int(os.environ.get("INSERT_INTERVAL_SECONDS", "2"))
UPDATE_INTERVAL = int(os.environ.get("UPDATE_INTERVAL_SECONDS", "10"))
DELETE_INTERVAL = int(os.environ.get("DELETE_INTERVAL_SECONDS", "60"))

STATUSES = ["pending", "shipped", "delivered", "cancelled", "deleted"]


def conn_str() -> str:
    return (
        f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
        f"user={PG_USER} password={PG_PASSWORD}"
    )


def connect_with_retry(retries: int = 30, delay: float = 2.0) -> psycopg.Connection:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            conn = psycopg.connect(conn_str(), autocommit=True)
            log.info("connected to source postgres (attempt %d)", attempt + 1)
            return conn
        except Exception as exc:
            last_exc = exc
            log.warning("connect attempt %d failed: %s", attempt + 1, exc)
            time.sleep(delay)
    raise RuntimeError(f"could not connect to postgres: {last_exc}")


def insert_order(conn: psycopg.Connection) -> int | None:
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return None
            customer_id = row[0]
            status = random.choice([s for s in STATUSES if s != "deleted"])
            total = random.randint(1000, 50000)
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes) "
                "VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, status, total, f"auto-generated at {datetime.now(timezone.utc).isoformat()}"),
            )
            order_id = cur.fetchone()[0]
            log.info("INSERT order id=%d customer=%d total=%d", order_id, customer_id, total)
            return order_id
    except pg_errors.Error as exc:
        log.error("insert failed: %s", exc)
        return None


def update_random_order(conn: psycopg.Connection) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = %s, total_cents = total_cents + %s "
                "WHERE id = (SELECT id FROM orders ORDER BY RANDOM() LIMIT 1) "
                "RETURNING id, status",
                (random.choice(STATUSES), random.randint(-500, 500)),
            )
            row = cur.fetchone()
            if row is not None:
                log.info("UPDATE order id=%d status=%s", row[0], row[1])
    except pg_errors.Error as exc:
        log.error("update failed: %s", exc)


def delete_oldest_pending(conn: psycopg.Connection) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM orders WHERE id = ("
                "  SELECT id FROM orders WHERE status = 'pending' "
                "  ORDER BY created_at ASC LIMIT 1"
                ") RETURNING id"
            )
            row = cur.fetchone()
            if row is not None:
                log.info("DELETE order id=%d", row[0])
    except pg_errors.Error as exc:
        log.error("delete failed: %s", exc)


def main() -> None:
    conn = connect_with_retry()
    last_insert = 0.0
    last_update = 0.0
    last_delete = 0.0
    log.info(
        "starting generator (insert=%ds update=%ds delete=%ds)",
        INSERT_INTERVAL, UPDATE_INTERVAL, DELETE_INTERVAL,
    )
    while True:
        now = time.monotonic()
        if now - last_insert >= INSERT_INTERVAL:
            insert_order(conn)
            last_insert = now
        if now - last_update >= UPDATE_INTERVAL:
            update_random_order(conn)
            last_update = now
        if now - last_delete >= DELETE_INTERVAL:
            delete_oldest_pending(conn)
            last_delete = now
        time.sleep(0.5)


if __name__ == "__main__":
    main()
