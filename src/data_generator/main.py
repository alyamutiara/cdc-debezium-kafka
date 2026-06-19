"""Realistic e-commerce transaction generator for CDC pipeline.

Simulates a live storefront by producing INSERTs, UPDATEs, and DELETEs
with real-world patterns across all four tables (customers, products,
orders, order_items). Every row carries an explicit timestamp; primary
keys are assigned by Postgres SERIAL sequences and tracked here so FK
relationships stay consistent.
"""
from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime

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

INSERT_INTERVAL = float(os.environ.get("INSERT_INTERVAL_SECONDS", "2"))
UPDATE_INTERVAL = float(os.environ.get("UPDATE_INTERVAL_SECONDS", "10"))
DELETE_INTERVAL = float(os.environ.get("DELETE_INTERVAL_SECONDS", "60"))
SEED_COUNT = int(os.environ.get("SEED_COUNT", "50"))

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mia", "Nora", "Oscar", "Priya",
    "Quinn", "Raj", "Sara", "Tom", "Uma", "Vik", "Wendy", "Xander",
    "Yara", "Zoe", "Amir", "Bella", "Carlos", "Diana", "Eli", "Fiona",
    "George", "Hana", "Ian", "Jada", "Kofi", "Lena", "Max", "Nina",
]

LAST_NAMES = [
    "Smith", "Jones", "Lee", "Patel", "Kim", "Garcia", "Chen", "Brown",
    "Singh", "Nguyen", "Andersen", "Müller", "Santos", "Johansson",
    "Tanaka", "Rivera", "Petrov", "Okafor", "Nakamura", "Dubois",
]

PRODUCT_CATALOG = [
    ("SKU-0100", "Mechanical Keyboard", 12999, "premium line"),
    ("SKU-0101", "Wireless Keyboard", 8999, "ergonomic"),
    ("SKU-0200", "Wireless Mouse", 4999, "best seller"),
    ("SKU-0201", "Gaming Mouse", 7999, "RGB lighting"),
    ("SKU-0300", "USB-C Hub", 3499, "7-port"),
    ("SKU-0301", "Thunderbolt Dock", 24999, "dual display"),
    ("SKU-0400", "27\" Monitor", 39999, "4K IPS"),
    ("SKU-0401", "24\" Monitor", 19999, "FHD"),
    ("SKU-0500", "Webcam 1080p", 6999, "autofocus"),
    ("SKU-0501", "Streaming Mic", 14999, "condenser"),
    ("SKU-0600", "Laptop Stand", 3999, "aluminum"),
    ("SKU-0601", "Desk Mat XL", 2999, "vegan leather"),
    ("SKU-0700", "SSD 1TB", 8999, "NVMe"),
    ("SKU-0701", "RAM 32GB DDR5", 11999, "5200MHz"),
    ("SKU-0800", "Router WiFi6", 12999, "mesh capable"),
    ("SKU-0801", "Ethernet Switch", 5999, "8-port gigabit"),
    ("SKU-0900", "Noise-Cancelling Headphones", 24999, "ANC"),
    ("SKU-0901", "Bluetooth Speaker", 7999, "waterproof"),
    ("SKU-1000", " backpack 30L", 8999, "laptop sleeve"),
    ("SKU-1001", "Cable Organizer Kit", 1999, "assorted"),
]

DOMAINS = [
    "gmail.com", "outlook.com", "yahoo.com", "protonmail.com",
    "icloud.com", "fastmail.com", "hey.com",
]

ORDER_STATUSES = ["pending", "confirmed", "processing", "shipped", "delivered"]
CANCELLABLE = ["pending", "confirmed"]


class State:
    """Tracks IDs and stats for the running generator."""

    def __init__(self) -> None:
        self.customer_ids: list[int] = []
        self.product_ids: list[int] = []
        self.product_prices: dict[int, int] = {}
        self.order_ids: list[int] = []
        self.orders_inserted = 0
        self.orders_updated = 0
        self.orders_deleted = 0
        self.customers_inserted = 0
        self.products_inserted = 0

    def pick_customer(self) -> int | None:
        if not self.customer_ids:
            return None
        return random.choice(self.customer_ids)

    def pick_product(self) -> tuple[int, int] | None:
        if not self.product_ids:
            return None
        pid = random.choice(self.product_ids)
        return pid, self.product_prices[pid]

    def pick_order(self) -> int | None:
        if not self.order_ids:
            return None
        return random.choice(self.order_ids)


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


def _ts() -> str:
    return datetime.now(datetime.UTC).isoformat()


def seed_products(conn: psycopg.Connection, state: State) -> None:
    for sku, name, price_cents, notes in PRODUCT_CATALOG:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO products (sku, name, price_cents, internal_notes) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT (sku) DO NOTHING "
                    "RETURNING id",
                    (sku, name, price_cents, notes),
                )
                row = cur.fetchone()
                if row is not None:
                    pid = row[0]
                    state.product_ids.append(pid)
                    state.product_prices[pid] = price_cents
                    state.products_inserted += 1
        except pg_errors.Error as exc:
            log.warning("seed product %s failed: %s", sku, exc)


def seed_customers(conn: psycopg.Connection, state: State) -> None:
    for i in range(SEED_COUNT):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        domain = random.choice(DOMAINS)
        email = f"{first.lower()}.{last.lower()}{i}@{domain}"
        full_name = f"{first} {last}"
        pw_hash = f"$2b$12$seed{i:04d}"
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO customers (email, full_name, password_hash, ssn, created_at) "
                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (email) DO NOTHING "
                    "RETURNING id",
                    (email, full_name, pw_hash, None, _ts()),
                )
                row = cur.fetchone()
                if row is not None:
                    state.customer_ids.append(row[0])
                    state.customers_inserted += 1
        except pg_errors.Error as exc:
            log.warning("seed customer %s failed: %s", email, exc)


def insert_customer(conn: psycopg.Connection, state: State) -> None:
    idx = len(state.customer_ids) + 1000
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    domain = random.choice(DOMAINS)
    email = f"{first.lower()}.{last.lower()}{idx}@{domain}"
    full_name = f"{first} {last}"
    pw_hash = f"$2b$12$gen{idx:06d}"
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO customers (email, full_name, password_hash, ssn, created_at) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (email, full_name, pw_hash, None, _ts()),
            )
            cid = cur.fetchone()[0]
            state.customer_ids.append(cid)
            state.customers_inserted += 1
            log.info("INSERT customer id=%d email=%s", cid, email)
    except pg_errors.UniqueViolation:
        pass
    except pg_errors.Error as exc:
        log.error("insert customer failed: %s", exc)


def insert_product(conn: psycopg.Connection, state: State) -> None:
    idx = len(state.product_ids) + 200
    sku = f"SKU-{random.randint(9000, 9999)}"
    names = [
        "Bluetooth Adapter", "USB Flash Drive 64GB", "HDMI Cable 2m",
        "Monitor Arm", "Wrist Rest", "Mouse Pad XL", "Cable Clip Set",
        "Screen Protector 27\"", "Power Strip 6-outlet", "Desk Organizer",
    ]
    name = random.choice(names) + f" v{idx}"
    price = random.choice([999, 1499, 1999, 2499, 3999, 5999, 9999, 14999, 29999])
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products (sku, name, price_cents, internal_notes, created_at) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (sku, name, price, f"auto-added {_ts()}", _ts()),
            )
            pid = cur.fetchone()[0]
            state.product_ids.append(pid)
            state.product_prices[pid] = price
            state.products_inserted += 1
            log.info("INSERT product id=%d sku=%s price=%d", pid, sku, price)
    except pg_errors.UniqueViolation:
        pass
    except pg_errors.Error as exc:
        log.error("insert product failed: %s", exc)


def insert_order(conn: psycopg.Connection, state: State) -> None:
    customer_id = state.pick_customer()
    if customer_id is None:
        log.warning("no customers yet, skipping order insert")
        return

    status = random.choices(
        ORDER_STATUSES,
        weights=[40, 25, 15, 12, 8],
    )[0]
    ts = _ts()

    num_items = random.choices([1, 2, 3, 4, 5], weights=[35, 30, 20, 10, 5])[0]
    picked = _pick_unique_products(state, num_items)
    if not picked:
        return

    total_cents = sum(price * qty for _, price, qty in picked)

    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (customer_id, status, total_cents, internal_notes, created_at) "
                "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (customer_id, status, total_cents, f"order at {ts}", ts),
            )
            order_id = cur.fetchone()[0]
            state.order_ids.append(order_id)
            state.orders_inserted += 1

            for product_id, price_cents, qty in picked:
                cur.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, price_cents) "
                    "VALUES (%s, %s, %s, %s)",
                    (order_id, product_id, qty, price_cents),
                )

            log.info(
                "INSERT order id=%d customer=%d status=%s items=%d total=%d ts=%s",
                order_id, customer_id, status, len(picked), total_cents, ts,
            )
    except pg_errors.Error as exc:
        log.error("insert order failed: %s", exc)


def update_order_status(conn: psycopg.Connection, state: State) -> None:
    order_id = state.pick_order()
    if order_id is None:
        return

    transitions = [
        ("pending", "confirmed"),
        ("confirmed", "processing"),
        ("processing", "shipped"),
        ("shipped", "delivered"),
    ]
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
            row = cur.fetchone()
            if row is None:
                state.order_ids.remove(order_id)
                return
            current = row[0]
            new_status = None
            for src, dst in transitions:
                if current == src:
                    new_status = dst
                    break
            if new_status is None:
                new_status = random.choice(CANCELLABLE) if current in CANCELLABLE else current
            ts = _ts()
            cur.execute(
                "UPDATE orders SET status = %s, internal_notes = %s, created_at = %s "
                "WHERE id = %s RETURNING status",
                (new_status, f"status changed at {ts}", ts),
            )
            updated = cur.fetchone()
            if updated is not None:
                state.orders_updated += 1
                log.info("UPDATE order id=%d %s->%s ts=%s", order_id, current, updated[0], ts)
    except pg_errors.Error as exc:
        log.error("update order failed: %s", exc)


def cancel_order(conn: psycopg.Connection, state: State) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM orders WHERE status IN %s ORDER BY created_at ASC LIMIT 1",
                (tuple(CANCELLABLE),),
            )
            row = cur.fetchone()
            if row is None:
                return
            order_id = row[0]
            ts = _ts()
            cur.execute(
                "UPDATE orders SET status = 'cancelled', internal_notes = %s, created_at = %s "
                "WHERE id = %s",
                (f"cancelled at {ts}", ts, order_id),
            )
            state.orders_updated += 1
            log.info("CANCEL order id=%d ts=%s", order_id, ts)
    except pg_errors.Error as exc:
        log.error("cancel order failed: %s", exc)


def delete_order(conn: psycopg.Connection, state: State) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM orders WHERE status = 'cancelled' "
                "ORDER BY created_at ASC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                return
            order_id = row[0]
            cur.execute("DELETE FROM order_items WHERE order_id = %s", (order_id,))
            cur.execute("DELETE FROM orders WHERE id = %s RETURNING id", (order_id,))
            deleted = cur.fetchone()
            if deleted is not None:
                state.order_ids.remove(order_id)
                state.orders_deleted += 1
                log.info("DELETE order id=%d", order_id)
    except (ValueError, pg_errors.Error) as exc:
        if isinstance(exc, ValueError):
            pass
        else:
            log.error("delete order failed: %s", exc)


def update_customer(conn: psycopg.Connection, state: State) -> None:
    customer_id = state.pick_customer()
    if customer_id is None:
        return
    new_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    ts = _ts()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE customers SET full_name = %s, created_at = %s WHERE id = %s RETURNING id",
                (new_name, ts, customer_id),
            )
            row = cur.fetchone()
            if row is not None:
                log.info("UPDATE customer id=%d name=%s ts=%s", customer_id, new_name, ts)
    except pg_errors.Error as exc:
        log.error("update customer failed: %s", exc)


def update_product_price(conn: psycopg.Connection, state: State) -> None:
    product_id, old_price = state.pick_product() or (None, 0)
    if product_id is None:
        return
    delta = random.choice([-500, -200, -100, 100, 200, 500, 1000])
    new_price = max(99, old_price + delta)
    state.product_prices[product_id] = new_price
    ts = _ts()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET price_cents = %s, internal_notes = %s, created_at = %s "
                "WHERE id = %s RETURNING name",
                (new_price, f"price change at {ts}", ts, product_id),
            )
            row = cur.fetchone()
            if row is not None:
                log.info(
                    "UPDATE product id=%d price %d->%d ts=%s",
                    product_id, old_price, new_price, ts,
                )
    except pg_errors.Error as exc:
        log.error("update product failed: %s", exc)


def _pick_unique_products(
    state: State, count: int,
) -> list[tuple[int, int, int]]:
    if not state.product_ids:
        return []
    available = list(state.product_ids)
    random.shuffle(available)
    result: list[tuple[int, int, int]] = []
    for pid in available[:count]:
        price = state.product_prices.get(pid, 999)
        qty = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
        result.append((pid, price, qty))
    return result


def print_stats(state: State) -> None:
    log.info(
        "stats: customers=%d products=%d orders=%d "
        "inserted=%d updated=%d deleted=%d",
        len(state.customer_ids),
        len(state.product_ids),
        len(state.order_ids),
        state.orders_inserted,
        state.orders_updated,
        state.orders_deleted,
    )


def main() -> None:
    conn = connect_with_retry()
    state = State()

    log.info("seeding %d customers and %d products...", SEED_COUNT, len(PRODUCT_CATALOG))
    seed_products(conn, state)
    seed_customers(conn, state)
    log.info(
        "seeded %d customers, %d products", len(state.customer_ids), len(state.product_ids),
    )

    last_insert = 0.0
    last_update = 0.0
    last_delete = 0.0
    last_stat = 0.0
    cycle = 0

    log.info(
        "starting generator (insert=%.1fs update=%.1fs delete=%.1fs)",
        INSERT_INTERVAL, UPDATE_INTERVAL, DELETE_INTERVAL,
    )

    while True:
        now = time.monotonic()

        if now - last_insert >= INSERT_INTERVAL:
            roll = random.random()
            if roll < 0.70:
                insert_order(conn, state)
            elif roll < 0.82:
                insert_customer(conn, state)
            elif roll < 0.90:
                insert_product(conn, state)
            else:
                update_customer(conn, state)
                update_product_price(conn, state)
            last_insert = now

        if now - last_update >= UPDATE_INTERVAL:
            roll = random.random()
            if roll < 0.65:
                update_order_status(conn, state)
            elif roll < 0.85:
                cancel_order(conn, state)
            else:
                update_product_price(conn, state)
            last_update = now

        if now - last_delete >= DELETE_INTERVAL:
            delete_order(conn, state)
            last_delete = now

        if now - last_stat >= 30.0:
            print_stats(state)
            last_stat = now

        cycle += 1
        time.sleep(0.5)


if __name__ == "__main__":
    main()