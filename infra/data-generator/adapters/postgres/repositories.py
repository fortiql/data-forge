"""Postgres Adapters – DIP implementations for repositories.

SRP: keep SQL isolated here. Services see only ports.
"""
from __future__ import annotations

import random
from typing import Iterable, Tuple

import psycopg2

from config import Config
from domain.enums import CATEGORIES, COUNTRIES, CUSTOMER_SEGMENTS
from ports.repositories import InventoryRepository, PricingRepository, ProductRepository


def pg_connect(config: Config):
    conn = psycopg2.connect(config.pg_dsn)
    conn.autocommit = True
    return conn


class PgInventoryRepository(InventoryRepository):
    def __init__(self, conn) -> None:
        self.conn = conn

    def read_qty_reserved(self, warehouse_id: str, product_id: str) -> Tuple[int, int]:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s",
                (warehouse_id, product_id),
            )
            row = cur.fetchone()
        return (row or (random.randint(0, 100), 0))

    def upsert_qty_reserved(self, warehouse_id: str, product_id: str, qty: int, reserved: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO warehouse_inventory (warehouse_id, product_id, qty, reserved_qty)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (warehouse_id, product_id)
                DO UPDATE SET qty = %s, reserved_qty = %s, updated_at = now()
                """,
                (warehouse_id, product_id, qty, reserved, qty, reserved),
            )

    def maybe_update_random_inventory(self) -> None:
        # Legacy behavior preserved: random product+warehouse delta with reserved consumption
        with self.conn.cursor() as cur:
            cur.execute("SELECT warehouse_id FROM warehouses ORDER BY random() LIMIT 1")
            wid = (cur.fetchone() or ("WH000",))[0]
            cur.execute("SELECT product_id FROM products ORDER BY random() LIMIT 1")
            pid = (cur.fetchone() or ("P000000",))[0]
            cur.execute(
                "SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s",
                (wid, pid),
            )
            row = cur.fetchone()
            qty, reserved = (row or (random.randint(0, 100), 0))
        delta = random.randint(-5, 20)
        if delta < 0:
            use_res = min(reserved, -delta)
            reserved -= use_res
            delta += use_res
        new_qty = max(0, qty + delta)
        self.upsert_qty_reserved(wid, pid, new_qty, reserved)


class PgPricingRepository(PricingRepository):
    def __init__(self, conn) -> None:
        self.conn = conn

    def maybe_update_random_price(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_id FROM products ORDER BY random() LIMIT 1")
            pid = (cur.fetchone() or ("P000000",))[0]
        factor = random.choice([0.95, 0.97, 1.03, 1.05])
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET price_usd = ROUND(price_usd * %s, 2), updated_at = now() WHERE product_id = %s",
                (factor, pid),
            )


class PgProductRepository(ProductRepository):
    def __init__(self, conn) -> None:
        self.conn = conn

    def preload_recent_products(self, limit: int) -> Iterable[str]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT product_id FROM products LIMIT %s", (limit,))
            for (pid,) in cur.fetchall():
                yield pid
