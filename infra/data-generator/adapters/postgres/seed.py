"""Seeder – SRP: bulk upserts for reference data.

Idempotent, minimal, and fast. Kept close to SQL.
"""
from __future__ import annotations

import random
from typing import Deque

from psycopg2.extras import execute_batch

from config import Config
from domain.enums import CATEGORIES, COUNTRIES, CUSTOMER_SEGMENTS


def seed_postgres(conn, config: Config, recent_deques: dict[str, Deque[str]]) -> None:
    print("🌱 Seeding Postgres reference data...")
    warehouses = []
    for i in range(config.seed_warehouses):
        country = random.choice(COUNTRIES)
        region = "North America" if country in {"US", "CA"} else "Europe"
        warehouse_id = f"WH{i+1:03d}"
        warehouses.append((warehouse_id, f"Warehouse {i+1}", country, region))
        recent_deques.get("warehouses").append(warehouse_id)

    suppliers = []
    for i in range(config.seed_suppliers):
        country = random.choice(COUNTRIES)
        rating = round(random.uniform(3.0, 5.0), 1)
        supplier_id = f"SUP{i+1:03d}"
        suppliers.append((supplier_id, f"Supplier {i+1}", country, rating))
        recent_deques.get("suppliers").append(supplier_id)

    users = []
    for i in range(config.seed_users):
        country = random.choice(COUNTRIES)
        email = f"user{i+1}@example.com"
        user_id = f"U{i+1:06d}"
        users.append((user_id, email, country))
        if i >= int(config.seed_users * 0.1):
            recent_deques.get("users").append(user_id)

    products = []
    for i in range(config.seed_products):
        category = random.choice(CATEGORIES)
        price = round(random.uniform(10.0, 500.0), 2)
        product_id = f"P{i+1:06d}"
        products.append((product_id, f"Product {i+1}", category, price))
        if i >= int(config.seed_products * 0.1):
            recent_deques.get("products").append(product_id)

    inv_global = [(pid, random.randint(0, 1000)) for pid, *_ in products]
    seg_weights = [0.05, 0.70, 0.20, 0.05]
    seg_rows = []
    for uid, _, _ in users:
        segment = random.choices(CUSTOMER_SEGMENTS, weights=seg_weights)[0]
        ltv = {
            "VIP": random.uniform(5000, 50000),
            "Regular": random.uniform(500, 5000),
            "New": random.uniform(0, 500),
            "Churned": random.uniform(0, 1000),
        }[segment]
        seg_rows.append((uid, segment, round(ltv, 2)))

    prod_supp = []
    for pid, _, _, price in products:
        chosen = random.sample(suppliers, min(random.randint(1, 3), len(suppliers)))
        for sid, *_ in chosen:
            cost = round(float(price) * random.uniform(0.3, 0.7), 2)
            lead_time = random.randint(3, 21)
            prod_supp.append((pid, sid, cost, lead_time))

    inv_by_wh = []
    for wid, *_ in warehouses:
        for pid, *_ in products:
            if random.random() < 0.8:
                qty = random.randint(10, 500)
                reserved = random.randint(0, min(qty, 50))
                inv_by_wh.append((wid, pid, qty, reserved))

    prev_autocommit = getattr(conn, "autocommit", False)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO warehouses(warehouse_id,name,country,region) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                warehouses,
            )
            execute_batch(
                cur,
                "INSERT INTO suppliers(supplier_id,name,country,rating) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                suppliers,
            )
            execute_batch(
                cur,
                "INSERT INTO users(user_id,email,country) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                users,
            )
            execute_batch(
                cur,
                "INSERT INTO products(product_id,title,category,price_usd) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                products,
            )
            execute_batch(
                cur,
                "INSERT INTO inventory(product_id,qty) VALUES (%s,%s) ON CONFLICT (product_id) DO UPDATE SET qty=EXCLUDED.qty",
                inv_global,
            )
            execute_batch(
                cur,
                "INSERT INTO customer_segments(user_id,segment,lifetime_value) VALUES (%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET segment=EXCLUDED.segment, lifetime_value=EXCLUDED.lifetime_value",
                seg_rows,
            )
            execute_batch(
                cur,
                "INSERT INTO product_suppliers(product_id,supplier_id,cost_usd,lead_time_days) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                prod_supp,
            )
            execute_batch(
                cur,
                "INSERT INTO warehouse_inventory(warehouse_id,product_id,qty,reserved_qty) VALUES (%s,%s,%s,%s) ON CONFLICT (warehouse_id,product_id) DO UPDATE SET qty=EXCLUDED.qty, reserved_qty=EXCLUDED.reserved_qty",
                inv_by_wh,
            )
        conn.commit()
    finally:
        conn.autocommit = prev_autocommit
    print(
        f"✅ Seeded: {len(users)} users, {len(products)} products, {len(warehouses)} warehouses, {len(suppliers)} suppliers"
    )
