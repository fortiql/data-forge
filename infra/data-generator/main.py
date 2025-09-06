"""
Data Generator for Data Forge Analytics Stack

Generates realistic retail data split between Postgres OLTP tables and Kafka event streams.
Creates multi-dimensional e-commerce scenarios with 8 countries, 5 warehouses, 20 suppliers.

Architecture:
- Postgres: Reference data (users, products, warehouses, suppliers) + analytical tables
- Kafka: Real-time events (orders, payments, shipments, inventory, interactions)
- Schema Registry: Avro serialization with fallback to embedded schemas

Business Logic:
- Order flow: Order → Payment (70%) → Shipment (60%)
- Customer segments: VIP/Regular/New/Churned based on lifetime value  
- Inventory events: RESTOCK/SALE/DAMAGE/RETURN/TRANSFER/ADJUSTMENT
- Geographic distribution: Multi-region operations with realistic patterns

Performance Features:
- Token bucket rate limiting with diurnal traffic curves
- LRU caches for hot data access (90% recent users/products)
- Batch inserts for seeding, streaming for events
- Backpressure-safe Kafka production with delivery callbacks

Data Quality:
- Referential integrity maintained across all tables
- Late-arriving events for realistic analytics complexity
- Trace IDs for event correlation
- Bad records sprinkled for robust data pipeline testing
"""

import os, time, random, string, signal, sys, json, math
from time import monotonic, sleep
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

import psycopg2
from psycopg2.extras import execute_batch

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.admin import AdminClient, NewTopic

try:
    from confluent_kafka.schema_registry.avro import AvroSerializer
except Exception:
    AvroSerializer = None

# -----------------------
# Config via environment
# -----------------------
BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC_ORDERS    = os.getenv("TOPIC_ORDERS", "orders.v1")
TOPIC_PAYMENTS  = os.getenv("TOPIC_PAYMENTS", "payments.v1")
TOPIC_SHIPMENTS = os.getenv("TOPIC_SHIPMENTS", "shipments.v1")
TOPIC_INVENTORY_CHANGES = os.getenv("TOPIC_INVENTORY_CHANGES", "inventory-changes.v1")
TOPIC_CUSTOMER_INTERACTIONS = os.getenv("TOPIC_CUSTOMER_INTERACTIONS", "customer-interactions.v1")

PG_DSN = os.getenv("PG_DSN", "host=postgres port=5432 dbname=demo user=admin password=admin")

# Canonical source controls (prevent mutual exclusivity):
# - If inventory is canonical in Postgres, we mutate the DB and let Debezium stream changes.
# - If inventory is canonical in Kafka, we emit InventoryChange events only; DB mirroring is optional.
CANON_INVENTORY = os.getenv("CANON_INVENTORY", "postgres")  # one of: "postgres", "kafka"
MIRROR_INVENTORY_TO_DB = os.getenv("MIRROR_INVENTORY_TO_DB", "false").lower() == "true"

# Honest total event rate per second (across topics); distributed by weights
TARGET_EPS = float(os.getenv("TARGET_EPS", "120"))
WEIGHT_ORDERS = float(os.getenv("WEIGHT_ORDERS", "0.35"))
WEIGHT_INTERACTIONS = float(os.getenv("WEIGHT_INTERACTIONS", "0.45"))
WEIGHT_INVENTORY_CHG = float(os.getenv("WEIGHT_INVENTORY_CHG", "0.20"))

SEED_USERS = int(os.getenv("SEED_USERS", "500"))
SEED_PRODUCTS = int(os.getenv("SEED_PRODUCTS", "200"))
SEED_WAREHOUSES = int(os.getenv("SEED_WAREHOUSES", "5"))
SEED_SUPPLIERS = int(os.getenv("SEED_SUPPLIERS", "20"))

# Probabilities inside an emit (per-order companions etc.)
P_ORDER_HAS_PAYMENT  = float(os.getenv("P_ORDER_HAS_PAYMENT", "0.7"))
P_ORDER_HAS_SHIPMENT = float(os.getenv("P_ORDER_HAS_SHIPMENT", "0.6"))

# Random mutation dials (CDC)
P_UPDATE_INVENTORY = float(os.getenv("P_UPDATE_INVENTORY", "0.12"))
P_UPDATE_PRICE     = float(os.getenv("P_UPDATE_PRICE", "0.04"))

# Late/out-of-order + bad records
P_LATE_EVENT       = float(os.getenv("P_LATE_EVENT", "0.05"))   # 5% late by minutes
MAX_LATE_MINUTES   = int(os.getenv("MAX_LATE_MINUTES", "25"))
P_BAD_RECORD       = float(os.getenv("P_BAD_RECORD", "0.01"))   # 1% minor schema drift/nulls

# Enums for realistic data
PAYMENT_METHODS = ["CARD", "APPLE_PAY", "PAYPAL"]
PAYMENT_STATUS  = ["PENDING", "SETTLED", "FAILED"]
CARRIERS        = ["UPS", "DHL", "FEDEX"]
CURRENCIES      = ["USD", "EUR", "GBP"]
COUNTRIES       = ["US","DE","GB","FR","IL","ES","IT","NL"]
CATEGORIES      = ["Electronics","Home","Sports","Fashion","Books","Beauty"]
CUSTOMER_SEGMENTS = ["VIP", "Regular", "New", "Churned"]
INTERACTION_TYPES = ["PAGE_VIEW", "SEARCH", "CART_ADD", "CART_REMOVE", "WISHLIST_ADD", "REVIEW", "SUPPORT_CHAT"]
INVENTORY_CHANGE_TYPES = ["RESTOCK", "SALE", "DAMAGE", "RETURN", "TRANSFER", "ADJUSTMENT"]

# LRU caches for realistic joins
RECENT_USERS      = deque(maxlen=1000)
RECENT_PRODUCTS   = deque(maxlen=1000)
RECENT_ORDERS     = deque(maxlen=3000)
RECENT_WAREHOUSES = deque(maxlen=50)
RECENT_SUPPLIERS  = deque(maxlen=100)
RECENT_SESSIONS   = deque(maxlen=200)

# -----------------------
# Utilities
# -----------------------

def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def rid(prefix, n=10):
    return prefix + "_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def generate_ip():
    return f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"


def generate_user_agent():
    browsers = ["Chrome/120.0", "Firefox/121.0", "Safari/17.0", "Edge/120.0"]
    os_list = ["Windows NT 10.0", "Macintosh; Intel Mac OS X 10_15_7", "X11; Linux x86_64"]
    return f"Mozilla/5.0 ({random.choice(os_list)}) {random.choice(browsers)}"


def diurnal_multiplier() -> float:
    """Return 0.4..1.4 multiplier based on local hour-of-day to mimic traffic curve."""
    hour = datetime.now().hour
    # Shift sine so min≈0.4, max≈1.4
    return 0.9 + 0.5 * math.sin((hour - 3) / 24 * 2 * math.pi)


def maybe_late(base_ms: int) -> int:
    if random.random() < P_LATE_EVENT:
        delay = random.randint(1, MAX_LATE_MINUTES * 60_000)
        return base_ms - delay
    return base_ms

# -----------------------
# Readiness helpers
# -----------------------

def wait_for_pg(max_wait_s=60):
    start = monotonic()
    while True:
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.close()
            return
        except Exception as e:
            if monotonic() - start > max_wait_s:
                raise
            sleep(1)


def wait_for_sr(max_wait_s=60):
    start = monotonic()
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    while True:
        try:
            # a cheap call: list subjects (may be empty)
            client.get_subjects()
            return client
        except Exception:
            if monotonic() - start > max_wait_s:
                raise
            sleep(1)

# -----------------------
# Postgres helpers
# -----------------------

def pg_connect():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True  # tiny updates benefit from autocommit
    return conn


def seed_postgres(conn):
    """
    Seed Postgres with reference data for analytics.
    Creates realistic e-commerce foundation: users, products, warehouses, suppliers.
    Uses a single transaction + ON CONFLICT for idempotency.
    """
    print("🌱 Seeding Postgres reference data...")
    warehouses = []
    for i in range(SEED_WAREHOUSES):
        country = random.choice(COUNTRIES)
        region = "North America" if country in {"US", "CA"} else "Europe"
        warehouse_id = f"WH{i+1:03d}"
        warehouses.append((warehouse_id, f"Warehouse {i+1}", country, region))
        try:
            RECENT_WAREHOUSES.append(warehouse_id)
        except NameError:
            pass

    suppliers = []
    for i in range(SEED_SUPPLIERS):
        country = random.choice(COUNTRIES)
        rating = round(random.uniform(3.0, 5.0), 1)
        supplier_id = f"SUP{i+1:03d}"
        suppliers.append((supplier_id, f"Supplier {i+1}", country, rating))
        try:
            RECENT_SUPPLIERS.append(supplier_id)
        except NameError:
            pass

    users = []
    for i in range(SEED_USERS):
        country = random.choice(COUNTRIES)
        email = f"user{i+1}@example.com"
        user_id = f"U{i+1:06d}"
        users.append((user_id, email, country))
        if i >= int(SEED_USERS * 0.1):
            try:
                RECENT_USERS.append(user_id)
            except NameError:
                pass
    products = []
    for i in range(SEED_PRODUCTS):
        category = random.choice(CATEGORIES)
        price = round(random.uniform(10.0, 500.0), 2)
        product_id = f"P{i+1:06d}"
        products.append((product_id, f"Product {i+1}", category, price))
        if i >= int(SEED_PRODUCTS * 0.1):
            try:
                RECENT_PRODUCTS.append(product_id)  # noqa: F821
            except NameError:
                pass

    # --- Legacy/global inventory snapshot ---
    inv_global = [(pid, random.randint(0, 1000)) for pid, _, _, _ in products]

    # --- Customer segments ---
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

    # --- Product ↔ Supplier relations ---
    prod_supp = []
    for pid, _, _, price in products:
        num_suppliers = random.randint(1, 3)
        chosen = random.sample(suppliers, min(num_suppliers, len(suppliers)))
        for sid, *_ in chosen:
            cost = round(float(price) * random.uniform(0.3, 0.7), 2)
            lead_time = random.randint(3, 21)
            prod_supp.append((pid, sid, cost, lead_time))

    # --- Warehouse inventory (only WH### IDs from `warehouses`) ---
    inv_by_wh = []
    for wid, _, _, _ in warehouses:
        for pid, _, _, _ in products:
            if random.random() < 0.8:
                qty = random.randint(10, 500)
                reserved = random.randint(0, min(qty, 50))
                inv_by_wh.append((wid, pid, qty, reserved))

    # --- Single transaction for speed & FK safety ---
    prev_autocommit = getattr(conn, "autocommit", False)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Parents first
            execute_batch(cur,
                "INSERT INTO warehouses(warehouse_id,name,country,region) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                warehouses)
            execute_batch(cur,
                "INSERT INTO suppliers(supplier_id,name,country,rating) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                suppliers)
            execute_batch(cur,
                "INSERT INTO users(user_id,email,country) "
                "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                users)
            execute_batch(cur,
                "INSERT INTO products(product_id,title,category,price_usd) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                products)

            # Then dependents
            execute_batch(cur,
                "INSERT INTO inventory(product_id,qty) "
                "VALUES (%s,%s) "
                "ON CONFLICT (product_id) DO UPDATE SET qty=EXCLUDED.qty",
                inv_global)
            execute_batch(cur,
                "INSERT INTO customer_segments(user_id,segment,lifetime_value) "
                "VALUES (%s,%s,%s) "
                "ON CONFLICT (user_id) DO UPDATE SET segment=EXCLUDED.segment, lifetime_value=EXCLUDED.lifetime_value",
                seg_rows)
            execute_batch(cur,
                "INSERT INTO product_suppliers(product_id,supplier_id,cost_usd,lead_time_days) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                prod_supp)
            execute_batch(cur,
                "INSERT INTO warehouse_inventory(warehouse_id,product_id,qty,reserved_qty) "
                "VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (warehouse_id,product_id) DO UPDATE "
                "SET qty=EXCLUDED.qty, reserved_qty=EXCLUDED.reserved_qty",
                inv_by_wh)

        conn.commit()
    finally:
        conn.autocommit = prev_autocommit

    print(f"✅ Seeded: {len(users)} users, {len(products)} products, {len(warehouses)} warehouses, {len(suppliers)} suppliers")



# -----------------------
# Kafka setup
# -----------------------

def fetch_serializer(sr_client, subject: str, embedded_schema: str):
    """Fetch latest schema for subject, else fall back to embedded string."""
    try:
        meta = sr_client.get_latest_version(subject)
        return AvroSerializer(sr_client, meta.schema.schema_str)
    except Exception:
        return AvroSerializer(sr_client, embedded_schema)


def setup_kafka():
    print("[fakegen] waiting for Schema Registry…", flush=True)
    sr_client = wait_for_sr()
    if AvroSerializer is None:
        raise RuntimeError("confluent_kafka.schema_registry.avro.AvroSerializer not available; install confluent-kafka[avro]")

    orders_schema = '{"type":"record","name":"Order","namespace":"demo","fields":[{"name":"order_id","type":"string"},{"name":"user_id","type":"string"},{"name":"product_id","type":"string"},{"name":"amount","type":"double"},{"name":"currency","type":"string","default":"USD"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    payments_schema = '{"type":"record","name":"Payment","namespace":"demo","fields":[{"name":"payment_id","type":"string"},{"name":"order_id","type":"string"},{"name":"method","type":{"type":"enum","name":"Method","symbols":["CARD","APPLE_PAY","PAYPAL"]}},{"name":"status","type":{"type":"enum","name":"Status","symbols":["PENDING","SETTLED","FAILED","UNKNOWN"]}},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    shipments_schema = '{"type":"record","name":"Shipment","namespace":"demo","fields":[{"name":"shipment_id","type":"string"},{"name":"order_id","type":"string"},{"name":"carrier","type":"string"},{"name":"eta_days","type":"int","default":3},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    invchg_schema = '{"type":"record","name":"InventoryChange","namespace":"demo","fields":[{"name":"change_id","type":"string"},{"name":"warehouse_id","type":"string"},{"name":"product_id","type":"string"},{"name":"change_type","type":{"type":"enum","name":"ChangeType","symbols":["RESTOCK","SALE","DAMAGE","RETURN","TRANSFER","ADJUSTMENT"]}},{"name":"quantity_delta","type":"int"},{"name":"previous_qty","type":"int"},{"name":"new_qty","type":"int"},{"name":"reason","type":["null","string"],"default":null},{"name":"order_id","type":["null","string"],"default":null},{"name":"supplier_id","type":["null","string"],"default":null},{"name":"cost_per_unit","type":["null","double"],"default":null},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    interactions_schema = '{"type":"record","name":"CustomerInteraction","namespace":"demo","fields":[{"name":"interaction_id","type":"string"},{"name":"user_id","type":"string"},{"name":"session_id","type":"string"},{"name":"interaction_type","type":{"type":"enum","name":"InteractionType","symbols":["PAGE_VIEW","SEARCH","CART_ADD","CART_REMOVE","WISHLIST_ADD","REVIEW","SUPPORT_CHAT"]}},{"name":"product_id","type":["null","string"],"default":null},{"name":"search_query","type":["null","string"],"default":null},{"name":"page_url","type":["null","string"],"default":null},{"name":"duration_ms","type":["null","long"],"default":null},{"name":"user_agent","type":"string"},{"name":"ip_address","type":"string"},{"name":"country","type":"string"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'

    producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 25,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "key.serializer": StringSerializer("utf_8"),
        "queue.buffering.max.messages": 200000,
    })

    orders_ser = fetch_serializer(sr_client, f"{TOPIC_ORDERS}-value", orders_schema)
    payments_ser = fetch_serializer(sr_client, f"{TOPIC_PAYMENTS}-value", payments_schema)
    shipments_ser = fetch_serializer(sr_client, f"{TOPIC_SHIPMENTS}-value", shipments_schema)
    invchg_ser   = fetch_serializer(sr_client, f"{TOPIC_INVENTORY_CHANGES}-value", invchg_schema)
    inter_ser    = fetch_serializer(sr_client, f"{TOPIC_CUSTOMER_INTERACTIONS}-value", interactions_schema)

    return producer, orders_ser, payments_ser, shipments_ser, invchg_ser, inter_ser

# -----------------------
# Backpressure-safe produce
# -----------------------

def send(producer, topic, key, value, serializer, headers=None):
    while True:
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=serializer(value, SerializationContext(topic, MessageField.VALUE)),
                headers=headers or [],
                on_delivery=lambda err, msg: print(f"[deliver-err] {err}") if err else None,
            )
            break
        except BufferError:
            producer.poll(0.05)  # drain queue on backpressure

# -----------------------
# Generators
# -----------------------

def get_or_create_session(user_id):
    # 40% chance of new session to keep spread
    if random.random() < 0.4 or not RECENT_SESSIONS:
        sid = rid("sess", 12)
        RECENT_SESSIONS.append((user_id, sid))
        return sid
    options = [s for u, s in RECENT_SESSIONS if u == user_id]
    if options:
        return random.choice(options)
    sid = rid("sess", 12)
    RECENT_SESSIONS.append((user_id, sid))
    return sid


def pick_user():
    if RECENT_USERS and random.random() < 0.95:
        return random.choice(list(RECENT_USERS))
    uid = rid("u", 8)
    RECENT_USERS.append(uid)
    return uid


def preload_products(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT product_id FROM products LIMIT %s", (SEED_PRODUCTS,))
        for (pid,) in cur.fetchall():
            RECENT_PRODUCTS.append(pid)


def pick_product():
    if RECENT_PRODUCTS:
        return random.choice(list(RECENT_PRODUCTS))
    return rid("p", 8)


def pick_warehouse():
    return random.choice(list(RECENT_WAREHOUSES)) if RECENT_WAREHOUSES else rid("wh", 6)


def pick_supplier():
    return random.choice(list(RECENT_SUPPLIERS)) if RECENT_SUPPLIERS else rid("sup", 6)


def maybe_update_inventory(conn):
    """Only mutate DB inventory if Postgres is the canonical source."""
    if CANON_INVENTORY != "postgres":
        return
    if random.random() < P_UPDATE_INVENTORY and RECENT_PRODUCTS and RECENT_WAREHOUSES:
        pid = pick_product()
        wid = pick_warehouse()
        with conn.cursor() as cur:
            cur.execute("SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s", (wid, pid))
            row = cur.fetchone()
            qty, reserved = (row or (random.randint(0, 100), 0))
        delta = random.randint(-5, 20)
        if delta < 0:
            use_res = min(reserved, -delta)
            reserved -= use_res
            delta += use_res
        new_qty = max(0, qty + delta)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO warehouse_inventory(warehouse_id, product_id, qty, reserved_qty)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (warehouse_id,product_id)
                DO UPDATE SET qty=EXCLUDED.qty, reserved_qty=EXCLUDED.reserved_qty, updated_at=now()
                """,
                (wid, pid, new_qty, reserved),
            )


def maybe_update_price(conn):
    if random.random() < P_UPDATE_PRICE and RECENT_PRODUCTS:
        pid = pick_product()
        factor = random.choice([0.95, 0.97, 1.03, 1.05])
        with conn.cursor() as cur:
            cur.execute("UPDATE products SET price_usd = ROUND(price_usd * %s, 2), updated_at = now() WHERE product_id = %s", (factor, pid))


def sprinkle_bad_record(obj: dict):
    if random.random() < P_BAD_RECORD:
        choice = random.choice(["drop_amount", "weird_enum", "null_product"]) 
        if choice == "drop_amount" and "amount" in obj:
            obj["amount"] = None
        elif choice == "weird_enum" and "status" in obj:
            obj["status"] = "UNKNOWN"
        elif choice == "null_product" and "product_id" in obj:
            obj["product_id"] = None
    return obj


def emit_customer_interaction(producer, inter_ser):
    ts = maybe_late(now_ms())
    user_id = pick_user()
    session_id = get_or_create_session(user_id)
    itype = random.choice(INTERACTION_TYPES)
    interaction = {
        "interaction_id": rid("int"),
        "user_id": user_id,
        "session_id": session_id,
        "interaction_type": itype,
        "product_id": pick_product() if itype in ["PAGE_VIEW","CART_ADD","CART_REMOVE","WISHLIST_ADD","REVIEW"] else None,
        "search_query": f"search_{rid('', 4)}" if itype == "SEARCH" else None,
        "page_url": f"/products/{rid('', 6)}" if itype == "PAGE_VIEW" else None,
        "duration_ms": random.randint(1000, 300000) if itype == "PAGE_VIEW" else None,
        "user_agent": generate_user_agent(),
        "ip_address": generate_ip(),
        "country": random.choice(COUNTRIES),
        "ts": ts
    }
    send(producer, TOPIC_CUSTOMER_INTERACTIONS, key=interaction["session_id"], value=interaction, serializer=inter_ser,
         headers=[("entity","customer_interaction"), ("source","fakegen")])


def _read_inventory_state(conn, warehouse_id: str, product_id: str):
    """Return (qty, reserved) for a (warehouse, product). If missing, synthesize a sane default."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s",
            (warehouse_id, product_id),
        )
        row = cur.fetchone()
    return (row or (random.randint(0, 100), 0))

def _delta_reason_sup(change_type: str, prev: int):
    """Compute delta and auxiliary fields for a given change type."""
    if change_type == "RESTOCK":
        delta = random.randint(50, 200)
        return delta, "Supplier delivery", pick_supplier(), round(random.uniform(10.0, 200.0), 2)
    if change_type == "SALE":
        delta = -random.randint(1, min(10, max(1, prev)))
        return delta, "Customer order fulfillment", None, None
    if change_type == "DAMAGE":
        delta = -random.randint(1, min(5, max(1, prev)))
        return delta, "Warehouse damage", None, None
    if change_type == "RETURN":
        delta = random.randint(1, 5)
        return delta, "Customer return", None, None
    if change_type == "TRANSFER":
        delta = -random.randint(1, min(20, max(1, prev))) if prev > 0 else 0
        return delta, f"Transfer to {pick_warehouse()}", None, None
    delta = random.randint(-10, 10)
    return delta, "Inventory audit", None, None

def _apply_reserved_first(delta: int, reserved: int):
    """Consume reserved stock first when applying negative deltas."""
    if delta < 0:
        use_res = min(reserved, -delta)
        reserved -= use_res
        delta += use_res
    return delta, reserved

def emit_inventory_change(producer, invchg_ser, conn):
    """Emit exactly one inventory-change event and optionally mirror to DB.
    """
    ts = maybe_late(now_ms())
    wid = pick_warehouse()
    pid = pick_product()
    ctype = random.choice(INVENTORY_CHANGE_TYPES)
    if CANON_INVENTORY == "postgres":
        prev, reserved = _read_inventory_state(conn, wid, pid)
    else:
        prev, reserved = random.randint(0, 100), 0
    delta, reason, supplier_id, cost_per_unit = _delta_reason_sup(ctype, prev)
    delta, reserved = _apply_reserved_first(delta, reserved)

    new_qty = max(0, prev + delta)

    change = {
        "change_id": rid("chg"),
        "warehouse_id": wid,
        "product_id": pid,
        "change_type": ctype,
        "quantity_delta": delta,
        "previous_qty": prev,
        "new_qty": new_qty,
        "reason": reason if ctype in ["DAMAGE", "ADJUSTMENT", "TRANSFER", "RETURN", "RESTOCK", "SALE"] else None,
        "order_id": random.choice(list(RECENT_ORDERS)) if ctype == "SALE" and RECENT_ORDERS else None,
        "supplier_id": supplier_id,
        "cost_per_unit": cost_per_unit,
        "ts": ts,
    }

    send(
        producer,
        TOPIC_INVENTORY_CHANGES,
        key=f"{wid}:{pid}",
        value=change,
        serializer=invchg_ser,
        headers=[("entity", "inventory_change"), ("source", "fakegen")],
    )
    if CANON_INVENTORY == "postgres" or MIRROR_INVENTORY_TO_DB:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO warehouse_inventory (warehouse_id, product_id, qty, reserved_qty)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (warehouse_id, product_id)
                DO UPDATE SET qty = %s, reserved_qty = %s, updated_at = now()
                """,
                (wid, pid, new_qty, reserved, new_qty, reserved),
            )



def emit_order_payment_shipment(producer, orders_ser, payments_ser, shipments_ser):
    """
    Emit correlated Order -> Payment -> Shipment events.
    """

    def _clamp_enum(v, allowed, default):
        return v if v in allowed else default

    def _ensure_str(v, fallback):
        return v if isinstance(v, str) and v != "" else fallback

    def _ensure_float(v, fallback):
        try:
            return float(v)
        except Exception:
            return float(fallback)

    def _ensure_int(v, fallback):
        try:
            return int(v)
        except Exception:
            return int(fallback)

    base_ts   = now_ms()
    order_ts  = maybe_late(base_ts)
    order_id  = rid("ord")
    user_id   = pick_user()
    product_id = pick_product()
    amount    = round(random.uniform(5.0, 900.0), 2)
    currency  = random.choice(CURRENCIES)
    trace_id  = rid("tr")

    if not user_id or not product_id:
        return

    order = {
        "order_id":   order_id,
        "user_id":    user_id,
        "product_id": product_id,
        "amount":     amount,
        "currency":   currency,
        "ts":         order_ts
    }

    order = sprinkle_bad_record(order)
    order["order_id"]   = _ensure_str(order.get("order_id"), order_id)
    order["user_id"]    = _ensure_str(order.get("user_id"), user_id)
    order["product_id"] = _ensure_str(order.get("product_id"), product_id)
    order["amount"]     = _ensure_float(order.get("amount"), amount)
    order["currency"]   = _ensure_str(order.get("currency"), currency)

    send(
        producer, TOPIC_ORDERS, key=order_id, value=order, serializer=orders_ser,
        headers=[("trace_id", trace_id), ("entity", "order"), ("source", "fakegen")]
    )
    RECENT_ORDERS.append(order_id)

    if random.random() < P_ORDER_HAS_PAYMENT:
        pay_ts = maybe_late(base_ts + random.randint(10, 2000))
        pay_status = random.choices(PAYMENT_STATUS, weights=[2, 5, 1])[0]
        pay_method = random.choice(PAYMENT_METHODS)

        pay = {
            "payment_id": rid("pay"),
            "order_id":   order_id,
            "method":     pay_method,
            "status":     pay_status,
            "ts":         pay_ts
        }
        pay = sprinkle_bad_record(pay)

        pay["payment_id"] = _ensure_str(pay.get("payment_id"), rid("pay"))
        pay["order_id"]   = _ensure_str(pay.get("order_id"), order_id)
        pay["method"]     = _clamp_enum(pay.get("method"), PAYMENT_METHODS, pay_method)   # keep enum valid
        pay["status"]     = _clamp_enum(pay.get("status"), PAYMENT_STATUS, pay_status)    # keep enum valid
        pay["ts"]         = _ensure_int(pay.get("ts"), pay_ts)

        send(
            producer, TOPIC_PAYMENTS, key=pay["payment_id"], value=pay, serializer=payments_ser,
            headers=[("trace_id", trace_id), ("entity", "payment"), ("source", "fakegen")]
        )

        if pay["status"] == "SETTLED" and random.random() < P_ORDER_HAS_SHIPMENT:
            shp_ts = maybe_late(base_ts + random.randint(5000, 60000))
            carrier = random.choice(CARRIERS)
            eta     = random.choice([1, 2, 3, 4, 5])

            shp = {
                "shipment_id": rid("shp"),
                "order_id":    order_id,
                "carrier":     carrier,   
                "eta_days":    eta,       
                "ts":          shp_ts
            }
            shp = sprinkle_bad_record(shp)

            shp["shipment_id"] = _ensure_str(shp.get("shipment_id"), rid("shp"))
            shp["order_id"]    = _ensure_str(shp.get("order_id"), order_id)
            shp["carrier"]     = _ensure_str(shp.get("carrier"), carrier)
            shp["eta_days"]    = _ensure_int(shp.get("eta_days"), eta)
            shp["ts"]          = _ensure_int(shp.get("ts"), shp_ts)

            send(
                producer, TOPIC_SHIPMENTS, key=shp["shipment_id"], value=shp, serializer=shipments_ser,
                headers=[("trace_id", trace_id), ("entity", "shipment"), ("source", "fakegen")]
            )

def clear_postgres(conn):
    """Truncate all demo tables for a fresh start."""
    tables = [
        "warehouse_inventory",
        "product_suppliers",
        "customer_segments",
        "inventory",
        "products",
        "users",
        "suppliers",
        "warehouses",
    ]
    with conn.cursor() as cur:
        for tbl in tables:
            cur.execute(f"TRUNCATE TABLE {tbl} CASCADE")
    conn.commit()
    print("🧹 Postgres tables truncated")

def clear_kafka():
    """Delete and recreate demo Kafka topics."""
    topics = [
        TOPIC_ORDERS,
        TOPIC_PAYMENTS,
        TOPIC_SHIPMENTS,
        TOPIC_INVENTORY_CHANGES,
        TOPIC_CUSTOMER_INTERACTIONS,
    ]
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    # delete topics
    print("🧹 Deleting Kafka topics...")
    futures = admin.delete_topics(topics, operation_timeout=10)
    for t, f in futures.items():
        try:
            f.result()
            print(f"  ✨ Deleted topic {t}")
        except Exception as e:
            print(f"  ⚠️ Delete failed for {t}: {e}")

    # recreate them (so producers won’t block later)
    print("📝 Re-creating topics...")
    new_topics = [NewTopic(t, num_partitions=3, replication_factor=1) for t in topics]
    futures = admin.create_topics(new_topics)
    for t, f in futures.items():
        try:
            f.result()
            print(f"  ✅ Re-created topic {t}")
        except Exception as e:
            print(f"  ⚠️ Create failed for {t}: {e}")

# -----------------------
# Main loop
# -----------------------

running = True

def _sig(*_):
    global running
    running = False
    print("\n🛑 Shutdown signal received – cleaning up playground...")
    try:
        conn = pg_connect()
        clear_postgres(conn)
        conn.close()
    except Exception as e:
        print(f"⚠️ Postgres cleanup failed: {e}")
    try:
        clear_kafka()
    except Exception as e:
        print(f"⚠️ Kafka cleanup failed: {e}")

signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def main():
    print("[fakegen] waiting for Postgres…", flush=True)
    wait_for_pg()
    conn = pg_connect()
    seed_postgres(conn)
    preload_products(conn)

    print("[fakegen] setting up Kafka…", flush=True)
    producer, orders_ser, payments_ser, shipments_ser, invchg_ser, inter_ser = setup_kafka()

    print(f"[fakegen] target EPS={TARGET_EPS} (orders={WEIGHT_ORDERS}, interactions={WEIGHT_INTERACTIONS}, invchg={WEIGHT_INVENTORY_CHG})", flush=True)

    tokens_total = 0.0
    last = monotonic()

    while running:
        now = monotonic()
        tokens_total += TARGET_EPS * diurnal_multiplier() * (now - last)
        last = now
        maybe_update_inventory(conn)
        maybe_update_price(conn)
        while tokens_total >= 1.0:
            r = random.random()
            if r < WEIGHT_ORDERS:
                emit_order_payment_shipment(producer, orders_ser, payments_ser, shipments_ser)
            elif r < WEIGHT_ORDERS + WEIGHT_INTERACTIONS:
                emit_customer_interaction(producer, inter_ser)
            else:
                emit_inventory_change(producer, invchg_ser, conn)
            tokens_total -= 1.0

        producer.poll(0)
        sleep(0.005)

    print("[fakegen] flushing…", flush=True)
    producer.flush(15)
    conn.close()


if __name__ == "__main__":
    main()
