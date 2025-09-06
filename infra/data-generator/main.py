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
RECENT_SESSIONS   = deque(maxlen=200)   # smaller to force rotation

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
    """Create+seed reference data; avoids ORDER BY random() hot paths by preloading caches."""
    with open("ddl.sql", "r", encoding="utf-8") as f:
        with conn.cursor() as cur:
            cur.execute(f.read())

    warehouses = []
    for i in range(SEED_WAREHOUSES):
        country = random.choice(COUNTRIES)
        wid = rid("wh", 6)
        name = f"Warehouse {chr(65+i)} - {country}"
        region = "EMEA" if country in ["DE","GB","FR","ES","IT","NL"] else ("Americas" if country == "US" else "APAC")
        warehouses.append((wid, name, country, region))
        RECENT_WAREHOUSES.append(wid)

    suppliers = []
    for _ in range(SEED_SUPPLIERS):
        sid = rid("sup", 6)
        name = f"Supplier {rid('', 4).upper()}"
        country = random.choice(COUNTRIES)
        rating = round(random.uniform(3.0, 5.0), 2)
        suppliers.append((sid, name, country, rating))
        RECENT_SUPPLIERS.append(sid)

    users = []
    seg_rows = []
    for _ in range(SEED_USERS):
        uid = rid("u", 8)
        email = f"{uid}@example.com"
        country = random.choice(COUNTRIES)
        users.append((uid, email, country))
        RECENT_USERS.append(uid)
        segment = random.choices(CUSTOMER_SEGMENTS, weights=[1,6,2,1])[0]
        ltv = round(random.uniform(50, 2000), 2) if segment == "VIP" else round(random.uniform(10, 500), 2)
        seg_rows.append((uid, segment, ltv))

    products = []
    inv_global = []
    inv_by_wh = []
    prod_supp = []

    for _ in range(SEED_PRODUCTS):
        pid = rid("p", 8)
        title = " ".join(random.sample(["Ultra","Pro","Max","Nano","Air","Lite","Edge","Prime","Neo","Pulse","Core","Elite"], k=2))
        category = random.choice(CATEGORIES)
        price = round(random.uniform(5.0, 900.0), 2)
        products.append((pid, title, category, price))
        total_qty = random.randint(50, 1000)
        inv_global.append((pid, total_qty))
        RECENT_PRODUCTS.append(pid)
        for sid, *_ in random.sample(suppliers, k=random.randint(1, min(3, len(suppliers)))):
            cost = round(price * random.uniform(0.3, 0.7), 2)
            lead = random.randint(3, 21)
            prod_supp.append((pid, sid, cost, lead))
        remain = total_qty
        for wid in list(RECENT_WAREHOUSES)[:-1]:
            if remain <= 0:
                break
            alloc = random.randint(0, max(0, total_qty // 3))
            reserv = random.randint(0, alloc // 4)
            inv_by_wh.append((wid, pid, alloc, reserv))
            remain -= alloc
        if remain > 0 and RECENT_WAREHOUSES:
            last = list(RECENT_WAREHOUSES)[-1]
            reserv = random.randint(0, remain // 4)
            inv_by_wh.append((last, pid, remain, reserv))

    with conn.cursor() as cur:
        execute_batch(cur, "INSERT INTO warehouses(warehouse_id,name,country,region) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", warehouses)
        execute_batch(cur, "INSERT INTO suppliers(supplier_id,name,country,rating) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", suppliers)
        execute_batch(cur, "INSERT INTO users(user_id,email,country) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", users)
        execute_batch(cur, "INSERT INTO products(product_id,title,category,price_usd) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", products)
        execute_batch(cur, "INSERT INTO inventory(product_id,qty) VALUES (%s,%s) ON CONFLICT (product_id) DO UPDATE SET qty=EXCLUDED.qty", inv_global)
        execute_batch(cur, "INSERT INTO customer_segments(user_id,segment,lifetime_value) VALUES (%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET segment=EXCLUDED.segment, lifetime_value=EXCLUDED.lifetime_value", seg_rows)
        execute_batch(cur, "INSERT INTO product_suppliers(product_id,supplier_id,cost_usd,lead_time_days) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", prod_supp)
        execute_batch(cur, "INSERT INTO warehouse_inventory(warehouse_id,product_id,qty,reserved_qty) VALUES (%s,%s,%s,%s) ON CONFLICT (warehouse_id,product_id) DO UPDATE SET qty=EXCLUDED.qty, reserved_qty=EXCLUDED.reserved_qty", inv_by_wh)


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
    payments_schema = '{"type":"record","name":"Payment","namespace":"demo","fields":[{"name":"payment_id","type":"string"},{"name":"order_id","type":"string"},{"name":"method","type":{"type":"enum","name":"Method","symbols":["CARD","APPLE_PAY","PAYPAL"]}},{"name":"status","type":{"type":"enum","name":"Status","symbols":["PENDING","SETTLED","FAILED"]}},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
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


def emit_inventory_change(producer, invchg_ser, conn):
    ts = maybe_late(now_ms())
    wid = pick_warehouse()
    pid = pick_product()
    ctype = random.choice(INVENTORY_CHANGE_TYPES)

    # If Postgres is canonical, read current state to craft a realistic change.
    # If Kafka is canonical, synthesize from small random walk; DB update optional.
    if CANON_INVENTORY == "postgres":
        with conn.cursor() as cur:
            cur.execute("SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s", (wid, pid))
            row = cur.fetchone()
            prev, reserved = (row or (random.randint(0, 100), 0))
    else:
        prev, reserved = random.randint(0, 100), 0

    if ctype == "RESTOCK":
        delta = random.randint(50, 200)
    elif ctype == "SALE":
        delta = -random.randint(1, min(10, max(1, prev)))
    elif ctype == "DAMAGE":
        delta = -random.randint(1, min(5, max(1, prev)))
    elif ctype == "RETURN":
        delta = random.randint(1, 5)
    else:
        delta = random.randint(-10, 10)

    if delta < 0:
        use_res = min(reserved, -delta)
        reserved -= use_res
        delta += use_res
    new_qty = max(0, prev + delta)

    change = {
        "change_id": rid("chg"),
        "warehouse_id": wid,
        "product_id": pid,
        "change_type": ctype,
        "quantity_delta": delta,
        "previous_qty": prev,
        "new_qty": new_qty,
        "reason": f"{ctype.lower()}_reason_{rid('',3)}" if ctype in ["DAMAGE","ADJUSTMENT"] else None,
        "order_id": random.choice(list(RECENT_ORDERS)) if ctype == "SALE" and RECENT_ORDERS else None,
        "supplier_id": pick_supplier() if ctype == "RESTOCK" else None,
        "cost_per_unit": round(random.uniform(10.0, 200.0), 2) if ctype == "RESTOCK" else None,
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

    # If Postgres is canonical, read current state to craft a realistic change.
    # If Kafka is canonical, synthesize from small random walk; DB update optional.
    if CANON_INVENTORY == "postgres":
        with conn.cursor() as cur:
            cur.execute("SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s", (wid, pid))
            row = cur.fetchone()
            prev, reserved = (row or (random.randint(0, 100), 0))
    else:
        prev, reserved = random.randint(0, 100), 0

    if ctype == "RESTOCK":
        delta = random.randint(50, 200)
    elif ctype == "SALE":
        delta = -random.randint(1, min(10, max(1, prev)))
    elif ctype == "DAMAGE":
        delta = -random.randint(1, min(5, max(1, prev)))
    elif ctype == "RETURN":
        delta = random.randint(1, 5)
    else:
        delta = random.randint(-10, 10)

    # Apply reserved-first policy for negatives
    if delta < 0:
        use_res = min(reserved, -delta)
        reserved -= use_res
        delta += use_res
    new_qty = max(0, prev + delta)

    change = {
        "change_id": rid("chg"),
        "warehouse_id": wid,
        "product_id": pid,
        "change_type": ctype,
        "quantity_delta": delta,
        "previous_qty": prev,
        "new_qty": new_qty,
        "reason": f"{ctype.lower()}_reason_{rid('',3)}" if ctype in ["DAMAGE","ADJUSTMENT"] else None,
        "order_id": random.choice(list(RECENT_ORDERS)) if ctype == "SALE" and RECENT_ORDERS else None,
        "supplier_id": pick_supplier() if ctype == "RESTOCK" else None,
        "cost_per_unit": round(random.uniform(10.0, 200.0), 2) if ctype == "RESTOCK" else None,
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

    # Mirror to DB only if (a) Postgres is canonical, or (b) mirroring explicitly enabled
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
    wid = pick_warehouse()
    pid = pick_product()
    ctype = random.choice(INVENTORY_CHANGE_TYPES)
    with conn.cursor() as cur:
        cur.execute("SELECT qty, reserved_qty FROM warehouse_inventory WHERE warehouse_id=%s AND product_id=%s", (wid, pid))
        row = cur.fetchone()
        prev, reserved = (row or (random.randint(0, 100), 0))
    if ctype == "RESTOCK":
        delta = random.randint(50, 200)
    elif ctype == "SALE":
        delta = -random.randint(1, min(10, max(1, prev)))
    elif ctype == "DAMAGE":
        delta = -random.randint(1, min(5, max(1, prev)))
    elif ctype == "RETURN":
        delta = random.randint(1, 5)
    else:
        delta = random.randint(-10, 10)
    if delta < 0:
        use_res = min(reserved, -delta)
        reserved -= use_res
        delta += use_res
    new_qty = max(0, prev + delta)

    change = {
        "change_id": rid("chg"),
        "warehouse_id": wid,
        "product_id": pid,
        "change_type": ctype,
        "quantity_delta": delta,
        "previous_qty": prev,
        "new_qty": new_qty,
        "reason": f"{ctype.lower()}_reason_{rid('',3)}" if ctype in ["DAMAGE","ADJUSTMENT"] else None,
        "order_id": random.choice(list(RECENT_ORDERS)) if ctype == "SALE" and RECENT_ORDERS else None,
        "supplier_id": pick_supplier() if ctype == "RESTOCK" else None,
        "cost_per_unit": round(random.uniform(10.0, 200.0), 2) if ctype == "RESTOCK" else None,
        "ts": ts
    }
    send(producer, TOPIC_INVENTORY_CHANGES, key=f"{wid}:{pid}", value=change, serializer=invchg_ser,
         headers=[("entity","inventory_change"), ("source","fakegen")])
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO warehouse_inventory (warehouse_id, product_id, qty, reserved_qty)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (warehouse_id, product_id)
            DO UPDATE SET qty = %s, reserved_qty = %s, updated_at = now()
        """, (wid, pid, new_qty, reserved, new_qty, reserved))


def emit_order_payment_shipment(producer, orders_ser, payments_ser, shipments_ser):
    base_ts = now_ms()
    order_ts = maybe_late(base_ts)
    order_id = rid("ord")
    user_id = pick_user()
    product_id = pick_product()
    amount = round(random.uniform(5.0, 900.0), 2)
    currency = random.choice(CURRENCIES)
    trace_id = rid("tr")

    order = sprinkle_bad_record({
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "amount": amount,
        "currency": currency,
        "ts": order_ts,
    })
    send(producer, TOPIC_ORDERS, key=order_id, value=order, serializer=orders_ser,
         headers=[("trace_id", trace_id), ("entity","order"), ("source","fakegen")])
    RECENT_ORDERS.append(order_id)

    if random.random() < P_ORDER_HAS_PAYMENT:
        pay_ts = maybe_late(base_ts + random.randint(10, 2000))
        pay = sprinkle_bad_record({
            "payment_id": rid("pay"),
            "order_id": order_id,
            "method": random.choice(PAYMENT_METHODS),
            "status": random.choices(PAYMENT_STATUS, weights=[2,5,1])[0],
            "ts": pay_ts,
        })
        send(producer, TOPIC_PAYMENTS, key=pay["payment_id"], value=pay, serializer=payments_ser,
             headers=[("trace_id", trace_id), ("entity","payment"), ("source","fakegen")])

    if random.random() < P_ORDER_HAS_SHIPMENT:
        shp_ts = maybe_late(base_ts + random.randint(5000, 60000))
        shp = sprinkle_bad_record({
            "shipment_id": rid("shp"),
            "order_id": order_id,
            "carrier": random.choice(CARRIERS),
            "eta_days": random.choice([1,2,3,4,5]),
            "ts": shp_ts,
        })
        send(producer, TOPIC_SHIPMENTS, key=shp["shipment_id"], value=shp, serializer=shipments_ser,
             headers=[("trace_id", trace_id), ("entity","shipment"), ("source","fakegen")])

# -----------------------
# Main loop
# -----------------------

running = True

def _sig(*_):
    global running
    running = False

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
