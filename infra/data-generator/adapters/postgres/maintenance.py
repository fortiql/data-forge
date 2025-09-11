"""Maintenance – SRP: clean demo tables quickly and safely."""

def clear_postgres(conn) -> None:
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
