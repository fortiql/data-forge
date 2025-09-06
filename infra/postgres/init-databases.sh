#!/bin/bash
set -euo pipefail

PGHOST="${POSTGRES_HOST:-localhost}"
PGUSER="${POSTGRES_USER:-postgres}"
PGDB="${POSTGRES_DB:-postgres}"

echo "Checking/creating required databases..."

until pg_isready -U "$PGUSER" -d "$PGDB" -h "$PGHOST" >/dev/null 2>&1; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

if ! psql -U "$PGUSER" -d "$PGDB" -tAc "SELECT 1 FROM pg_database WHERE datname = 'demo'" | grep -q 1; then
  echo "Creating demo database..."
  psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 -c "CREATE DATABASE demo;"
fi

echo "Ensuring demo tables exist..."
psql -U "$PGUSER" -d demo -v ON_ERROR_STOP=1 <<EOSQL
-- ===== Core OLTP tables (match generator canvas) =====

CREATE TABLE IF NOT EXISTS users(
  user_id    TEXT PRIMARY KEY,
  email      TEXT UNIQUE NOT NULL,
  country    TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products(
  product_id TEXT PRIMARY KEY,
  title      TEXT NOT NULL,
  category   TEXT,
  price_usd  NUMERIC(10,2),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Legacy/global inventory snapshot (kept for simple sums)
CREATE TABLE IF NOT EXISTS inventory(
  product_id TEXT PRIMARY KEY REFERENCES products(product_id),
  qty        INT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouses(
  warehouse_id TEXT PRIMARY KEY,
  name         TEXT NOT NULL,
  country      TEXT,
  region       TEXT
);

CREATE TABLE IF NOT EXISTS suppliers(
  supplier_id TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  country     TEXT,
  rating      NUMERIC(3,2)
);

CREATE TABLE IF NOT EXISTS customer_segments(
  user_id         TEXT PRIMARY KEY REFERENCES users(user_id),
  segment         TEXT,
  lifetime_value  NUMERIC(12,2)
);

-- Multi-location stock (authoritative inventory-by-location)
CREATE TABLE IF NOT EXISTS warehouse_inventory(
  warehouse_id TEXT REFERENCES warehouses(warehouse_id),
  product_id   TEXT REFERENCES products(product_id),
  qty          INT NOT NULL,
  reserved_qty INT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (warehouse_id, product_id)
);

CREATE TABLE IF NOT EXISTS product_suppliers(
  product_id      TEXT REFERENCES products(product_id),
  supplier_id     TEXT REFERENCES suppliers(supplier_id),
  cost_usd        NUMERIC(10,2),
  lead_time_days  INT,
  PRIMARY KEY(product_id, supplier_id)
);

-- ===== Indexes (idempotent) =====
CREATE INDEX IF NOT EXISTS idx_users_country         ON users(country);
CREATE INDEX IF NOT EXISTS idx_users_created         ON users(created_at);

CREATE INDEX IF NOT EXISTS ix_products_category      ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_updated      ON products(updated_at);

CREATE INDEX IF NOT EXISTS idx_inventory_qty         ON inventory(qty);
CREATE INDEX IF NOT EXISTS idx_inventory_updated     ON inventory(updated_at);

CREATE INDEX IF NOT EXISTS idx_warehouses_country    ON warehouses(country);
CREATE INDEX IF NOT EXISTS idx_suppliers_country     ON suppliers(country);
CREATE INDEX IF NOT EXISTS idx_suppliers_rating      ON suppliers(rating);

CREATE INDEX IF NOT EXISTS idx_product_suppliers_cost ON product_suppliers(cost_usd);

CREATE INDEX IF NOT EXISTS ix_whinv_product          ON warehouse_inventory(product_id);
CREATE INDEX IF NOT EXISTS idx_warehouse_inventory_qty ON warehouse_inventory(qty);

CREATE INDEX IF NOT EXISTS idx_customer_segments_segment ON customer_segments(segment);
CREATE INDEX IF NOT EXISTS idx_customer_segments_ltv     ON customer_segments(lifetime_value);

-- ===== Grants (safe if run multiple times) =====
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO "$PGUSER";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "$PGUSER";

-- Ensure future objects are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON TABLES    TO "$PGUSER";
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT ALL ON SEQUENCES TO "$PGUSER";
EOSQL

echo "Database setup complete!"
