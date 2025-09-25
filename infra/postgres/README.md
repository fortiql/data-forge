# 🧩 PostgreSQL

Why: Primary OLTP database and CDC source for Debezium.

## ⚙️ Profile

- `core`, `airflow`

## 🔗 Dependencies

- None

## 🚀 How

- Start service:
  - `docker compose --profile core up -d postgres`
- Port: `5432`
- Credentials from `.env`:
  - Admin: `POSTGRES_USER` / `POSTGRES_PASSWORD`
  - CDC role (read-only): `POSTGRES_CDC_USER` / `POSTGRES_CDC_PASSWORD`

## 📝 Notes

- Initializes the `demo` database with OLTP tables on first run.
- Applies logical replication settings (`wal_level=logical`, replication slots, WAL keep size) on every boot.
- Creates and grants a CDC role so Debezium can snapshot and stream tables without superuser access.
- Maintains the `demo_publication` logical publication over every table in `demo.public` so connectors can reuse it.
- Data persists in the `pg-data` volume; remove it to reset all state.
