# 🧩 ClickHouse

Why: Columnar analytics database for fast OLAP queries.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- None (used by other services)

## 🚀 How

- Start service:
  - `docker compose --profile core up -d clickhouse`

- HTTP: `http://localhost:8123`
- Native port: `9000` (mapped to `9002` on host)
- Credentials: from `.env` (`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`)

## 📝 Notes

- Config files mounted: [config.xml](config.xml), [users.xml](users.xml).
- Data persisted in `clickhouse-data` volume.
