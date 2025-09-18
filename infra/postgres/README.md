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
- Credentials: from `.env` (`POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`)

## 📝 Notes

- Data persisted in `pg-data` volume.
- Used by data generator and Debezium CDC.

