# 🧩 Debezium

Why: Change Data Capture (CDC) from PostgreSQL into Kafka.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- Kafka, PostgreSQL

## 🚀 How

- Start service:
  - `docker compose --profile core up -d debezium`

- Connect API: `http://localhost:8083`

## 📝 Notes

- Configure connectors via REST API (see Debezium docs).
- Topics flow into Kafka; downstream consumers include Spark.

