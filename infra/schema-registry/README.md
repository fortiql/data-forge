# 🧩 Schema Registry

Why: Manage Avro/JSON schemas and compatibility for Kafka topics.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- Kafka

## 🚀 How

- Start service:
  - `docker compose --profile core up -d schema-registry`

- API: `http://localhost:8081`

## 📝 Notes

- Client URL is set via `SCHEMA_REGISTRY_*` envs in compose.
- Used by data generator and Spark structured streaming.

