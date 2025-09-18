# 🧩 Kafka UI

Why: Inspect Kafka topics, consumer groups, and schemas.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- Kafka, Schema Registry

## 🚀 How

- Start service:
  - `docker compose --profile core up -d kafka-ui`

- UI: `http://localhost:8082`

## 📝 Notes

- Configured via `KAFKA_CLUSTERS_*` envs in compose.

