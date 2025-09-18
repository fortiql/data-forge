# 🧩 Apache Kafka

Why: Event streaming backbone for real‑time data flows.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- None (Schema Registry, Debezium, and Kafka UI depend on it)

## 🚀 How

- Start service:
  - `docker compose --profile core up -d kafka`

- Broker: `localhost:9092`

## 📝 Notes

- Listener settings come from `.env` (advertised listeners, controller quorum).
- Data stored in `kafka-data` volume.

