# 🧩 Debezium

Why: Change Data Capture (CDC) from PostgreSQL into Kafka topics with schemas in Schema Registry.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- Kafka, Schema Registry, PostgreSQL (CDC role provisioned by the Postgres container)

## 🚀 How

- Start service:
  - `docker compose --profile core up -d debezium`
- Kafka Connect API: `http://localhost:8083`
- Connector configs: `infra/debezium/config/*.json` (auto-applied on boot)

## 📝 Notes

- The image starts Kafka Connect then upserts every JSON config in `/kafka/connectors` (see `start-with-connectors.sh`).
- Default connector `demo-postgres` streams every table from the `public` schema in the `demo` database into Avro-backed topics using Schema Registry at `http://schema-registry:8081`.
- Update credentials by changing `POSTGRES_CDC_USER` / `POSTGRES_CDC_PASSWORD` in `.env` and mirroring them in connector configs.
- Validate with `curl http://localhost:8083/connectors/demo-postgres/status` or via Kafka UI once data lands.
- The Dockerfile pulls the Confluent Avro converter from Confluent Hub during build so the Schema Registry converters resolve without manual steps.
- Publication management is disabled in the connector; the Postgres container pre-creates and keeps `demo_publication` aligned with all tables in `demo.public`.
