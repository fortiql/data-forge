# 🧩 Trino

Why: Interactive SQL query engine for federated analytics over lakehouse data.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- Hive Metastore, MinIO (for S3 storage). Optional: ClickHouse as a target.

## 🚀 How

- Start service:
  - `docker compose --profile core up -d trino`

- UI: `http://localhost:8080` (no auth)

- Catalogs:
  - Iceberg catalog config: [config/catalog/iceberg.properties](config/catalog/iceberg.properties)
  - S3 endpoint: `http://minio:9000`

## 📝 Notes

- Trino reads Iceberg tables in MinIO via the Iceberg catalog.
- Adjust catalog configs under [config/catalog/](config/catalog/) as needed.
- Bronze tables include generator topics and Debezium CDC streams. Postgres is configured for logical replication (`wal_level=logical`, `demo_publication`), Debezium streams WAL changes into Kafka, and Spark “AvailableNow” jobs land them in Iceberg so Trino can query every insert/update/delete directly—no need for broad “audit” tables.
- Query CDC-backed tables under `iceberg.bronze.*`. Each row retains `event_source`, `event_time`, partition/offset, schema ID, and JSON payload, keeping lineage intact for analytics.
- More background on the pipeline: [docs/articles/cdc-for-bronze.md](../../docs/articles/cdc-for-bronze.md).
