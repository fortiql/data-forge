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
