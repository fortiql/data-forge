# 🧩 Hive Metastore

Why: Centralized metadata catalog used by Spark and Trino.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- PostgreSQL

## 🚀 How

- Start service:
  - `docker compose --profile core up -d hive-metastore`

- Thrift: `9083`

## 📝 Notes

- S3A/Hadoop settings for MinIO are in [config/core-site.xml](config/core-site.xml).
- Ensure Metastore is healthy before running Iceberg jobs.
