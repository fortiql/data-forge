# 🧩 Architecture & Profiles

Why: Understand what runs in each profile and how to start it.

## ⚙️ Profiles

- `core` → storage, catalog, streaming, databases.
  - MinIO, PostgreSQL, ClickHouse, Hive Metastore, Kafka, Debezium, Schema Registry, Redis
- `airflow` → orchestration layer.
  - Airflow API server, Scheduler, Worker, Triggerer, DAG Processor
- `explore` → exploration and BI.
  - JupyterLab, Superset
- `datagen` → realistic sample data.
  - Data Generator for Kafka and Postgres

## 🚀 How

- Start core:
  - `docker compose --profile core up -d`
- Add orchestration:
  - `docker compose --profile airflow up -d`
- Add exploration:
  - `docker compose --profile explore up -d`
- Add sample data:
  - `docker compose --profile datagen up -d`

## 📝 Notes

- Services have health checks; give them time to report healthy.
- Resource hint: 8 GB RAM and 20+ GB disk recommended.
- See service docs index: [services.md](services.md).
