# Data Forge

A reproducible data engineering playground for modern lakehouse and streaming patterns.

## Core Services
- MinIO (S3-compatible storage)
- Hive Metastore
- Apache Iceberg
- Trino
- Postgres (OLTP source)
- Debezium (CDC)
- Kafka (KRaft mode)
- Schema Registry
- Kafka-UI
- Spark (batch + streaming)
- Airflow 3
- Superset

## Medallion Architecture
- Bronze: Raw events
- Silver: Cleaned, typed tables
- Gold: Facts/dims for analytics

## Quickstart
```sh
docker compose --profile core up
```

See `docs/architecture.md` for details.
