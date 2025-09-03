# Data Forge Architecture

## Overview
Data Forge is a modular, reproducible data engineering playground for lakehouse and streaming patterns.

## Components
- Storage: MinIO, Iceberg, Hive Metastore
- Processing: Spark, Airflow
- Streaming: Kafka, Debezium, Schema Registry
- Query: Trino
- BI: Superset

## Medallion Layers
- Bronze: Raw Avro events
- Silver: Cleaned, typed tables
- Gold: Facts/dims for analytics

## Orchestration
Airflow triggers Spark jobs via spark-submit.

## Example Workflow
1. Ingest Wikimedia events into Kafka (Bronze)
2. Spark job: Bronze → Silver
3. Spark job: Silver → Gold
4. Query in Trino, visualize in Superset
