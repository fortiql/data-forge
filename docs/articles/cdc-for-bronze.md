# Bronze Needs CDC

This companion to [Bronze Is the Battlefield](bronze-is-the-battlefield.md) explains why our Bronze layer depends on change data capture, how we wire it with Postgres and Debezium, and how to keep the stream healthy.

---

## Why We Pick CDC Over Replicas

Replicas look comforting. Point Trino at a follower, query away, and call it a pipeline. But replicas break the very principles Bronze rests on:

- **Freshness** – replicas hiccup during VACUUM or checkpoints. WAL decoding streams intent as it happens, not when storage is ready.
- **History** – a replica only shows the latest row, like a board game frozen mid-turn. CDC preserves inserts, updates, deletes, and their order, so Bronze can replay the full story.
- **Isolation** – OLTP stays untouched. Debezium reads WAL once, and the firehose is safely redirected into Kafka.
- **Schema fidelity** – each change carries a Schema Registry ID, anchoring it in history. Replicas forget what shifted.
- **Selective capture** – publications let us stream only what matters, such as `demo.public.*` in Data Forge’s playground, without noise.

With the change log living in Bronze, R&D no longer maintains oversized “audit” tables in OLTP just to answer historical questions—Trino queries the CDC-backed Iceberg tables directly. When a non-technical stakeholder asks “why bother with CDC if everything already goes to Kafka?”, the answer is scope: domain events cover what producers emit, while OLTP tables accumulate quiet mutations (price corrections, admin fixes, backfills). CDC streams *every* row-level change so Bronze stays faithful; events provide the business narrative.

The cost is configuring Postgres for logical decoding, sizing WAL retention, and running Debezium. The payoff is Bronze that actually deserves the name.

```
Postgres (WAL)
     │
     ▼
Debezium (Kafka Connect)
     │
     ▼
Kafka + Schema Registry
     │
     ▼
Spark (AvailableNow jobs)
     │
     ▼
Iceberg Bronze Tables
     │
     ▼
Trino / Analytics
```

Think of Postgres as the frontline, WAL as the courier bag, Debezium as the scout who radios every move, and Bronze as the journal where it all gets written down.

---

## How Data Forge Streams CDC

### Postgres: prepare the source

[`infra/postgres/init-databases.sh`](../../infra/postgres/init-databases.sh) runs on container start and:

- sets `wal_level=logical`, `max_replication_slots=16`, `max_wal_senders=16`, `wal_keep_size=256MB`;
- creates or updates the `cdc_reader` role with `REPLICATION` and `SELECT` on `demo.public` tables;
- keeps `demo_publication` aligned with every table in `demo.public`.

Credentials live in `.env` (`POSTGRES_CDC_USER`, `POSTGRES_CDC_PASSWORD`) and [`docker-compose.yml`](../../docker-compose.yml) passes them to both Postgres and Debezium.

Logical replication decodes WAL into relation-level changes (insert, update, delete) rather than byte-for-byte pages. By anchoring everything in one publication we can decide schema by schema what flows into CDC, keep privilege scope narrow, and evolve tables without rebuilding replication slots.

A detail worth noting from the logical-replication playbook: slots never advance until consumers acknowledge the LSN. If Debezium is offline too long, WAL files pile up. Postgres 13+ supports `pg_replication_origin` and `pg_slot_advance` to recover, but the simplest guardrail is monitoring slot lag and ensuring `wal_keep_size` covers your longest downtime window.

Additional takeaways from the PostgreSQL CDC guides:

- **Snapshot strategy** – Debezium’s `snapshot.mode=initial` issues a repeatable-read snapshot before streaming WAL. For rebootstrapping from a clean backup, `initial_only` captures the baseline and stops; `never` assumes you restored data another way. Picking the right mode avoids the “long snapshot” warnings highlighted in the Airbyte article.
- **Schema evolution** – logical replication captures DDL, but consumers still need to cope with column additions and drops. Pairing Schema Registry with Iceberg keeps schema changes compatible so Bronze tables don’t break when OLTP adds a nullable column mid-flight.
- **Monitoring lag** – keep an eye on `pg_replication_slots.restart_lsn` versus `pg_stat_replication.write_lsn`. Alert when the gap grows; otherwise WAL files linger on disk. This is the “watch your slots” rule stressed in the replicated-changelog literature.
- **Security scope** – replication slots respect row-level security. By confining `cdc_reader` to `SELECT` + `REPLICATION`, and publishing only `demo.public.*`, we avoid handing Debezium broader write privileges.

### Debezium: capture and publish

[`infra/debezium/Dockerfile`](../../infra/debezium/Dockerfile) extends `debezium/connect:3.0.0.Final` by installing the Confluent Avro converters and copying connector configs plus [`start-with-connectors.sh`](../../infra/debezium/start-with-connectors.sh). On boot the script:

1. waits for Kafka Connect to report ready;
2. PUTs each [`infra/debezium/config/*.json`](../../infra/debezium/config/) file to the REST API (default: `demo-postgres`).

The connector ([`infra/debezium/config/demo-postgres.json`](../../infra/debezium/config/demo-postgres.json)) reuses `demo_slot`, listens to `demo_publication`, snapshots on first run, and emits Avro messages with Schema Registry metadata.

### Kafka and Schema Registry

CDC topics follow `demo.public.<table>`. Offsets and schema IDs land alongside payloads so Spark can stitch provenance into Bronze tables. Internal topics (`schema-changes.demo`, `my_connect_*`) track schema history and connector state.

---

## Orchestration and Bronze Writes

The Airflow DAG ([`infra/airflow/dags/bronze_events_kafka_stream_dag.py`](../../infra/airflow/dags/bronze_events_kafka_stream_dag.py)) now runs two paths:

- **`bounded_ingest`** – batches generator topics into the shared `iceberg.bronze.raw_events` table (same flow described in the Bronze article). The job lives in [`infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py`](../../infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py). Generator feeds already represent multiple event types that analysts often join together, so a single Bronze table keeps that “raw bus” intact.
- **`ingest_<table>` tasks** – one per CDC topic using [`infra/airflow/processing/spark/jobs/bronze_cdc_stream.py`](../../infra/airflow/processing/spark/jobs/bronze_cdc_stream.py). Each Spark job:
  - reads a single topic with `AvailableNow` semantics;
  - writes to a dedicated Iceberg table (for example `iceberg.bronze.demo_public_users`);
  - records `event_source`, `event_time`, partition or offset, schema ID, payload size, JSON payload.

Every ingest task fans into an Iceberg maintenance task that optimises files, expires snapshots, and removes orphans via Trino. Shared Spark helpers live in [`infra/airflow/processing/spark/jobs/spark_utils.py`](../../infra/airflow/processing/spark/jobs/spark_utils.py). CDC streams stay split per table so each Iceberg dataset mirrors one OLTP table—ideal for deterministic upserts and history tracking—while the generator path intentionally remains a single mixed stream for convenience when analysing the synthetic retail bus.

---

## Operating Checklist

- **Start services** – `docker compose --profile core up -d` (this brings Postgres, Kafka, Schema Registry, Debezium, Spark, Trino, and other core components up together).
- **Bring up Airflow** – `docker compose --profile airflow up -d` (scheduler, webserver, workers). Visit `http://localhost:8085` to monitor tasks and trigger runs.
- **Verify connector** – `curl http://localhost:8083/connectors/demo-postgres/status` should show `RUNNING`.
- **Seed changes** – run the data generator (`docker compose --profile datagen up -d data-generator`) or insert directly into Postgres tables to trigger snapshot output.
- **Inspect topics** – use Kafka UI or `docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list` and confirm `demo.public.*` topics exist.
- **Trigger the Bronze DAG** – in Airflow, trigger `bronze_events_kafka_stream`; watch `bounded_ingest` and the per-table CDC tasks reach `success`.
- **Watch Bronze tables** – query in Trino (`SELECT * FROM iceberg.bronze.demo_public_users LIMIT 5;`) to confirm CDC payloads and provenance columns landed.

---

## Troubleshooting

- **Connector 400 about `AvroConverter`** – rebuild Debezium so the plugins install (`docker compose build debezium`).
- **Publication errors** – drop the `pg-data` volume or rerun Postgres after the updated init script has been baked, the publication must exist before Debezium starts.
- **Only heartbeat topic** – ensure tables have rows, the initial snapshot emits topics only when data exists. Check logs for “no changes will be captured” (wrong include list).
- **WAL retention warnings** – increase `wal_keep_size` or verify Debezium is running, logical slots retain WAL until consumption advances.
- **Checkpoint cleanup** – stop the data generator; it now clears Kafka topics and MinIO checkpoints so repeats start clean ([`infra/data-generator/adapters/minio/checkpoints.py`](../../infra/data-generator/adapters/minio/checkpoints.py)).

---

## Further Reading

- [Bronze Is the Battlefield](bronze-is-the-battlefield.md)
- [A Guide to Logical Replication and CDC in PostgreSQL](https://airbyte.com/blog/a-guide-to-logical-replication-and-cdc-in-postgresql)
- Debezium Postgres connector docs
- Service references: [`infra/postgres/README.md`](../../infra/postgres/README.md), [`infra/debezium/README.md`](../../infra/debezium/README.md)

Bronze stays trustworthy when we capture every change, not just the latest state. CDC is the tool that keeps that promise.
