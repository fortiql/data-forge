# Bronze Needs CDC

This companion to [Bronze Is the Battlefield](bronze-is-the-battlefield.md) explains why our Bronze layer depends on change data capture, how we wire it with Postgres and Debezium, and how to keep the stream healthy.

---

## Why We Pick CDC Over Replicas

Postgres replicas look tempting: connect Trino, query snapshots, move on. The trade-offs hit the Bronze principles immediately.

- **Freshness** – Replicas lag when VACUUM or checkpoints spike I/O. Logical decoding streams intent as soon as it lands in WAL.
- **History** – A replica only shows the latest row. CDC preserves inserts, updates, deletes, and ordering so Bronze can replay the actual story.
- **Isolation** – Analytics never touches OLTP tables. Debezium reads WAL once; downstream load rides Kafka rather than the production database.
- **Schema fidelity** – CDC pairs every payload with a Schema Registry ID, keeping historical schemas available. Replicas forget what changed.
- **Selective capture** – Publications let us stream only the `demo.public.*` tables that matter.

With the change log living in Bronze, R&D no longer maintains oversized “audit” tables in OLTP just to answer historical questions—Trino queries the CDC-backed Iceberg tables directly.

The cost: configure Postgres for logical decoding, run Debezium, size WAL retention. The payoff: Bronze remains the forensic log described in the primary article.

---

## How Data Forge Streams CDC

### Postgres: prepare the source

`infra/postgres/init-databases.sh` runs on container start and:

- sets `wal_level=logical`, `max_replication_slots=16`, `max_wal_senders=16`, `wal_keep_size=256MB`;
- creates / updates the `cdc_reader` role with `REPLICATION` and `SELECT` on `demo.public` tables;
- keeps `demo_publication` aligned with every table in `demo.public`.

Credentials live in `.env` (`POSTGRES_CDC_USER`, `POSTGRES_CDC_PASSWORD`) and `docker-compose.yml` passes them to both Postgres and Debezium.

Logical replication decodes WAL into relation-level changes (insert/update/delete) rather than byte-for-byte pages. By anchoring everything in one publication we can decide—schema by schema—what flows into CDC, keep privilege scope narrow, and evolve tables without rebuilding replication slots.

### Debezium: capture and publish

`infra/debezium/Dockerfile` extends `debezium/connect:3.0.0.Final` by installing the Confluent Avro converters and copying connector configs plus the `start-with-connectors.sh` launcher. On boot the script:

1. waits for Kafka Connect to report ready;
2. PUTs each `infra/debezium/config/*.json` file to the REST API (default: `demo-postgres`).

The connector (`infra/debezium/config/demo-postgres.json`) reuses `demo_slot`, listens to `demo_publication`, snapshots on first run, and emits Avro messages with Schema Registry metadata.

### Kafka & Schema Registry

CDC topics follow `demo.public.<table>`. Offsets and schema IDs land alongside payloads so Spark can stitch provenance into Bronze tables. Internal topics (`schema-changes.demo`, `my_connect_*`) track schema history and connector state.

---

## Orchestration and Bronze Writes

The Airflow DAG (`infra/airflow/dags/bronze_events_kafka_stream_dag.py`) now runs two paths:

- **`bounded_ingest`** – batches generator topics into the shared `iceberg.bronze.raw_events` table (same flow described in the Bronze article).
- **`ingest_<table>` tasks** – one per CDC topic using `bronze_cdc_stream.py`. Each Spark job:
  - reads a single topic with `AvailableNow` semantics;
  - writes to a dedicated Iceberg table (e.g. `iceberg.bronze.demo_public_users`);
  - records `event_source`, `event_time`, partition/offset, schema ID, payload size, JSON payload.

Every ingest task fans into an Iceberg maintenance task that optimises files, expires snapshots, and removes orphans via Trino.

Spark helpers live in `infra/airflow/processing/spark/jobs/spark_utils.py` (session builder, Avro decoding, Iceberg table creation, checkpoint warnings). Both Spark jobs import it, whether run inside Airflow or manually via `spark-submit`:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --py-files infra/airflow/processing/spark/jobs/spark_utils.py \
  infra/airflow/processing/spark/jobs/bronze_cdc_stream.py \
  --topic demo.public.users \
  --table iceberg.bronze.demo_public_users \
  --checkpoint s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_users
```

---

## Operating Checklist

- **Start services** – `docker compose --profile core up -d postgres kafka schema-registry debezium spark-master spark-worker trino` (profiles bring the stack up; no single-container commands required).
- **Verify connector** – `curl http://localhost:8083/connectors/demo-postgres/status` should show `RUNNING`.
- **Seed changes** – run the data generator (`docker compose --profile datagen up -d data-generator`) or insert directly into Postgres tables to trigger snapshot output.
- **Inspect topics** – use Kafka UI or `docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list` and confirm `demo.public.*` topics exist.
- **Watch Bronze tables** – run the Airflow DAG once; confirm Iceberg tables (`SELECT * FROM iceberg.bronze.demo_public_users LIMIT 5;` in Trino) contain CDC payloads with provenance columns.

---

## Troubleshooting

- **Connector 400 about `AvroConverter`** – rebuild Debezium so the plugins install (`docker compose build debezium`).
- **Publication errors** – drop the `pg-data` volume or rerun Postgres after the updated init script has been baked; the publication must exist before Debezium starts.
- **Only heartbeat topic** – ensure tables have rows; the initial snapshot emits topics only when data exists. Check logs for “no changes will be captured” (wrong include list).
- **WAL retention warnings** – increase `wal_keep_size` or verify Debezium is running; logical slots retain WAL until consumption advances.
- **Checkpoint cleanup** – stop the data generator: it now clears Kafka topics and MinIO checkpoints so repeats start clean (`infra/data-generator/adapters/minio/checkpoints.py`).

---

## Further Reading

- [Bronze Is the Battlefield](bronze-is-the-battlefield.md)
- [A Guide to Logical Replication and CDC in PostgreSQL](https://airbyte.com/blog/a-guide-to-logical-replication-and-cdc-in-postgresql)
- Debezium Postgres connector docs
- Service references: [`infra/postgres/README.md`](../../infra/postgres/README.md), [`infra/debezium/README.md`](../../infra/debezium/README.md)

Bronze stays trustworthy when we capture every change, not just the latest state. CDC is the tool that keeps that promise.
