# Bronze Needs CDC

In [Bronze Is the Battlefield](bronze-is-the-battlefield.md) we planted the flag: Bronze is not a warehouse of polished facts, it is a battlefield log. Offsets, schemas, and late events must survive or downstream truth means nothing. But a log is useless if your scouts stop reporting. That is why Bronze needs change data capture.

---

## Why We Pick CDC Over Replicas

Replicas look comforting. Point Trino at it, query, call it a pipeline. But replicas break the Bronze playbook:

- **Freshness** – replicas could struggle during VACUUM or checkpoints. WAL decoding streams intent as it happens.
- **History** – a replica is a snapshot, like a chess board frozen mid-turn. CDC keeps every insert, update, delete, and the order they arrived.
- **Isolation** – OLTP stays untouched. Debezium reads WAL once and ships the firehose into Kafka.
- **Schema fidelity** – every change carries a Schema Registry ID. Replicas forget what shifted.
- **Selective capture** – publications let us stream only what matters (`demo.public.*` in our playground).

With the change log in Bronze, R&D doesn't need to build bloating audit tables in OLTP to answer history. Trino queries the CDC-backed Iceberg tables directly. When someone asks “why bother with CDC if everything already flows through Kafka?”, the answer is coverage: domain events represent what producers choose to emit; CDC records the quiet mutations (price fixes, admin toggles, backfills) that never become events.

The cost is enabling logical decoding, sizing WAL retention, and running Debezium. The payoff is Bronze that actually deserves the name.

```
Postgres
     │
     ▼
Debezium
     │
     ▼
Kafka + Schema Registry
     │
     ▼
Spark
     │
     ▼
Iceberg Bronze Tables
     │
     ▼
Trino / Analytics
```

Think of Postgres as the frontline, WAL as the courier bag, Debezium as the scout radioing every move, and Bronze as the journal where it all lands.

---

## Data Forge: a CDC Gym

This is why Data Forge ships with Postgres, Debezium, Kafka, Schema Registry, and Spark out of the box. It is a gym for data engineers. Spin it up locally, break it safely, fix it, and feel the full CDC loop without production risk. Start with the [project README](../../README.md) for a diagram of every service and how the `demo.public.*` publication threads through them, then drill into [Trino's walkthrough](../../infra/trino/README.md) to see how the Iceberg tables surface for SQL and the [data generator guide](../../infra/data-generator/README.md) to learn how demo changes land in Kafka and Postgres.

---

## How Data Forge Streams CDC

### Postgres: prepare the source

`infra/postgres/init-databases.sh` runs on container start and:

- sets `wal_level=logical`, `max_replication_slots=16`, `max_wal_senders=16`, `wal_keep_size=256MB`;
- creates or updates the `cdc_reader` role with `REPLICATION` and `SELECT` on `demo.public` tables;
- keeps `demo_publication` aligned with every table in `demo.public`.

Two Postgres primitives make this work:

- **Replication slot** – a logical slot (here `demo_slot`) pegs the database to an LSN for a given consumer. Postgres retains WAL segments until the slot confirms they were consumed. Debezium binds to this slot so it can resume exactly where it left off, even across restarts.
- **Publication** – a named list of tables that expose change events. `demo_publication` advertises every table in `demo.public`; Debezium subscribes to it so new tables appear without rebuilding the connector. Slots and publications together let Debezium stream only the intended tables while protecting WAL from being pruned beneath the connector.

Credentials live in `.env` and `docker-compose.yml` passes them into Postgres and Debezium. Logical replication decodes WAL into inserts, updates, and deletes. Not raw bytes. Actual intent. One publication gives us schema-by-schema control without rebuilding slots.

A warning from the replication guides: slots only advance when consumers acknowledge the LSN. The log sequence number (LSN) is Postgres' 64-bit pointer into the write-ahead log; every committed change is stamped with a higher LSN so replicas and logical decoders know the exact byte position they have processed. Debezium persists this pointer in its offsets topic, and on restart it asks Postgres to resume streaming from that LSN so there are no gaps or duplicates. Because LSNs advance even when Debezium is offline, a paused connector means the slot's restart LSN stops moving and WAL segments accumulate. Monitor slot lag with queries like:

```sql
SELECT slot_name, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;
SELECT pid, state, sent_lsn, write_lsn, flush_lsn, replay_lsn FROM pg_stat_replication;
```

Keep `wal_keep_size` sized for your downtime window, and lean on `pg_slot_advance` only as a last resort.

Additional lessons from the CDC community:

- Snapshot strategy matters. `snapshot.mode=initial` takes a repeatable-read snapshot, `initial_only` captures and stops, `never` assumes you restored another way.
- Schema evolution happens. Pair Schema Registry with Iceberg so column additions or drops do not break consumers.
- Lag metrics tell the story. Compare `pg_replication_slots.restart_lsn` and `pg_stat_replication.write_lsn`; alert when the gap grows.
- Security scope stays tight. `cdc_reader` has `REPLICATION` but no write privileges, and row-level security still applies.

### Debezium: capture and publish

[`infra/debezium/Dockerfile`](../../infra/debezium/Dockerfile) installs the Confluent Avro converters and copies [`start-with-connectors.sh`](../../infra/debezium/start-with-connectors.sh). On boot the launcher waits for Kafka Connect and `PUT`s every connector JSON in [`infra/debezium/config/`](../../infra/debezium/config/). The default config ([`demo-postgres.json`](../../infra/debezium/config/demo-postgres.json)) reuses `demo_slot`, listens to `demo_publication`, snapshots once, and streams Avro payloads with Schema Registry metadata. Deletes stay as update-style events because `tombstones.on.delete` is `false`; flip it to `true` when you need Kafka tombstones for log compaction.

### Kafka and Schema Registry

CDC topics follow `demo.public.<table>`. Each record carries partition, offset, schema ID, and the decoded payload so we can stitch provenance into Bronze. Internal topics (`schema-changes.demo`, `my_connect_*`) keep track of schema history and connector state.

### Orchestration and Bronze Writes

The Airflow DAG ([`infra/airflow/dags/bronze_events_kafka_stream_dag.py`](../../infra/airflow/dags/bronze_events_kafka_stream_dag.py)) runs two paths:

- **`bounded_ingest`** – batches generator topics into the shared `iceberg.bronze.raw_events` table. The job lives in [`infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py`](../../infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py). Generator feeds already blend multiple event types, so one Bronze table keeps that raw bus intact.
- **`ingest_<table>` tasks** – one per CDC topic using [`infra/airflow/processing/spark/jobs/bronze_cdc_stream.py`](../../infra/airflow/processing/spark/jobs/bronze_cdc_stream.py). Each job reads a single topic with `AvailableNow`, writes to a dedicated Iceberg table, and records `event_source`, `event_time`, schema IDs, payload sizes, partitions, and offsets.

Those CDC tasks are explicitly listed in `CDC_STREAMS` inside the DAG ([`infra/airflow/dags/bronze_events_kafka_stream_dag.py`](../../infra/airflow/dags/bronze_events_kafka_stream_dag.py#L69)). To onboard a new Postgres table you must add its topic, target table, and checkpoint path there so Airflow schedules the matching ingest job.

#### Example: `iceberg.bronze.demo_public_warehouse_inventory`

The CDC table for `demo.public.warehouse_inventory` follows the common Bronze schema (`event_source`, `event_time`, `schema_id`, `payload_size`, `json_payload`, `partition`, `offset`). The payload itself stores the full Debezium envelope so you can replay, reprocess, or audit every change without decoding Avro again. A real update looks like this:

```json
{"before":null,"after":{"warehouse_id":"WH002","product_id":"P000040","qty":598,"reserved_qty":22,"updated_at":"2025-09-26T07:33:47.178366Z"},"source":{"version":"3.0.0.Final","connector":"postgresql","name":"demo","ts_ms":1758872027178,"snapshot":"false","db":"demo","sequence":"[\"1152784920\",\"1152784976\"]","ts_us":1758872027178519,"ts_ns":1758872027178519000,"schema":"public","table":"warehouse_inventory","txId":2460848,"lsn":1152784976,"xmin":null},"transaction":null,"op":"u","ts_ms":1758872027457,"ts_us":1758872027457782,"ts_ns":1758872027457782333}
```

- `event_source` is the Kafka topic (`demo.public.warehouse_inventory`), so lineage to the publication stays obvious.
- `event_time` is when Kafka observed the event; `json_payload.ts_*` fields retain Debezium's original timestamps.
- `json_payload.after` carries the row image that analytics teams read most often, while `json_payload.before` appears on deletes and updates so you can build slowly changing dimensions or audit trails.
- `json_payload.source.lsn` mirrors the slot's replay position; use it to confirm ingestion progressed and to correlate with Postgres logs when investigating gaps.
- Offsets and partitions remain alongside the payload, giving you the option to re-seek a specific record in Kafka if you ever need to rebuild Bronze.

Every ingest task fans into an Iceberg maintenance step (optimize, expire snapshots, remove orphans) via Trino. Shared Spark helpers live in [`infra/airflow/processing/spark/jobs/spark_utils.py`](../../infra/airflow/processing/spark/jobs/spark_utils.py). CDC streams stay split per table so each Iceberg dataset mirrors one OLTP table, which is perfect for deterministic upserts and history tracking.

---

## Operating Checklist

- **Start services** – `docker compose --profile core up -d` (brings Postgres, Kafka, Schema Registry, Debezium, Spark, Trino, and friends online).
- **Bring up Airflow** – `docker compose --profile airflow up -d` (scheduler, webserver, workers). Open `http://localhost:8085` to monitor runs.
- **Verify connector** – `curl http://localhost:8083/connectors/demo-postgres/status` should show `RUNNING`.
- **Seed changes** – `docker compose --profile datagen up -d data-generator` or insert rows directly into Postgres.
- **Inspect topics** – `docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list` and confirm `demo.public.*` topics exist.
- **Trigger the Bronze DAG** – run `bronze_events_kafka_stream` in Airflow and wait for `bounded_ingest` plus the per-table CDC tasks to succeed.
- **Check Iceberg tables** – in Trino: `SELECT * FROM iceberg.bronze.demo_public_users LIMIT 5;` to confirm payloads and provenance landed.

---

## Troubleshooting

- **Only heartbeat topic** – ensure tables have rows. Snapshot mode emits topics only when data exists; check logs for “no changes will be captured” if include lists are wrong.
- **WAL retention warnings** – bump `wal_keep_size` or confirm Debezium is running; slots hold WAL until consumption advances.
- **Checkpoint cleanup** – stop the data generator; it now clears Kafka topics and MinIO checkpoints so repeats start clean ([`infra/data-generator/adapters/minio/checkpoints.py`](../../infra/data-generator/adapters/minio/checkpoints.py)).

---

## For Practitioners

<details>
<summary>Key files</summary>

- Postgres init: [`infra/postgres/init-databases.sh`](../../infra/postgres/init-databases.sh)
- Debezium image and launcher: [`infra/debezium/Dockerfile`](../../infra/debezium/Dockerfile), [`infra/debezium/start-with-connectors.sh`](../../infra/debezium/start-with-connectors.sh)
- Debezium connector config: [`infra/debezium/config/demo-postgres.json`](../../infra/debezium/config/demo-postgres.json)
- Airflow DAG: [`infra/airflow/dags/bronze_events_kafka_stream_dag.py`](../../infra/airflow/dags/bronze_events_kafka_stream_dag.py)
- Spark jobs: [`infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py`](../../infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py), [`infra/airflow/processing/spark/jobs/bronze_cdc_stream.py`](../../infra/airflow/processing/spark/jobs/bronze_cdc_stream.py), [`infra/airflow/processing/spark/jobs/spark_utils.py`](../../infra/airflow/processing/spark/jobs/spark_utils.py)
- Silver discipline: read [Silver Is the Discipline](silver-is-the-discipline.md) for how the DAG and builders convert CDC history into SCD2 tables.
- Silver DAG: [`infra/airflow/dags/silver_retail_star_schema_dag.py`](../../infra/airflow/dags/silver_retail_star_schema_dag.py) — one SparkSubmitOperator per Silver table with Iceberg maintenance and dependency wiring.
- Silver Spark job: [`infra/airflow/processing/spark/jobs/silver_retail_service.py`](../../infra/airflow/processing/spark/jobs/silver_retail_service.py) — pass `--tables dim_customer_profile,fact_order_service` to rebuild targeted outputs; each dimension emits surrogate keys consumed by the facts.
- Checkpoint cleaner: [`infra/data-generator/adapters/minio/checkpoints.py`](../../infra/data-generator/adapters/minio/checkpoints.py)

</details>

---

## Further Reading

- [Bronze Is the Battlefield](bronze-is-the-battlefield.md)
- [A Guide to Logical Replication and CDC in PostgreSQL](https://airbyte.com/blog/a-guide-to-logical-replication-and-cdc-in-postgresql)
- [Debezium Postgres connector docs](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- Service references: [`infra/postgres/README.md`](../../infra/postgres/README.md), [`infra/debezium/README.md`](../../infra/debezium/README.md)

Few companies wire Bronze this way. If you are a data engineer who wants to feel the real thing (CDC, WALs, Debezium connectors, Iceberg sinks), Data Forge is your gym. Spin it up, break it, fix it, and learn what Bronze is supposed to feel like.
