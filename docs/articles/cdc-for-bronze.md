# CDC Wins the Bronze: Capturing Postgres Changes for the Raw Layer

Why the Bronze layer leans on logical replication, how Debezium moves changes into Kafka, and how Data Forge keeps the stream reliable.

---

## Replica or CDC?

A Postgres replica wired to Trino feels comfortable: point SQL at a follower, run the joins you already know. The trade-off is latency, load, and missing history.

Logical replication (CDC) streams intent—every `INSERT`, `UPDATE`, `DELETE` with the precise order the OLTP system emitted. For Bronze, that timeline matters more than raw query access.

Reasons CDC wins:

- **Freshness:** WAL decoding emits changes within seconds. Replicas lag whenever VACUUM or checkpoints saturate I/O; the bronze layer wants deltas as they happen.
- **Replayability:** Replicas expose the current state. CDC preserves the story so you can rebuild silver tables, audit bugs, and explain deletes.
- **Isolation:** CDC reads the write-ahead log once. Analytics never touches the production tables, unlike replicas that still serve heavy SQL.
- **Schema fidelity:** With Schema Registry, CDC carries versioned schemas. Replicas only expose “now,” so historical columns disappear silently.
- **Selective capture:** Publications filter to just the tables analytics needs. Replicas mirror everything, even noisy operational tables.

CDC costs: you must enable logical WAL, size retention, and keep a connector healthy. The payoff is Bronze that records “what happened,” not just “what is.”

---

## CDC Flow in Data Forge

Profile `core` spins up the necessary services:

- **Postgres (`infra/postgres`)** – OLTP source; init script enables logical WAL, provisions the `cdc_reader` role, and maintains `demo_publication`.
- **Debezium (`infra/debezium`)** – Kafka Connect worker packaged with the PostgreSQL connector and the Confluent Avro converters.
- **Kafka (`infra/kafka`)** – event bus for change topics.
- **Schema Registry (`infra/schema-registry`)** – Avro schema store so downstream consumers keep compatibility guarantees.

Bring the stack online:

```
docker compose --profile core build postgres debezium
docker compose --profile core up -d postgres kafka schema-registry debezium
```

Debezium waits for Kafka Connect readiness, then PUTs every config under `/kafka/connectors`. The default `demo-postgres` connector snapshots existing rows and switches to streaming without manual intervention.

---

## Postgres Side: Logical Replication

`infra/postgres/init-databases.sh` applies the required settings on startup:

- `wal_level=logical`, `max_replication_slots=16`, `max_wal_senders=16`, `wal_keep_size=256MB` → WAL is prepared for logical decoding and retains enough history in case consumers pause.
- `cdc_reader` role → login with `REPLICATION` rights plus `SELECT` on all future tables in `demo.public`.
- `demo_publication` → covers every table in the `public` schema; recreated idempotently so Debezium can subscribe without superuser privileges.

The script is idempotent, so restarts keep the publication aligned with any new demo tables.

---

## Debezium Side: Connector Anatomy

`infra/debezium/config/demo-postgres.json` drives the capture. Important fields:

- `database.user` / `database.password` – maps to `POSTGRES_CDC_USER` / `POSTGRES_CDC_PASSWORD` from `.env`.
- `slot.name=demo_slot` – reuses the slot managed by Postgres; Debezium will resume from the last acknowledged LSN.
- `publication.name=demo_publication` and `publication.autocreate.mode=disabled` – rely on the pre-created publication.
- `snapshot.mode=initial` – first run snapshots existing rows before consuming live WAL changes.
- `schema.include.list=public` – capture every table in the demo schema without prefix mismatches.
- `key.converter` / `value.converter` – Confluent Avro converters pointing to `http://schema-registry:8081`; ensures keys/values are versioned and validated.
- `schema.history.internal.kafka.topic=schema-changes.demo` – internal topic where Debezium stores schema evolution events.

The Debezium image installs the Avro converter plugin during `docker build`, so no manual JAR copying is required.

Health checks:

```
curl http://localhost:8083/connectors/demo-postgres/status
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Snapshot completion is quick (the seeded dataset is ~500 rows). After that, any `INSERT`/`UPDATE`/`DELETE` reaches Kafka with second-level lag.

---

## Using CDC to Feed Bronze

Debezium topics follow `demo.public.<table>`. Each message contains:

- **Key:** primary key fields for deterministic upserts.
- **Value:** structured payload with `before`, `after`, operation code (`op`), source metadata, and the transaction LSN.

In the Bronze ingestion DAG (Spark AvailableNow), we persist both the Avro payload and provenance: topic, partition, offset, schema ID, LSN, and timestamp. From there:

- Silver tables can be rebuilt by replaying Bronze with deterministic merges (`op = c/u/d`).
- Late arriving updates are handled by offsets, not heuristics.
- Schema evolution remains traceable because Schema Registry versions accompany every record.

Compared to querying a replica via Trino:

- **History-first:** Bronze keeps the full change log; replicas only show the latest snapshot.
- **Replay safety:** Kafka offsets + Iceberg snapshots give exactly-once semantics across reruns.
- **Operational isolation:** Streaming loads never touch the OLTP or its replicas.

If you need a stateful snapshot for analytics, build it in Silver or Gold from Bronze using `last_value` or window functions. The Bronze log stays immutable.

---

## Troubleshooting Quick Hits

- Connector 400 referencing `AvroConverter` → rebuild Debezium so the Confluent plugin is present (see `infra/debezium/Dockerfile`).
- Connector fails creating publication → ensure Postgres has rebuilt with the latest init script or remove the `pg-data` volume for a clean reset.
- Only heartbeat topic appears → verify tables contain rows and `schema.include.list=public`; the initial snapshot writes topics when data exists.
- WAL bloat warnings → increase `wal_keep_size` or ensure Debezium stays connected so slots acknowledge progress.

---

## Further Reading

- [A Guide to Logical Replication and CDC in PostgreSQL](https://airbyte.com/blog/a-guide-to-logical-replication-and-cdc-in-postgresql)
- Debezium Postgres connector docs
- Data Forge service guides: [`infra/postgres/README.md`](../../infra/postgres/README.md), [`infra/debezium/README.md`](../../infra/debezium/README.md)

Bronze earns its name when it records the story, not just the snapshot. CDC is how we get there without burdening the source.
