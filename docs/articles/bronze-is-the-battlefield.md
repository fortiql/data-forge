# Bronze Is the Battlefield: Why Real Data Engineers Start at the Source

Don’t just query downstream tables. Capture events, own lineage, and forge the Bronze layer where data engineering truly begins.

---

## The Trap of SQL Modeling

Many junior data engineers start with SQL: query tables, shape views, and model downstream data. This is useful, but if you stay there you inherit upstream flaws: missing lineage, late data, brittle reprocessing. That is building on sand.

What this looks like in practice:

- You optimize queries on data you did not ingest, so you cannot explain gaps, duplicates, or odd timestamps.
- You deduplicate by heuristics instead of by offsets and event time, so results drift as volumes grow.
- You backfill with ad hoc SQL because the original offsets and checkpoints are unknown.
- You cannot answer basic questions about provenance: where did this record come from, and what was its schema at ingestion time.

Why this hurts teams:

- Debugging slows to a crawl because no one owns the first mile of data.
- Lineage is incomplete, so trust erodes and rework grows.
- Reprocessing is risky and expensive without authoritative checkpoints.

How to escape the trap:

- Start at the source. Capture raw events from Kafka or CDC with offsets, partitions, schema identifiers, and event time.
- Write a Bronze table that preserves provenance fields intact, and treat it as append only.
- Use a durable checkpoint for ingestion jobs, and treat it as the journal of truth.
- Teach downstream models to read from Bronze or from a curated Silver built directly on Bronze.

Real data engineering begins one step earlier: at the raw ingestion layer. This is the Bronze layer.

---

## The Question

Ask yourself: “Am I truly aware of where the data I work on each day is deriving?”

- Event streams (clicks, payments, shipments)?
- Operational databases (CDC feeds)?
- Third‑party APIs (ads, SaaS, partners)?

If you only meet data once it’s already been transformed into tables, you’re blind to its origins.

---

## Why I Built Data Forge

[Data Forge](../../README.md) is a local, open‑source modern data stack you can run on a laptop: Spark, Trino, ClickHouse, Iceberg, Kafka, Airflow, MinIO, wired via Docker Compose. Resettable. No cloud bills.

The point isn’t “spin up tools.” It’s to practice flows: events moving through layers, orchestration that feels real, and patterns that make a data engineer more than just a SQL modeller.

One simple but powerful component is the data generator. It produces a live retail stream: orders, payments, shipments, customers, flowing into Kafka.

- Data Generator: [infra/data-generator/README.md](../../infra/data-generator/README.md)

This article shows why real data engineering starts at Bronze, and how Data Forge implements it.

---

## Medallion Logic

- Bronze: the uncut stone. Raw events with offsets, schema IDs, timestamps, payloads. Forensic truth.
- Silver: the refined ingot. Deduplicated, validated, enriched. Analyst‑friendly.
- Gold: the minted coin. Metrics/models for decisions.

Without Bronze, Silver is shaky; without Silver, Gold misleads.

---

## What Is Bronze (Practically)

A single Bronze row (teaching‑oriented JSON payload) might look like:

```
(event_source=orders.v1,
 partition=3,
 offset=12847,
 schema_id=17,
 event_time=2025-09-15 14:22:00,
 payload_size=512,
 json_payload='{...}')
```

Every field is provenance. Need to replay, reprocess, or debug? Bronze is your logbook.

### Why keep a copy of events in the lake

A reasonable question from a non technical stakeholder is: why duplicate events into the lake if they already exist in Kafka or in source systems. The short answer is reliability and independence for analytics.

- Durability beyond retention: Kafka is a conveyor belt with limited retention. The lake is long term storage for history.
- Replay and reproducibility: with a full event history plus checkpoints, you can rebuild tables, fix bugs, and audit decisions.
- Decoupling from producers: analytics does not depend on producer SLAs, outages, or schema churn. The lake isolates consumers from source instability.
- Governance and access control: one place to apply PII policies, masking, and lineage. Easier reviews and compliance.
- Cost and performance: scanning columnar tables and snapshots in the lake is cheaper and safer than hammering operational systems.
- Multi team enablement: new use cases can start from Bronze without touching producers or requesting new feeds.

In this repo, correctness is anchored by two mechanisms:

- Checkpoints: `--checkpoint` identifies the authoritative offset state for each run.
- ACID writes: Iceberg commits are atomic. Combined with offsets, this gives exactly once ingestion.

Storage is not unbounded. We compact, optimize, and expire snapshots as part of the DAG maintenance, so Bronze remains lean while keeping the facts that matter.

---

## Bounded Streaming Orchestration (Airflow + AvailableNow by Spark Streaming)

Some engineers ask: is Airflow for batch and Spark Streaming for streams, so why cross them. The answer is that AvailableNow turns an infinite stream into a finite, catch up run. Airflow handles run level lifecycle, scheduling, retries, and observability. Spark provides record level streaming semantics, offsets, watermarks, and exactly once writes. Crossing the streams is deliberate: orchestration belongs to Airflow, streaming correctness belongs to Spark.

AvailableNow is a hybrid between batch and always on streaming.

### How it works in this repo

- Airflow triggers a bounded Spark Structured Streaming job via `SparkSubmitOperator` with `trigger(availableNow=True)`.
- The job reads Kafka with `startingOffsets` and `maxOffsetsPerTrigger` to control how much to catch up.
- Offsets and streaming state live under the configured `checkpointLocation` (S3/MinIO).
- Writes land in Iceberg as atomic commits. On retry, Spark resumes from the last successful offset and Iceberg snapshot.

Key DAG params you can tweak at trigger time:

- `topics`: `orders.v1,payments.v1,shipments.v1,inventory-changes.v1,customer-interactions.v1`
- `checkpoint`: `s3a://checkpoints/spark/iceberg/bronze/raw_events` (durable; don’t delete casually)
- `starting_offsets`: `latest` (default) or `earliest` for backfills
- `batch_size`: maps to `maxOffsetsPerTrigger` (backpressure)
- `table`: `iceberg.bronze.raw_events`

### What you get

- Bounded runs with streaming semantics: each run consumes “what’s new,” commits, and stops.
- Offset‑driven correctness: Kafka offsets in checkpoints; Iceberg ACID snapshots on write.
- Late data handled predictably: watermarks/TTL carry across runs; no manual overlap windows.
- Stateful logic survives across runs: checkpoint persists state for joins/aggregations.
- Operational fit: backpressure via `batch_size`; simple retries; capacity scales per run.

### Replay and backfill

- Full rebuild: set `starting_offsets=earliest` and write to a fresh `checkpoint` path.
- Targeted catch‑up: run AvailableNow repeatedly until offsets/time range caught up; switch back to `latest`.
- Idempotency: offsets + Iceberg commits are authoritative; reruns won’t duplicate rows.

### Why it feels strange

- Airflow starts and stops the job, which looks like batch. The job itself is streaming and uses checkpoints and watermarks. Both are true at different layers.
- There is no always on cluster. Capacity is paid per run. The checkpoint carries continuity across runs.
- Recovery is by rerun. Offsets and Iceberg snapshots protect against duplicates and gaps.

### Why not just batch or always on

- Classic batch: you hand‑manage offsets, overlaps, and idempotency, which is fragile at scale.
- Always‑on streaming: ideal for sub‑second SLAs, but requires 24/7 resources and ops.
- AvailableNow: streaming correctness without 24/7 clusters, ideal for hourly Bronze ingestion.

### Common pitfalls

- Deleting checkpoints breaks replay guarantees. Version/rotate intentionally.
- Schema changes may require a new checkpoint path; plan migrations.
- Don’t bypass Iceberg (e.g., writing files directly) or you lose ACID lineage.

Orchestrating AvailableNow with Airflow hits a reliable, simple sweet spot for Bronze.

---

## Architecture


Markdown diagram:

```
+------------------+        Avro events        +--------+
| Data Generator   | ----------------------->  | Kafka  |
+------------------+                            +--------+
                                                ^
                         schemas                |
+------------------+  <-------------------------+
| Schema Registry  |
+------------------+


+------------------+      triggers       +-----------------------------------+
| Airflow (DAG)    | ------------------> | Spark Structured Streaming         |
| bronze_events... |                     | Trigger: AvailableNow              |
+------------------+                     +-----------------------------------+
                                                |
                                   append writes (exactly once)
                                                v
                                 +-----------------------------+
                                 | Iceberg (Bronze tables)     |
                                 +-----------------------------+
                                                |
                                  S3 object storage and paths
                                                v
                                 +-----------------------------+
                                 | MinIO (S3-compatible)       |
                                 +-----------------------------+
                                                ^
                         checkpointLocation     |
                         s3a://checkpoints/...  |
                                                |
                                 +-----------------------------+
                                 | Checkpoints (Spark state)   |
                                 +-----------------------------+


+--------+       maintenance + SQL        +-----------------------------+
| Trino  | --------------------------------> Iceberg Catalog & Tables   |
+--------+                                  (OPTIMIZE, EXPIRE, ORPHANS) |
                                           +-----------------------------+
```

---

## Step 1: Boot the Playground

```bash
# Clone and configure
git clone https://github.com/fortiql/data-forge.git
cd data-forge && cp .env.example .env

# Start core services (MinIO, Trino, Spark, Kafka, etc.)
docker compose --profile core up -d

# Orchestration (Airflow)
docker compose --profile airflow up -d

# Live retail stream (Kafka + Postgres)
docker compose --profile datagen up -d
```

Now events flow: orders, payments, shipments, customer interactions.

- Quick start and ports: [README.md](../../README.md)
- Kafka topics and config: [infra/data-generator/README.md](../../infra/data-generator/README.md)

---

## Step 2: Orchestrate with Airflow

- Open Airflow: http://localhost:8085
- DAG: `bronze_events_kafka_stream`
- Path: [infra/airflow/dags/bronze_events_kafka_stream_dag.py](../../infra/airflow/dags/bronze_events_kafka_stream_dag.py)

Key pieces:

```python
with DAG(
    dag_id="bronze_events_kafka_stream",
    description="Manage Bronze raw_events via streaming from Kafka",
    params={
        "topics": ["orders.v1","payments.v1","shipments.v1","inventory-changes.v1","customer-interactions.v1"],
        "batch_size": 10000,
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/raw_events",
        "starting_offsets": "latest",
        "table": "iceberg.bronze.raw_events",
        "expire_days": "7d",
    },
):
    bounded_ingest = SparkSubmitOperator(
        application="/opt/spark/jobs/bronze_events_kafka_stream.py",
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": "iceberg",
            # ... S3/MinIO + Iceberg + Kafka
        },
        application_args=["--topics","{{ params.topics | join(',') }}",
                          "--batch-size","{{ params.batch_size }}",
                          "--checkpoint","{{ params.checkpoint }}",
                          "--starting-offsets","{{ params.starting_offsets }}",
                          "--table","{{ params.table }}"],
    )
```

After ingestion, the DAG runs Iceberg maintenance (OPTIMIZE, EXPIRE_SNAPSHOTS, REMOVE_ORPHANS) via Trino.

---

## Step 3: The Spark Job (AvailableNow)

Path: [infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py](../../infra/airflow/processing/spark/jobs/bronze_events_kafka_stream.py)

Read Kafka with AvailableNow, extract provenance, write exactly‑once to Iceberg:

```python
src = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topics)
    .option("startingOffsets", starting_offsets)
    .option("maxOffsetsPerTrigger", str(batch_size))
    .option("failOnDataLoss", "false")
    .load().withWatermark("timestamp", "1 minute")
)

# Extract provenance + lightweight payload summary
ordered = src.select(
    F.col("topic").alias("event_source"),
    F.col("timestamp").alias("event_time"),
    F.col("partition"),
    F.col("offset"),
    F.col("value"),
).withColumn(
    "schema_id",
    F.when(F.length("value") >= 5,
           F.conv(F.hex(F.expr("substring(value, 2, 4)")), 16, 10).cast("int"))
     .otherwise(F.lit(None).cast("int"))
).withColumn(
    "payload_size",
    F.when(F.col("value").isNotNull(), F.length("value") - F.lit(5))
     .otherwise(F.lit(None))
).drop("value")

# Exactly-once to Iceberg with AvailableNow
(
  ordered.writeStream.format("iceberg")
  .option("path", table)
  .option("checkpointLocation", checkpoint)
  .outputMode("append")
  .trigger(availableNow=True)
  .start()
  .awaitTermination()
)
```

Checkpoint = the journal of truth. Lose it, and you lose replayability. Respect it.

---

## Step 4: Watch the Flow (Trino)

Connect any SQL client to Trino (`http://localhost:8080`) and run:

```sql
SELECT *
FROM iceberg.bronze.raw_events
ORDER BY event_time DESC
LIMIT 20;
```

Each row ties back to a Kafka offset and partition.

---

## When Not to Use This Pattern

- <1s latency fraud detection
- High‑volume ad‑tech clickstreams (millions/sec)
- Always‑on pipelines where bounded catch‑up is unacceptable

For those: Flink, always‑on Spark, or a dedicated stream processor.

---

## Step 5: Train in Notebooks

Open JupyterLab (`docker compose --profile explore up -d`, then http://localhost:8888) and practice the katas:

- Streaming Fundamentals: checkpoints, offsets, Avro wire format: `notebooks/lessons/streaming/streaming-fundamentals-lesson.ipynb`
- Multi-Topic Streaming: schema metadata, unified checkpoints: `notebooks/lessons/streaming/multi-topic-streaming-lesson.ipynb`
- Bronze on Iceberg: time travel, snapshot cleanup: `notebooks/lessons/streaming/bronze-layer-iceberg-example.ipynb`

Repeat them until the motions feel natural.

---

## A Battle‑Scar Lesson

Daily batch once double‑inserted 300M rows because offsets lived in a fragile side table. It took days to unwind. Offsets belong in checkpoints, not spreadsheets.

---

## Operational Discipline

- Checkpoints are sacred. Version them. Don’t casually delete.
- Maintenance is daily kata. Run OPTIMIZE and EXPIRE SNAPSHOTS.
- Observability is armor. Emit metrics: offsets caught, rows written, batch duration.

---

## ⚔️ A Dojo, Not a Dogma

Data Forge is open ground. AvailableNow, Bronze schema, Airflow orchestration, these are training exercises, not gospel.

- Think Flink is better? Fork and prove it.
- Prefer Delta over Iceberg? Add a profile.
- See a simpler DAG? Open a PR.

Start here: [Data Forge repo](../../README.md)

---

## What’s Next

- Part 1.1: CDC - change data capture from Postgres into Kafka and Bronze using Debezium, with schema evolution, delete tombstones, and replay strategy. See infra/debezium/README.md
- Part 2: Silver - cleaning, dedupe, shaping raw events
- Part 3: Gold - metrics and models in ClickHouse
- Part 4: Federation - Trino across Iceberg, ClickHouse, Postgres

Run the DAG. Query Trino. Watch the flow. Then challenge it. Fork it. Break it. Improve it.

---

> The battlefield of data engineering isn’t won by one playbook, it’s forged in shared practice.

This article is part of the [Data Forge](https://github.com/fortiql/data-forge) project.  
> Published also on Medium: [link]
