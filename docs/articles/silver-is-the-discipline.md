# Silver Is the Discipline: Where Truth Becomes Usable

Bronze taught us to own the offsets, the schemas, the raw reality. Silver demands something harder: discipline. This is the layer where raw facts become reliable signals that analysts, dashboards, and machine learning models can trust.

---

## Data Forge Context

[Data Forge](../../README.md) is the laboratory: a full modern data stack you run locally to rehearse these patterns without guesswork. Spin up the Docker profiles, stream real retail events, and watch how Bronze checkpoints feed directly into the Silver star schema.

- Part 1 – Bronze mindset: [Bronze Is the Battlefield](bronze-is-the-battlefield.md)
- Part 1.1 – CDC supply line: [Bronze Needs CDC](cdc-for-bronze.md)

This article is Part 2, turning those raw feeds into a disciplined model.

---

## From Battlefield to Forge

Bronze is a battlefield log. Silver is the forge where we shape that log into a blade. Discipline replaces adrenaline:

- Surrogate keys replace leaky natural identifiers.
- Slowly changing dimensions capture history without losing context.
- Facts conform to a grain that downstream teams can reason about.

The question moves from “Did we ingest it?” to “Can anyone use it without guessing?”

---

## Why Silver Matters

You can ship a dashboard straight from Bronze, but it will rot. Without Silver:

- Product IDs change? Historical analytics point to the wrong attributes.
- Customers move segments? Yesterday’s churn model is based on today’s labels.
- Inventory snapshots overwrite themselves? Operations cannot reconcile stockouts.

Silver is the contract. It makes the semantics explicit, audit trails durable, and joins predictable.

---

## Kimball in Practice, Not Theory

Kimball’s medallion is not nostalgia. It is a checklist you can run:

1. **Dimensions own surrogate keys.** Each row carries effective dates, `is_current`, and the natural key for traceability.
2. **Facts respect grain and conformed dimensions.** Dates, customers, products, warehouses--all referenced through surrogate keys.
3. **Audit fields survive.** Offsets, partitions, and processed timestamps stay in Silver so you can trace every record back to Bronze.

When you run the Silver DAG in Data Forge, you are not running theory. You are practicing the routine of a star schema build under real change data capture pressure.

---

## Anatomy of the Silver DAG

File: [`infra/airflow/dags/silver_retail_star_schema_dag.py`](../infra/airflow/dags/silver_retail_star_schema_dag.py)

- **Trigger:** Iceberg Bronze datasets. Silver rebuilds only after raw sources land safely.
- **Factory Pattern:** Each TableBuilder spawns a SparkSubmitOperator plus an Iceberg maintenance task. The DAG tracks dependencies so dimensions finish before facts.
- **Profiles:** Memory, packages, env vars, and py-files are reused from the Bronze machinery to keep configuration centralized.

This is orchestration discipline: deterministic task names, explicit outlets for lineage, and maintenance scheduled the moment writes complete.

---

## Table Builders: Discipline Encoded

File: [`infra/airflow/processing/spark/jobs/silver/registry.py`](../infra/airflow/processing/spark/jobs/silver/registry.py)

Each builder declares:

- `identifier`: human-readable name (`dim_product_catalog`).
- `table`: Iceberg destination (`iceberg.silver.dim_product_catalog`).
- `primary_key`: enforced before any write hits storage.
- `partition_cols`: optional, but explicit when facts benefit from pruning.
- `requires_raw_events`: flags the need for Kafka topics in addition to CDC tables.

This registry is the contract between orchestration and Spark. Add a builder, and the DAG picks it up without copy-paste.

---

## Star Schema Blueprint

Silver shapes a Kimball star built for retail analytics:

- **Dimensions** – `dim_date`, `dim_customer_profile`, `dim_product_catalog`, `dim_supplier`, `dim_warehouse`.
- **Facts** – `fact_order_service`, `fact_inventory_position`, `fact_customer_engagement`.
- **Conformed Keys** – every fact row references the same surrogate keys for customers, products, dates, and warehouses so metrics reconcile across subject areas.

You can sketch the layout as facts in the center, dimensions on the rim. Analysts query by joining facts to the relevant dimensions through surrogate keys, not by stitching raw natural identifiers together. That distinction matters even to stakeholders outside analytics: the product code in production might look stable, but vendors rename SKUs, billing systems recycle IDs, and marketing reuses segment labels. If we keep “raw columns as is,” dashboards silently drift. Surrogate keys absorb that churn while the natural key columns stick around for traceability.

---

## Dimensions: History Without Guesswork

Take [`build_dim_customer_profile`](../infra/airflow/processing/spark/jobs/silver/builders/dim_customer_profile.py). It merges Debezium feeds for users and segments, consolidates simultaneous updates, and runs an SCD2 routine:

- Window functions forward-fill sparse CDC payloads.
- `scd2_from_events` detects change hashes and assigns `valid_from`/`valid_to`.
- Surrogate keys come from `xxhash64(user_id, valid_from, bronze_offset)`.
- `is_current` and audit columns stick around for analysts and operators alike.

Every other dimension follows the same rhythm: products track price and category shifts, suppliers capture ratings, warehouses blend static metadata with inventory telemetry.

---

## Why SCD2, Not SCD1

- **SCD2 (slowly changing dimension type 2)** keeps each historical change with `valid_from`/`valid_to` ranges. Customer or product changes never overwrite prior state, so facts resolve to the exact attribute set that was true at the event timestamp.
- **SCD1** would overwrite the row in place. Order facts from last quarter would suddenly inherit today’s customer segment or product price. That breaks auditing and erodes trust.

Data Forge is a training ground for real-world CDC. We therefore model dimensions with SCD2 exclusively--history is preserved, temporal joins are deterministic, and analysts can slice by past states without guessing.

---

## Facts: Grain With Teeth

### Order Service

[`build_fact_order_service`](../infra/airflow/processing/spark/jobs/silver/builders/fact_order_service.py) joins orders, payments, and shipments. The joins use event timestamps to select the correct SCD2 slice for each customer and product. The fact table emits:

- Surrogate key (`order_sk`).
- Conformed dimensions (`date_sk`, `customer_sk`, `product_sk`).
- Degenerate dimensions (`order_id`, `payment_id`, `shipment_id`).
- Measures (`order_amount`, `shipment_eta_days`).
- Bronze tracebacks (`bronze_partition`, `bronze_offset`).

### Inventory Position

[`build_fact_inventory_position`](../infra/airflow/processing/spark/jobs/silver/builders/fact_inventory_position.py) treats every inventory change event as a transactional fact. Temporal joins attach the correct warehouse and product version. Each row lands with a `date_sk` and preserved offsets so supply chain teams can reconcile history without reruns.

### Customer Engagement

[`build_fact_customer_engagement`](../infra/airflow/processing/spark/jobs/silver/builders/fact_customer_engagement.py) aggregates interactions per user, product, and day. By keying the joins on `last_interaction_ts`, it honours the customer and product state that was true when that engagement happened.

---

## Maintenance Is Part of the Build

Silver tables are still Iceberg tables. Each DAG run finishes with `OPTIMIZE`, `EXPIRE SNAPSHOTS`, and `REMOVE ORPHANS`. Treat maintenance as part of extraction, not an afterthought.

---

## Practise the Routine

1. Start the stack: `docker compose --profile core --profile airflow --profile datagen up -d`.
2. Let Bronze land events -- in Airflow’s UI, unpause the `bronze_events_kafka_stream` DAG and trigger a run. Watch offsets in the Iceberg console or query via Trino.
3. Trigger the Silver DAG in Airflow or run the Spark job directly:  
   `python infra/airflow/processing/spark/jobs/silver_retail_service.py --tables all`
4. Query the results with Trino:  
   `SELECT * FROM iceberg.silver.fact_order_service ORDER BY order_ts DESC LIMIT 20;`

Run it again after changing generator parameters. Observe how SCD2 dimensions capture each variation. That is the discipline.

---

## Hands-on Kata

Hold the sequence end to end:

1. **Feel Bronze** – in Airflow, unpause `bronze_events_kafka_stream`. Watch the available-now Spark job pull Kafka offsets while checkpoints advance in MinIO. Query `iceberg.bronze.raw_events` in Trino to see raw payloads plus provenance.
2. **Enable Silver** – unpause `silver_retail_service`. The DAG will wait until Bronze datasets publish fresh Iceberg snapshots, then build dimensions first, facts second.
3. **Verify** – once tasks settle, query `iceberg.silver.dim_product_catalog` to see SCD2 rows accumulate. Then query `iceberg.silver.fact_inventory_position` to correlate a single inventory change back to its Bronze offset.
4. **Mutate the Source** – tweak the data generator (change product pricing or customer segments), rerun the Silver job, and observe the new surrogate keys and `valid_from` ranges.

That kata is the muscle memory: source change → Bronze offset → Silver discipline → analyst-ready table.

---

## Metrics the Silver Layer Unlocks

The star schema is the launchpad for Gold metrics. With conformed dimensions in place, you can assemble:

- **Revenue cadence** – daily or weekly `SUM(order_amount)` sliced by product category, supplier, or customer segment.
- **Retention funnels** – join `fact_order_service` and `fact_customer_engagement` to measure how interactions convert to orders across cohorts.
- **Inventory health** – track `AVG(new_qty)` and `SUM(quantity_delta)` per warehouse to flag stockouts before they bite.
- **Supplier performance** – blend `dim_supplier.rating` with on-time shipment metrics from `fact_order_service` for vendor scorecards.
- **Customer lifetime value** – combine Silver facts with `dim_customer_profile.lifetime_value` snapshots for more nuanced forecasting.

Because surrogate keys unify the story, those metrics stay accurate even when upstream systems rename IDs or shuffle segments.

---

## Common Failure Modes--and How Silver Stops Them

- **Natural keys in facts:** Surrogate keys break coupling to volatile IDs.
- **Latest dimension joins:** Temporal predicates guarantee historical accuracy.
- **Silent duplicates:** Primary key enforcement raises on write, not weeks later.
- **Forgotten audits:** Bronze offsets stay attached, so replay is provable.

Silver is not glamorous, but it prevents 2 a.m. incidents.

---

## `_nk` and `_sk`: Traceability by Design

- Columns ending with `_sk` are **surrogate keys**. They are deterministic hashes or sequences generated inside Silver, never exposed to source systems. Facts carry these to ensure joins stay stable even if natural identifiers change or collide.
- Columns ending with `_nk` are **natural keys**. We retain them as readable anchors--`customer_nk`, `product_nk`--so engineers can trace a surrogate key back to the upstream identifier when debugging or reconciling loads. Keeping `customer_nk` even when a `customer_id` exists in the Bronze payload matters because CDC feeds are messy: deletes arrive as tombstones, IDs can be recycled across tenants, and some producers quietly fix data by reusing identifiers. The `_nk` column captures the authoritative natural key used to derive the surrogate, insulated from payload quirks or renames.

Together they give us the best of both worlds: durable joins for analytics and transparent lineage back to the source systems.

---

## What’s Next

- Gold metrics so decision-makers see consistent KPIs.
- More domains (finance, logistics, marketing) to stress-test the pattern.
- Alternative compute engines (Flink, dbt, Iceberg REST) for advanced kata.

Practice the discipline. The forge makes the blade.

---

> “Discipline replaces guesswork. Surrogate keys replace stories. That is the Silver layer.”
