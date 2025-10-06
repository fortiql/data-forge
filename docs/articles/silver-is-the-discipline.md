# Silver Is the Discipline: Where Truth Becomes Usable

> Bronze taught us to survive the battlefield. Silver teaches us to build the weapon.

Bronze taught us to own offsets, schemas, and raw events. Silver demands something harder: discipline. This layer turns uncut facts into reliable signals that analysts, dashboards, and machine learning models can trust.

---

## Data Forge Context

[Data Forge](../../README.md) is an open-source practice ground for modern data engineering. Spin up the Docker profiles, stream the generated retail data, and watch Bronze checkpoints feed the Silver star schema.

- Part 1 – Bronze mindset: [Bronze Is the Battlefield](bronze-is-the-battlefield.md)
- Part 1.1 – CDC supply line: [Bronze Needs CDC](cdc-for-bronze.md)

This article is Part 2: shaping those raw feeds into disciplined, analyst-ready tables.

---

## From Battlefield to Forge

Bronze is a battlefield log. Silver is the forge where that log becomes a blade. Discipline replaces adrenaline:

- Surrogate keys stand in for leaky natural identifiers.
- Slowly changing dimensions capture history instead of overwriting it.
- Facts hold a stable grain that downstream teams can reason about.

The question shifts from “Did we ingest it?” to “Can anyone use it without guessing?”

---

## Why Silver Matters

You can wire a dashboard straight to Bronze, but it will rot. Without Silver:

- Product identifiers drift and break historical attribution.
- Customers change segments; churn models inherit today’s context for yesterday’s events.
- Inventory snapshots overwrite themselves; operations cannot reconcile stockouts.

Silver is the contract. It makes semantics explicit, preserves audit trails, and keeps joins predictable.

---

## Kimball Discipline in a Medallion Flow

Dimensional modeling from Kimball meets the modern Bronze/Silver/Gold pattern popularised by the Delta Lake community. Discipline here includes holding the line on naming conventions: surrogate keys (`*_sk`), natural keys (`*_nk`), degenerate dimensions, metrics in facts, attributes in dims. Hybrid patterns absolutely exist because real platforms evolve through negotiated manifests between data teams and the business, but the manifest only stays trustworthy if the notation is consistent. The moment “just add a metric column to the dimension” slips through, you lose the signal of where grain, lineage, and surrogate boundaries live. Call the object whatever you want, but honour the suffixes and the grain they imply.

Why the insistence?
- Suffix conventions become guardrails for automated tests, lineage tools, and downstream SQL that expect surrogate keys in facts and natural keys in dims.
- Business glossaries map to the manifest; if a “dimension” starts carrying ad-hoc metrics or opaque IDs, reconciliation with source systems breaks.
- Snowball exceptions cost more than they save. One shortcut forces custom joins, bespoke loaders, and unplanned rework when you later standardise.

Most production incidents I have lived through started as “just this once” pressure from the business: a Jira with an ad-hoc SQL slice that bypasses the star schema because a stakeholder needs a metric by morning. Those shortcuts ship fast, but they erode the Silver contract. Tables fork, lineage blurs, and the next refresh breaks because nobody wired the temporary column into the builders. You can empathise with the timing pressure and still insist that every change lands through the manifest, with `_sk`/`_nk` consistency and regression checks. The negotiation is real; the discipline is non-negotiable.

You can adopt a hybrid model, but write it down and tag every column accordingly. The notation is the contract.

That combination gives you a checklist you can run:

1. **Dimensions own surrogate keys.** Each row keeps effective dates, `is_current`, and the natural key for traceability.
2. **Facts respect grain and conformed dimensions.** Dates, customers, products, and warehouses are referenced through surrogate keys.
3. **Audit fields survive.** Offsets, partitions, and processed timestamps stay present so every record traces back to Bronze.

Running the Silver DAG in Data Forge is real practice, not theory. You are building a star schema under change-data-capture pressure.

---

## Anatomy of the Silver DAG

File: [`infra/airflow/dags/silver_retail_star_schema_dag.py`](../../infra/airflow/dags/silver_retail_star_schema_dag.py)

- **Triggering** – Silver runs only after Bronze Iceberg snapshots publish successfully.
- **Factory pattern** – Each TableBuilder spawns a `SparkSubmitOperator`, followed by Iceberg maintenance.
- **Shared profiles** – Memory settings, package deps, and `py_files` are reused from Bronze jobs to keep configuration centralized.

This is orchestration discipline: deterministic task names, explicit outlets for lineage, and maintenance scheduled the moment writes complete.

---

## Table Builders: Discipline Encoded

File: [`infra/airflow/processing/spark/jobs/silver/registry.py`](../../infra/airflow/processing/spark/jobs/silver/registry.py)

Each builder declares:

- `identifier` – human-readable handle (`dim_product_catalog`).
- `table` – Iceberg destination (`iceberg.silver.dim_product_catalog`).
- `primary_key` – enforced before any write hits storage.
- `partition_cols` – optional but explicit when pruning helps.
- `requires_raw_events` – flags dependencies on Kafka topics in addition to CDC tables.

The registry is the contract between orchestration and Spark. Add a builder and the DAG picks it up without copy-paste.

---

## Star Schema Blueprint

Silver shapes a Kimball star tuned for retail analytics:

- **Dimensions** – `dim_date`, `dim_customer_profile`, `dim_product_catalog`, `dim_supplier`, `dim_warehouse`.
- **Facts** – `fact_order_service`, `fact_inventory_position`, `fact_customer_engagement`.
- **Conformed keys** – every fact row references the same customer, product, date, and warehouse surrogates so metrics reconcile across subject areas.

Think of facts in the hub and dimensions on the rim. Analysts join through surrogate keys rather than stitching volatile natural identifiers. That keeps dashboards stable even when upstream systems rename SKUs or recycle customer IDs.

---

## Dimensions: History Without Guesswork

Take [`build_dim_customer_profile`](../../infra/airflow/processing/spark/jobs/silver/builders/dim_customer_profile.py). It merges Debezium feeds for users and segments, consolidates simultaneous updates, and runs an SCD2 routine:

- Window functions forward-fill sparse CDC payloads.
- `scd2_from_events` hashes change fields, then assigns `valid_from` / `valid_to`.
- Surrogate keys are generated with `xxhash64(user_id, valid_from, bronze_offset)`.
- `is_current` plus audit columns stay available for analysts and operators.

Other dimensions follow the same rhythm: products track price and category shifts, suppliers maintain ratings, warehouses blend static metadata with inventory telemetry.

```python
from silver.common import scd2_from_events, surrogate_key

scd = scd2_from_events(
    filled,
    key_cols=["user_id"],
    ordering_cols=["valid_from", "bronze_offset"],
    state_cols=["email", "country", "segment", "lifetime_value"],
).withColumn(
    "customer_sk",
    surrogate_key(F.col("user_id"), F.col("valid_from"), F.col("bronze_offset")),
)
```

_Excerpt from `infra/airflow/processing/spark/jobs/silver/builders/dim_customer_profile.py`._

---

## Facts: Grain With Teeth

### Order Service

[`build_fact_order_service`](../../infra/airflow/processing/spark/jobs/silver/builders/fact_order_service.py) joins orders, payments, and shipments. Event timestamps pick the right SCD2 slice for each customer and product. The fact emits:

- Surrogate key (`order_sk`).
- Conformed dimensions (`date_sk`, `customer_sk`, `product_sk`).
- Degenerate dimensions (`order_id`, `payment_id`, `shipment_id`).
- Measures (`order_amount`, `shipment_eta_days`).
- Bronze tracebacks (`bronze_partition`, `bronze_offset`).

### Inventory Position

[`build_fact_inventory_position`](../../infra/airflow/processing/spark/jobs/silver/builders/fact_inventory_position.py) treats each inventory change as a transactional fact. Temporal joins attach the correct warehouse and product version. Rows land with `date_sk` and preserved offsets so supply-chain teams can reconcile history without reruns.

### Customer Engagement

[`build_fact_customer_engagement`](../../infra/airflow/processing/spark/jobs/silver/builders/fact_customer_engagement.py) aggregates interactions per user, product, and day. Joins keyed on `last_interaction_ts` honour the customer and product state active when the engagement happened.

---

## Maintenance Is Part of the Build

Silver tables live on Iceberg, so every run ends with `OPTIMIZE`, `EXPIRE SNAPSHOTS`, and `REMOVE ORPHANS`. Treat maintenance as part of extraction, not an afterthought.

---

## Hands-on Kata

Run the loop end to end until it feels automatic.

**Baseline loop**
1. Start the stack – `docker compose --profile core --profile airflow --profile datagen up -d`.
2. Feel Bronze – in Airflow, unpause `bronze_events_kafka_stream` and trigger a run. Watch the available-now Spark job pull Kafka offsets while checkpoints advance in MinIO; query `iceberg.bronze.raw_events` in Trino to inspect raw payloads plus provenance.
3. Enable Silver – unpause `silver_retail_service` or run `python infra/airflow/processing/spark/jobs/silver_retail_service.py --tables all`. Dimensions build first, facts second, as soon as Bronze publishes fresh Iceberg snapshots.
4. Verify outputs – once tasks settle, query `SELECT * FROM iceberg.silver.fact_order_service ORDER BY order_ts DESC LIMIT 20;` and inspect `iceberg.silver.dim_product_catalog` / `iceberg.silver.fact_inventory_position` to check SCD2 ranges and trace facts back to Bronze offsets.

## Metrics the Silver Layer Unlocks

Silver facts and dimensions power Gold analytics:

- **Revenue cadence** – daily or weekly `SUM(order_amount)` sliced by product category, supplier, or customer segment.
- **Retention funnels** – join `fact_order_service` and `fact_customer_engagement` to measure how interactions convert to orders across cohorts.
- **Inventory health** – track `AVG(new_qty)` and `SUM(quantity_delta)` per warehouse to flag stockouts before they bite.
- **Supplier performance** – blend `dim_supplier.rating` with on-time shipment metrics from `fact_order_service` for vendor scorecards.
- **Customer lifetime value** – pair Silver facts with `dim_customer_profile.lifetime_value` snapshots for nuanced forecasting.

Because surrogate keys unify the story, these metrics stay accurate even when upstream systems rename IDs or shuffle segments.

---

## Common Failure Modes and How Silver Stops Them

- Natural keys in facts → Surrogate keys break coupling to volatile IDs.
- “Latest” dimension joins → Temporal predicates guarantee historical accuracy.
- Silent duplicates → Primary-key enforcement surfaces issues at write time, not weeks later.
- Forgotten audits → Bronze offsets remain attached, so replay is provable.

Silver is not glamorous, but it prevents 2 a.m. incidents.

---

## When to Outgrow This Pattern

The Spark builders in Data Forge are ideal for a small warehouse or a single-domain star schema: you control every transformation, understand the checkpoints, and practice medallion discipline without extra infrastructure.

### Advantages

- Minimal moving parts – Airflow + Spark + Iceberg, all portable to a laptop or modest cluster.
- Full transparency – every builder is Python you can step through, so debugging is quick.
- Flexible orchestration – add or rerun specific tables by updating the registry.

### Limitations

- Manual dependency management – surrogate keys, constraints, and notebook verification are on you.
- Limited ergonomics for large teams – no built-in semantic models, lineage graphs, or templated testing.
- Scaling pressure – dozens of dimensions and facts mean larger codebases and longer Spark submits per DAG.

### Alternatives for Scale

When Silver grows to dozens of tables or multiple teams, layer in specialized tooling:

- **dbt Core/Cloud** – SQL-first modeling with tests, docs, and environment promotion; great for analyst collaboration, less flexible for complex CDC joins.
- **Dagster or Prefect** – asset-based orchestration with lineage and type checks; more overhead than Airflow but stronger observability for sprawling DAGs.
- **Delta Live Tables / Databricks Workflows** – managed medallion pipelines with auto-scaling clusters and quality rules; sacrifice local portability for platform integration.
- **Flink or Materialize** – continuous computation when latency SLAs demand always-on processing instead of Airflow-triggered Spark batches.

Treat the Data Forge implementation as a kata. When scale or SLAs demand more, graduate to the platform that fits while carrying forward the discipline you learned here.

---

## Key Concepts: `_nk` and `_sk`

- Columns ending with `_sk` are **surrogate keys**. They are deterministic hashes or sequences generated in Silver and never exposed upstream. Facts carry them so joins remain stable even if natural identifiers change or collide.
- Columns ending with `_nk` are **natural keys**. They are human-readable anchors like `customer_nk` or `product_nk` that trace a surrogate through CDC tombstones, identifier recycling, and quiet upstream fixes.

Keeping both gives analysts durable joins and engineers transparent lineage back to the source systems.

---

## What’s Next

- Gold metrics surfaced in ClickHouse and visualised in Superset so KPIs stay consistent across teams.

Practice the discipline. The forge makes the blade.

---

> “Discipline replaces guesswork. Surrogate keys replace stories. That is the Silver layer.”
