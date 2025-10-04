"""Warehouse dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_warehouse(spark: SparkSession, _: DataFrame | None) -> DataFrame:
        # Parse warehouse changes from CDC stream
    warehouses = parse_cdc_table(spark, "iceberg.bronze.demo_public_warehouses").filter(
        F.col("payload").isNotNull() & F.col("payload.after").isNotNull()
    ).select(
        F.col("payload.after.warehouse_id").alias("warehouse_id"),
        F.col("payload.after.name").alias("name"),
        F.col("payload.after.city").alias("city"),
        F.col("payload.after.state").alias("state"),
        F.col("payload.after.country").alias("country"),
        unix_ms_to_ts(F.col("payload.ts_ms")).alias("change_ts"),
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
    ).filter(F.col("warehouse_id").isNotNull())

    # Aggregate inventory metrics per warehouse per change event
    warehouse_stats = (
        warehouse_inv.groupBy("warehouse_id", "change_ts")
        .agg(
            F.sum("qty").alias("total_qty"),
            F.sum("reserved_qty").alias("total_reserved"),
            # Use max bronze_offset for deterministic lineage within the same timestamp
            F.max("bronze_offset").alias("bronze_offset"),
            F.max("bronze_partition").alias("bronze_partition"),
        )
        .withColumn(
            "inventory_turnover",
            F.when(F.col("total_reserved") == 0, F.lit(None)).otherwise(
                F.col("total_qty") / F.col("total_reserved")
            ),
        )
    )

    # Apply SCD Type 2 logic to track warehouse capacity changes over time
    scd2_warehouses = scd2_from_events(
        warehouse_stats,
        key_cols=["warehouse_id"],
        ordering_cols=["change_ts", "bronze_offset"],
        state_cols=["total_qty", "total_reserved", "inventory_turnover"]
    )

    # Add dimension attributes and rename change_ts to valid_from for clarity
    dimensional = scd2_warehouses.withColumn(
        "valid_from", F.col("change_ts")
    ).withColumn(
        "is_current",
        F.col("valid_to") == F.lit("2999-12-31 23:59:59").cast("timestamp")
    ).withColumn(
        "processed_at", F.current_timestamp()
    )

    # Generate surrogate key using business key + valid_from + bronze_offset for deterministic uniqueness
    return dimensional.withColumn(
        "warehouse_sk", surrogate_key(
            F.col("warehouse_id"), 
            F.col("valid_from"),
            F.col("bronze_offset")
        )
    ).select(
        "warehouse_sk",
        F.col("warehouse_id").alias("warehouse_nk"),
        "warehouse_id",
        "total_qty",
        "total_reserved", 
        "inventory_turnover",
        "valid_from",
        "valid_to",
        "is_current",
        "bronze_partition",
        "bronze_offset",
        "processed_at",
    )
