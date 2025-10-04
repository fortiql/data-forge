"""Inventory position fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_bronze_topic


def build_fact_inventory_position(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_inventory_position")

    changes = parse_bronze_topic(raw_events, "inventory-changes.v1").select(
        F.col("payload.warehouse_id").alias("warehouse_id"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.change_type").alias("change_type"),
        F.col("payload.quantity_delta").alias("quantity_delta"),
        F.col("payload.new_qty").alias("new_qty"),
        F.col("payload.reason").alias("reason"),
        F.col("payload.order_id").alias("order_id"),
        F.to_timestamp("payload.ts").alias("change_ts"),
        "event_time",
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
    )

    # Get the truly latest record per warehouse/product combination
    # First, find the max timestamp per warehouse/product
    max_timestamps = changes.groupBy("warehouse_id", "product_id").agg(
        F.max("change_ts").alias("max_change_ts")
    )
    
    # Filter to records with max timestamp, then take max bronze_offset for deterministic selection
    latest_at_max_time = (
        changes.join(
            max_timestamps,
            (changes.warehouse_id == max_timestamps.warehouse_id) &
            (changes.product_id == max_timestamps.product_id) &
            (changes.change_ts == max_timestamps.max_change_ts)
        )
        .select(changes["*"])  # Select all columns from changes table
    )
    
    # If there are still multiple records at the same max timestamp, take the one with max bronze_offset
    latest = (
        latest_at_max_time.groupBy("warehouse_id", "product_id", "change_ts")
        .agg(
            F.first("change_type").alias("change_type"),
            F.first("quantity_delta").alias("quantity_delta"),
            F.first("new_qty").alias("new_qty"),
            F.first("reason").alias("reason"),
            F.first("order_id").alias("order_id"),
            F.first("event_time").alias("event_time"),
            F.max("bronze_offset").alias("bronze_offset"),
            F.max("bronze_partition").alias("bronze_partition"),
        )
    )

    warehouses = spark.table("iceberg.silver.dim_warehouse").select(
        "warehouse_sk",
        "warehouse_id",
    )
    products = spark.table("iceberg.silver.dim_product_catalog").where(F.col("is_current")).select(
        "product_sk",
        "product_id",
    )

    enriched = (
        latest.join(warehouses, "warehouse_id", "left")
        .join(products, "product_id", "left")
        .withColumn("processed_at", F.current_timestamp())
    )

    return enriched.select(
        "warehouse_id",
        "product_id",
        "warehouse_sk",
        "product_sk",
        "change_type",
        "quantity_delta",
        "new_qty",
        "reason",
        "order_id",
        "change_ts",
        "event_time",
        "bronze_partition",
        "bronze_offset",
        "processed_at",
    )
