"""Inventory position fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_bronze_topic, surrogate_key


def build_fact_inventory_position(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_inventory_position")

    # Extract inventory change events
    changes = (parse_bronze_topic(raw_events, "inventory-changes.v1")
        .select(
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
        .filter(F.col("warehouse_id").isNotNull() & F.col("product_id").isNotNull())
    )

    # Get latest position per warehouse/product
    latest = (changes
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("warehouse_id", "product_id")
                  .orderBy(F.desc("change_ts"), F.desc("bronze_offset"))))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Lookup dimension keys
    warehouses = spark.table("iceberg.silver.dim_warehouse").select("warehouse_sk", "warehouse_id")
    products = (spark.table("iceberg.silver.dim_product_catalog")
        .where(F.col("is_current"))
        .select("product_sk", "product_id")
    )

    # Join with dimensions and return Kimball-compliant fact table
    enriched = (latest
        .join(warehouses, latest.warehouse_id == warehouses.warehouse_id, "left")
        .join(products, latest.product_id == products.product_id, "left")
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("inventory_sk", surrogate_key(F.col("warehouse_sk"), F.col("product_sk"), F.col("change_ts")))
    )
    
    return (enriched
        .select(
            "inventory_sk",       # Fact surrogate key
            "warehouse_sk",       # Warehouse dimension FK
            "product_sk",         # Product dimension FK
            "change_type",        # Descriptive attributes
            "quantity_delta",     # Additive measure
            "new_qty",            # Semi-additive measure (balance)
            "reason",
            "order_id",           # Degenerate dimension
            "change_ts",          # Event timestamp
            "bronze_partition",   # Audit fields (standardized naming)
            "bronze_offset", 
            "processed_at",
        )
        .dropDuplicates(["warehouse_sk", "product_sk"])  # Ensure no duplicates on business key
    )
