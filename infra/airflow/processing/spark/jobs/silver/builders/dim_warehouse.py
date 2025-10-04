"""Warehouse dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_warehouse(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    # Parse warehouse metadata
    warehouses = (parse_cdc_table(spark, "iceberg.bronze.demo_public_warehouses")
        .filter(F.col("payload.after").isNotNull())
        .select(
            F.col("payload.after.warehouse_id").alias("warehouse_id"),
            F.col("payload.after.name").alias("name"),
            F.col("payload.after.region").alias("region"),
            F.col("payload.after.country").alias("country"),
            unix_ms_to_ts(F.col("payload.ts_ms")).alias("change_ts"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("warehouse_id").isNotNull())
    )

    # Parse warehouse inventory changes
    warehouse_inv = (parse_cdc_table(spark, "iceberg.bronze.demo_public_warehouse_inventory")
        .filter(F.col("payload.after").isNotNull())
        .select(
            F.col("payload.after.warehouse_id").alias("warehouse_id"),
            F.col("payload.after.qty").alias("qty"),
            F.col("payload.after.reserved_qty").alias("reserved_qty"),
            unix_ms_to_ts(F.col("payload.ts_ms")).alias("change_ts"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("warehouse_id").isNotNull())
    )

    # Aggregate inventory metrics per warehouse
    warehouse_stats = (warehouse_inv
        .groupBy("warehouse_id", "change_ts")
        .agg(
            F.sum("qty").alias("total_qty"),
            F.sum("reserved_qty").alias("total_reserved"),
            F.max("bronze_offset").alias("bronze_offset"),
            F.max("bronze_partition").alias("bronze_partition"),
        )
        .withColumn("inventory_turnover", 
            F.when(F.col("total_reserved") == 0, F.lit(None))
             .otherwise(F.col("total_qty") / F.col("total_reserved")))
    )

    # Get latest warehouse metadata per warehouse
    latest_metadata = (warehouses
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("warehouse_id").orderBy(F.desc("change_ts"))))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("warehouse_id", "name", "region", "country")
    )
    
    # Combine inventory stats with warehouse metadata
    combined = warehouse_stats.join(latest_metadata, "warehouse_id", "left")

    # Apply SCD2 logic to track warehouse capacity changes
    scd = scd2_from_events(
        combined,
        key_cols=["warehouse_id"],
        ordering_cols=["change_ts", "bronze_offset"],
        state_cols=["name", "region", "country", "total_qty", "total_reserved", "inventory_turnover"]
    )

    # Add dimension attributes and surrogate key
    return (scd
        .withColumn("valid_from", F.col("change_ts"))
        .withColumn("is_current", F.col("valid_to") == F.lit("2999-12-31 23:59:59").cast("timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("warehouse_sk", surrogate_key(F.col("warehouse_id"), F.col("valid_from"), F.col("bronze_offset")))
        .select(
            "warehouse_sk",
            F.col("warehouse_id").alias("warehouse_nk"),
            "warehouse_id",
            "name",
            "region", 
            "country",
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
    )
