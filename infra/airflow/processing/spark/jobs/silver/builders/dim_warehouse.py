"""Warehouse dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_warehouse(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    warehouses_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_warehouses")
    warehouses = (warehouses_raw
        .select(
            F.coalesce(
                F.get_json_object("json_payload", "$.after.warehouse_id"),
                F.get_json_object("json_payload", "$.before.warehouse_id"),
                F.get_json_object("json_payload", "$.warehouse_id")
            ).alias("warehouse_id"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.name"),
                F.get_json_object("json_payload", "$.before.name"),
                F.get_json_object("json_payload", "$.name")
            ).alias("name"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.region"),
                F.get_json_object("json_payload", "$.before.region"),
                F.get_json_object("json_payload", "$.region")
            ).alias("region"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.country"),
                F.get_json_object("json_payload", "$.before.country"),
                F.get_json_object("json_payload", "$.country")
            ).alias("country"),
            F.coalesce(
                unix_ms_to_ts(F.get_json_object("json_payload", "$.ts_ms").cast("long")),
                F.col("event_time")
            ).alias("change_ts"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("warehouse_id").isNotNull())
    )
    warehouse_inv_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_warehouse_inventory")
    warehouse_inv = (warehouse_inv_raw
        .select(
            F.coalesce(
                F.get_json_object("json_payload", "$.after.warehouse_id"),
                F.get_json_object("json_payload", "$.before.warehouse_id"),
                F.get_json_object("json_payload", "$.warehouse_id")
            ).alias("warehouse_id"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.qty"),
                F.get_json_object("json_payload", "$.before.qty"),
                F.get_json_object("json_payload", "$.qty")
            ).alias("qty"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.reserved_qty"),
                F.get_json_object("json_payload", "$.before.reserved_qty"),
                F.get_json_object("json_payload", "$.reserved_qty")
            ).alias("reserved_qty"),
            F.coalesce(
                unix_ms_to_ts(F.get_json_object("json_payload", "$.ts_ms").cast("long")),
                F.col("event_time")
            ).alias("change_ts"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("warehouse_id").isNotNull())
    )
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
    latest_metadata = (warehouses
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("warehouse_id").orderBy(F.desc("change_ts"))))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("warehouse_id", "name", "region", "country")
    )
    combined = warehouse_stats.join(latest_metadata, "warehouse_id", "left")
    scd = scd2_from_events(
        combined,
        key_cols=["warehouse_id"],
        ordering_cols=["change_ts", "bronze_offset"],
        state_cols=["name", "region", "country", "total_qty", "total_reserved", "inventory_turnover"]
    )
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
