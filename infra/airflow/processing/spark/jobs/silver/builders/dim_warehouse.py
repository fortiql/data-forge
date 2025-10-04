"""Warehouse dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from silver.common import parse_cdc_table, surrogate_key, unix_ms_to_ts


def build_dim_warehouse(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    warehouse_inv = parse_cdc_table(spark, "iceberg.bronze.demo_public_warehouse_inventory").select(
        F.col("payload.after.warehouse_id").alias("warehouse_id"),
        F.col("payload.after.product_id").alias("product_id"),
        F.col("payload.after.qty").alias("qty"),
        F.col("payload.after.reserved_qty").alias("reserved_qty"),
        unix_ms_to_ts(F.col("payload.ts_ms")).alias("change_ts"),
    )

    stats = (
        warehouse_inv.groupBy("warehouse_id")
        .agg(
            F.sum("qty").alias("total_qty"),
            F.sum("reserved_qty").alias("total_reserved"),
            F.max("change_ts").alias("last_change_ts"),
        )
        .withColumn(
            "inventory_turnover",
            F.when(F.col("total_reserved") == 0, F.lit(None)).otherwise(
                F.col("total_qty") / F.col("total_reserved")
            ),
        )
    )

    return stats.select(
        surrogate_key(F.col("warehouse_id")).alias("warehouse_sk"),
        F.col("warehouse_id").alias("warehouse_nk"),
        "warehouse_id",
        "total_qty",
        "total_reserved",
        "inventory_turnover",
        "last_change_ts",
        F.current_timestamp().alias("processed_at"),
    )
