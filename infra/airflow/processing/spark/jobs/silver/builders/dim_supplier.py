"""Supplier dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, surrogate_key, unix_ms_to_ts


def build_dim_supplier(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    suppliers = parse_cdc_table(spark, "iceberg.bronze.demo_public_suppliers").select(
        F.col("payload.after.supplier_id").alias("supplier_id"),
        F.col("payload.after.name").alias("name"),
        F.col("payload.after.country").alias("country"),
        F.col("payload.after.rating").alias("rating_raw"),
        unix_ms_to_ts(F.col("payload.ts_ms")).alias("valid_from"),
    )

    suppliers = suppliers.withColumn("rating", F.col("rating_raw").cast("double")).drop("rating_raw")

    latest = suppliers.withColumn(
        "rn", F.row_number().over(Window.partitionBy("supplier_id").orderBy(F.desc("valid_from")))
    ).filter(F.col("rn") == 1)

    return latest.drop("rn").select(
        surrogate_key(F.col("supplier_id")).alias("supplier_sk"),
        F.col("supplier_id").alias("supplier_nk"),
        "supplier_id",
        "name",
        "country",
        "rating",
        "valid_from",
        F.lit("2999-12-31 23:59:59").cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("processed_at"),
    )
