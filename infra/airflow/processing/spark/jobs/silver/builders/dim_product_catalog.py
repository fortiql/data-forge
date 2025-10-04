"""Product catalog dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, surrogate_key, unix_ms_to_ts


def build_dim_product_catalog(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    products = parse_cdc_table(spark, "iceberg.bronze.demo_public_products").select(
        F.col("payload.after.product_id").alias("product_id"),
        F.col("payload.after.title").alias("title"),
        F.col("payload.after.category").alias("category"),
        F.col("payload.after.price_usd").alias("price_raw"),
        unix_ms_to_ts(F.col("payload.ts_ms")).alias("change_ts"),
    )

    products = products.withColumn("price_usd", F.col("price_raw").cast("double")).drop("price_raw")

    inventory = parse_cdc_table(spark, "iceberg.bronze.demo_public_inventory").select(
        F.col("payload.after.product_id").alias("product_id"),
        F.col("payload.after.qty").alias("qty"),
        unix_ms_to_ts(F.col("payload.ts_ms")).alias("update_ts"),
    )

    latest_inventory = inventory.withColumn(
        "rn", F.row_number().over(Window.partitionBy("product_id").orderBy(F.desc("update_ts")))
    ).filter(F.col("rn") == 1)

    return products.join(latest_inventory.drop("rn"), "product_id", "left").select(
        surrogate_key(F.col("product_id")).alias("product_sk"),
        F.col("product_id").alias("product_nk"),
        "product_id",
        "title",
        "category",
        "price_usd",
        F.col("qty").alias("inventory_qty"),
        F.col("change_ts").alias("valid_from"),
        F.lit("2999-12-31 23:59:59").cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("processed_at"),
    )
