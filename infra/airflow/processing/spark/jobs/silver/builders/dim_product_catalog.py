"""Product catalog dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_product_catalog(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    products_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_products")
    
    products = (products_raw
        .select(
            F.coalesce(
                F.get_json_object("json_payload", "$.after.product_id"),
                F.get_json_object("json_payload", "$.before.product_id"),
                F.get_json_object("json_payload", "$.product_id")
            ).alias("product_id"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.title"),
                F.get_json_object("json_payload", "$.before.title"),
                F.get_json_object("json_payload", "$.title")
            ).alias("title"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.category"),
                F.get_json_object("json_payload", "$.before.category"),
                F.get_json_object("json_payload", "$.category")
            ).alias("category"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.price_usd"),
                F.get_json_object("json_payload", "$.before.price_usd"),
                F.get_json_object("json_payload", "$.price_usd")
            ).cast("double").alias("price_usd"),
            F.coalesce(
                unix_ms_to_ts(F.get_json_object("json_payload", "$.ts_ms").cast("long")),
                F.col("event_time")
            ).alias("change_ts"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("product_id").isNotNull())
    )
    scd = scd2_from_events(
        products,
        key_cols=["product_id"],
        ordering_cols=["change_ts", "bronze_offset"],
        state_cols=["title", "category", "price_usd"]
    )
    return (scd
        .withColumn("valid_from", F.col("change_ts"))
        .withColumn("is_current", F.col("valid_to") == F.lit("2999-12-31 23:59:59").cast("timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("product_sk", surrogate_key(F.col("product_id"), F.col("valid_from"), F.col("bronze_offset")))
        .select(
            "product_sk",
            F.col("product_id").alias("product_nk"),
            "product_id",
            "title",
            "category",
            "price_usd",
            "valid_from",
            "valid_to",
            "is_current",
            "bronze_partition",
            "bronze_offset",
            "processed_at",
        )
    )
