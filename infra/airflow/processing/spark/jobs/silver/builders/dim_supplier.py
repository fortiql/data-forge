"""Supplier dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_supplier(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    suppliers_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_suppliers")
    
    suppliers = (suppliers_raw
        .select(
            F.coalesce(
                F.get_json_object("json_payload", "$.after.supplier_id"),
                F.get_json_object("json_payload", "$.before.supplier_id"),
                F.get_json_object("json_payload", "$.supplier_id")
            ).alias("supplier_id"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.name"),
                F.get_json_object("json_payload", "$.before.name"),
                F.get_json_object("json_payload", "$.name")
            ).alias("name"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.country"),
                F.get_json_object("json_payload", "$.before.country"),
                F.get_json_object("json_payload", "$.country")
            ).alias("country"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.rating"),
                F.get_json_object("json_payload", "$.before.rating"),
                F.get_json_object("json_payload", "$.rating")
            ).cast("double").alias("rating"),
            F.coalesce(
                unix_ms_to_ts(F.get_json_object("json_payload", "$.ts_ms").cast("long")),
                F.col("event_time")
            ).alias("change_ts"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("supplier_id").isNotNull())
    )
    scd = scd2_from_events(
        suppliers,
        key_cols=["supplier_id"],
        ordering_cols=["change_ts", "bronze_offset"],
        state_cols=["name", "country", "rating"]
    )
    return (scd
        .withColumn("valid_from", F.col("change_ts"))
        .withColumn("is_current", F.col("valid_to") == F.lit("2999-12-31 23:59:59").cast("timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("supplier_sk", surrogate_key(F.col("supplier_id"), F.col("valid_from"), F.col("bronze_offset")))
        .select(
            "supplier_sk",
            F.col("supplier_id").alias("supplier_nk"),
            "supplier_id",
            "name",
            "country",
            "rating",
            "valid_from",
            "valid_to",
            "is_current",
            "bronze_partition",
            "bronze_offset",
            "processed_at",
        )
    )
