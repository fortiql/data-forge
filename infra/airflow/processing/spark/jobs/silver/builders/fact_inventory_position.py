"""Inventory position fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from silver.common import parse_bronze_topic, surrogate_key


def build_fact_inventory_position(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_inventory_position")

    changes = (parse_bronze_topic(raw_events, "inventory-changes.v1")
        .select(
            F.get_json_object("json_payload", "$.warehouse_id").alias("warehouse_id"),
            F.get_json_object("json_payload", "$.product_id").alias("product_id"),
            F.get_json_object("json_payload", "$.change_type").alias("change_type"),
            F.get_json_object("json_payload", "$.quantity_delta").cast("int").alias("quantity_delta"),
            F.get_json_object("json_payload", "$.new_qty").cast("int").alias("new_qty"),
            F.get_json_object("json_payload", "$.reason").alias("reason"),
            F.get_json_object("json_payload", "$.order_id").alias("order_id"),
            F.to_timestamp(F.get_json_object("json_payload", "$.ts")).alias("change_ts"),
            "event_time",
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(F.col("warehouse_id").isNotNull() & F.col("product_id").isNotNull())
        .withColumn("change_ts", F.coalesce(F.col("change_ts"), F.col("event_time")))
        .withColumn("event_date", F.to_date("change_ts"))
    )

    warehouses = (spark.table("iceberg.silver.dim_warehouse")
        .select("warehouse_sk", "warehouse_id", "valid_from", "valid_to")
        .alias("w")
    )

    products = (spark.table("iceberg.silver.dim_product_catalog")
        .select("product_sk", "product_id", "valid_from", "valid_to")
        .alias("p")
    )

    dates = (spark.table("iceberg.silver.dim_date")
        .select("date_sk", "date_key")
        .alias("d")
    )

    enriched = (changes.alias("c")
        .join(
            warehouses,
            (F.col("c.warehouse_id") == F.col("w.warehouse_id"))
            & (F.col("c.change_ts") >= F.col("w.valid_from"))
            & (F.col("c.change_ts") < F.col("w.valid_to")),
            "left",
        )
        .join(
            products,
            (F.col("c.product_id") == F.col("p.product_id"))
            & (F.col("c.change_ts") >= F.col("p.valid_from"))
            & (F.col("c.change_ts") < F.col("p.valid_to")),
            "left",
        )
        .join(dates, F.col("c.event_date") == F.col("d.date_key"), "left")
        .withColumn("processed_at", F.current_timestamp())
        .withColumn(
            "inventory_sk",
            surrogate_key(
                F.col("w.warehouse_sk"),
                F.col("p.product_sk"),
                F.col("c.change_ts"),
                F.col("c.bronze_offset"),
            ),
        )
    )

    return (enriched
        .select(
            "inventory_sk",
            F.col("d.date_sk").alias("date_sk"),
            F.col("w.warehouse_sk").alias("warehouse_sk"),
            F.col("p.product_sk").alias("product_sk"),
            F.col("c.change_type").alias("change_type"),
            F.col("c.quantity_delta").alias("quantity_delta"),
            F.col("c.new_qty").alias("new_qty"),
            F.col("c.reason").alias("reason"),
            F.col("c.order_id").alias("order_id"),
            F.col("c.change_ts").alias("change_ts"),
            F.col("c.bronze_partition").alias("bronze_partition"),
            F.col("c.bronze_offset").alias("bronze_offset"),
            "processed_at",
        )
    )
