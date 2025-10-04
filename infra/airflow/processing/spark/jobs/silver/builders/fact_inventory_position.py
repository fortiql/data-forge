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
    )

    latest = changes.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("warehouse_id", "product_id").orderBy(F.desc("change_ts"))
        ),
    ).filter(F.col("rn") == 1)

    latest = latest.drop("rn")

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
        "processed_at",
    )
