"""Order service fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_bronze_topic


def build_fact_order_service(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_order_service")

    orders = parse_bronze_topic(raw_events, "orders.v1").select(
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.user_id").alias("user_id"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.amount").alias("order_amount"),
        F.col("payload.currency").alias("order_currency"),
        F.to_timestamp("payload.ts").alias("order_ts"),
        "event_time",
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
    )

    payments = parse_bronze_topic(raw_events, "payments.v1").select(
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.payment_id").alias("payment_id"),
        F.col("payload.method").alias("payment_method"),
        F.col("payload.status").alias("payment_status"),
        F.to_timestamp("payload.ts").alias("payment_ts"),
    )

    latest_payment = payments.withColumn(
        "rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.desc("payment_ts")))
    ).filter(F.col("rn") == 1)

    shipments = parse_bronze_topic(raw_events, "shipments.v1").select(
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.shipment_id").alias("shipment_id"),
        F.col("payload.carrier").alias("shipment_carrier"),
        F.col("payload.eta_days").alias("shipment_eta_days"),
        F.to_timestamp("payload.ts").alias("shipment_ts"),
    )

    latest_shipment = shipments.withColumn(
        "rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.desc("shipment_ts")))
    ).filter(F.col("rn") == 1)

    fact = (
        orders.join(latest_payment.drop("rn"), "order_id", "left")
        .join(latest_shipment.drop("rn"), "order_id", "left")
        .withColumn("order_ts", F.coalesce("order_ts", "event_time"))
        .withColumn("order_date", F.to_date("order_ts"))
    )

    customers = spark.table("iceberg.silver.dim_customer_profile").select(
        "customer_sk",
        "user_id",
        "valid_from",
        "valid_to",
    )

    products = spark.table("iceberg.silver.dim_product_catalog").where(F.col("is_current")).select(
        "product_sk",
        "product_id",
    )

    enriched = (
        fact.join(
            customers,
            (fact.user_id == customers.user_id)
            & (fact.order_ts >= customers.valid_from)
            & (fact.order_ts < customers.valid_to),
            "left",
        )
        .join(products, "product_id", "left")
        .withColumn("processed_at", F.current_timestamp())
    )

    return enriched.select(
        "order_id",
        "order_date",
        "order_ts",
        "order_amount",
        "order_currency",
        "payment_id",
        "payment_method",
        "payment_status",
        "payment_ts",
        "shipment_id",
        "shipment_carrier",
        "shipment_eta_days",
        "shipment_ts",
        "event_time",
        "bronze_partition",
        "bronze_offset",
        "user_id",
        "product_id",
        "customer_sk",
        "product_sk",
        "processed_at",
    )
