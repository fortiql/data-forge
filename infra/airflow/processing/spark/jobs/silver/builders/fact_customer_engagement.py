"""Customer engagement fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from silver.common import parse_bronze_topic


def build_fact_customer_engagement(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_customer_engagement")

    interactions = parse_bronze_topic(raw_events, "customer-interactions.v1").select(
        F.col("payload.interaction_id").alias("interaction_id"),
        F.col("payload.user_id").alias("user_id"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.interaction_type").alias("interaction_type"),
        F.col("payload.duration_ms").alias("duration_ms"),
        F.to_timestamp("payload.ts").alias("interaction_ts"),
        "event_time",
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
    )

    enriched = interactions.withColumn("event_date", F.to_date("interaction_ts"))

    aggregated = (
        enriched.groupBy("user_id", "product_id", "event_date")
        .agg(
            F.countDistinct("interaction_id").alias("interactions"),
            F.sum(F.when(F.col("interaction_type") == "CART_ADD", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("interaction_type") == "CART_REMOVE", 1).otherwise(0)).alias("cart_removes"),
            F.sum(F.when(F.col("interaction_type") == "REVIEW", 1).otherwise(0)).alias("reviews"),
            F.sum("duration_ms").alias("total_duration_ms"),
            F.max("interaction_ts").alias("last_interaction_ts"),
            # Preserve Bronze lineage from latest interaction for traceability
            F.max(F.struct("interaction_ts", "bronze_partition", "bronze_offset", "event_time"))
             .alias("latest_bronze_context"),
        )
        .withColumn("latest_bronze_partition", F.col("latest_bronze_context.bronze_partition"))
        .withColumn("latest_bronze_offset", F.col("latest_bronze_context.bronze_offset"))
        .withColumn("latest_event_time", F.col("latest_bronze_context.event_time"))
        .drop("latest_bronze_context")
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

    enriched_fact = (
        aggregated.join(
            customers,
            (aggregated.user_id == customers.user_id)
            & (aggregated.last_interaction_ts >= customers.valid_from)
            & (aggregated.last_interaction_ts < customers.valid_to),
            "left",
        )
        .join(products, "product_id", "left")
        .withColumn("processed_at", F.current_timestamp())
    )

    return enriched_fact.select(
        "user_id",
        "product_id",
        "event_date",
        "interactions",
        "cart_adds",
        "cart_removes",
        "reviews",
        "total_duration_ms",
        "last_interaction_ts",
        "latest_event_time",
        "latest_bronze_partition",
        "latest_bronze_offset",
        "customer_sk",
        "product_sk",
        "processed_at",
    )
