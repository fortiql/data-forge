"""Customer engagement fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from silver.common import parse_bronze_topic, surrogate_key


def build_fact_customer_engagement(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_customer_engagement")
    interactions = (parse_bronze_topic(raw_events, "customer-interactions.v1")
        .select(
            F.get_json_object("json_payload", "$.interaction_id").alias("interaction_id"),
            F.get_json_object("json_payload", "$.user_id").alias("user_id"),
            F.get_json_object("json_payload", "$.product_id").alias("product_id"),
            F.get_json_object("json_payload", "$.interaction_type").alias("interaction_type"),
            F.get_json_object("json_payload", "$.duration_ms").cast("long").alias("duration_ms"),
            F.to_timestamp(F.get_json_object("json_payload", "$.ts")).alias("interaction_ts"),
            "event_time",
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        )
        .filter(
            F.col("interaction_id").isNotNull() & 
            F.col("user_id").isNotNull() & 
            F.col("product_id").isNotNull()
        )
        .withColumn("event_date", F.to_date("interaction_ts"))
    )
    aggregated = (interactions
        .repartition(400, "user_id", "product_id")
        .groupBy("user_id", "product_id", "event_date")
        .agg(
            F.countDistinct("interaction_id").alias("interactions"),
            F.sum(F.when(F.col("interaction_type") == "CART_ADD", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("interaction_type") == "CART_REMOVE", 1).otherwise(0)).alias("cart_removes"),
            F.sum(F.when(F.col("interaction_type") == "REVIEW", 1).otherwise(0)).alias("reviews"),
            F.sum("duration_ms").alias("total_duration_ms"),
            F.max("interaction_ts").alias("last_interaction_ts"),
            F.max(F.struct("interaction_ts", "bronze_partition", "bronze_offset", "event_time")).alias("latest_context"),
        )
        .withColumn("latest_bronze_partition", F.col("latest_context.bronze_partition"))
        .withColumn("latest_bronze_offset", F.col("latest_context.bronze_offset"))
        .withColumn("latest_event_time", F.col("latest_context.event_time"))
        .drop("latest_context")
        .coalesce(200)
    )
    customers = (spark.table("iceberg.silver.dim_customer_profile")
        .select("customer_sk", "user_id", "valid_from", "valid_to")
        .alias("cust")
    )

    products = (spark.table("iceberg.silver.dim_product_catalog")
        .select("product_sk", "product_id", "valid_from", "valid_to")
        .alias("prod")
    )

    dates = (spark.table("iceberg.silver.dim_date")
        .select("date_sk", "date_key")
        .alias("d")
    )

    aggregated = aggregated.alias("agg")

    enriched_fact = (aggregated
        .join(
            customers,
            (F.col("agg.user_id") == F.col("cust.user_id"))
            & (F.col("agg.last_interaction_ts") >= F.col("cust.valid_from"))
            & (F.col("agg.last_interaction_ts") < F.col("cust.valid_to")),
            "left",
        )
        .join(
            products,
            (F.col("agg.product_id") == F.col("prod.product_id"))
            & (F.col("agg.last_interaction_ts") >= F.col("prod.valid_from"))
            & (F.col("agg.last_interaction_ts") < F.col("prod.valid_to")),
            "left",
        )
        .join(dates, F.col("agg.event_date") == F.col("d.date_key"), "left")
        .withColumn("processed_at", F.current_timestamp())
    )

    return (enriched_fact
        .filter(
            F.col("agg.user_id").isNotNull() & F.col("agg.product_id").isNotNull()
        )
        .withColumn(
            "engagement_sk",
            surrogate_key(
                F.col("agg.user_id"),
                F.col("agg.product_id"),
                F.col("agg.event_date"),
                F.col("agg.latest_bronze_offset"),
            ),
        )
        .select(
            "engagement_sk",
            F.col("d.date_sk").alias("date_sk"),
            F.col("cust.customer_sk").alias("customer_sk"),
            F.col("prod.product_sk").alias("product_sk"),
            F.col("agg.interactions").alias("interactions"),
            F.col("agg.cart_adds").alias("cart_adds"),
            F.col("agg.cart_removes").alias("cart_removes"),
            F.col("agg.reviews").alias("reviews"),
            F.col("agg.total_duration_ms").alias("total_duration_ms"),
            F.col("agg.last_interaction_ts").alias("last_interaction_ts"),
            F.col("agg.latest_bronze_partition").alias("latest_bronze_partition"),
            F.col("agg.latest_bronze_offset").alias("latest_bronze_offset"),
            "processed_at",
        )
    )
