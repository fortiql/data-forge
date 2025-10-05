"""Customer engagement fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from silver.common import parse_bronze_topic, surrogate_key


def build_fact_customer_engagement(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_customer_engagement")

    # Extract interaction events from json_payload
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

    # Aggregate interactions by user, product, and date with memory optimization
    aggregated = (interactions
        .repartition(400, "user_id", "product_id")  # Increase partitions for better parallelism
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
        .coalesce(200)  # Reduce partitions after aggregation
    )

    # Lookup dimension keys
    customers = spark.table("iceberg.silver.dim_customer_profile").select(
        "customer_sk", "user_id", "valid_from", "valid_to"
    )
    
    products = (spark.table("iceberg.silver.dim_product_catalog")
        .where(F.col("is_current"))
        .select("product_sk", "product_id")
    )

    # Lookup date dimension
    dates = spark.table("iceberg.silver.dim_date").select("date_sk", "date_key")
    
    # Join with dimensions using SCD2 temporal join for customers
    enriched_fact = (aggregated
        .join(customers, 
            (aggregated.user_id == customers.user_id) & 
            (aggregated.last_interaction_ts >= customers.valid_from) & 
            (aggregated.last_interaction_ts < customers.valid_to), "left")
        .join(products, aggregated.product_id == products.product_id, "left")
        .join(dates, aggregated.event_date == dates.date_key, "left")
        .withColumn("processed_at", F.current_timestamp())
        .drop(customers.user_id, products.product_id, dates.date_key)  # Remove duplicate columns
        .distinct()  # Ensure no duplicates from joins
    )

    # Generate unique fact surrogate key using business keys plus context
    return (enriched_fact
        .filter(F.col("user_id").isNotNull() & F.col("product_id").isNotNull())  # Filter nulls
        .withColumn("engagement_sk", surrogate_key(
            F.col("user_id"), 
            F.col("product_id"), 
            F.col("event_date"),
            F.col("latest_bronze_offset")  # Add offset for uniqueness
        ))
        .select(
            "engagement_sk",      # Fact surrogate key
            "date_sk",            # Date dimension FK
            "customer_sk",        # Customer dimension FK
            "product_sk",         # Product dimension FK  
            "interactions",       # Additive measures
            "cart_adds",
            "cart_removes", 
            "reviews",
            "total_duration_ms",
            "last_interaction_ts", # Semi-additive measure
            "latest_bronze_partition",  # Audit fields (standardized naming)
            "latest_bronze_offset",
            "processed_at",
        )
    )
