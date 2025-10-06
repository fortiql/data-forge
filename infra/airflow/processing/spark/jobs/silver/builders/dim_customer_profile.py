"""Customer profile dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_customer_profile(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    users_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_users")
    segments_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_customer_segments")
    users = (users_raw
        .select(
            F.coalesce(
                F.get_json_object("json_payload", "$.after.user_id"),
                F.get_json_object("json_payload", "$.before.user_id"),
                F.get_json_object("json_payload", "$.user_id")
            ).alias("user_id"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.email"),
                F.get_json_object("json_payload", "$.before.email"),
                F.get_json_object("json_payload", "$.email")
            ).alias("email"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.country"),
                F.get_json_object("json_payload", "$.before.country"),
                F.get_json_object("json_payload", "$.country")
            ).alias("country"),
            F.to_timestamp(F.coalesce(
                F.get_json_object("json_payload", "$.after.created_at"),
                F.get_json_object("json_payload", "$.before.created_at"),
                F.get_json_object("json_payload", "$.created_at")
            )).alias("created_at"),
            F.coalesce(
                unix_ms_to_ts(F.get_json_object("json_payload", "$.ts_ms").cast("long")),
                F.col("event_time")
            ).alias("change_ts"),
            "event_time",
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
            F.lit("users").alias("change_source"),
        )
        .filter(F.col("user_id").isNotNull())
    )

    segments = (segments_raw
        .select(
            F.coalesce(
                F.get_json_object("json_payload", "$.after.user_id"),
                F.get_json_object("json_payload", "$.before.user_id"),
                F.get_json_object("json_payload", "$.user_id")
            ).alias("user_id"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.segment"),
                F.get_json_object("json_payload", "$.before.segment"),
                F.get_json_object("json_payload", "$.segment")
            ).alias("segment"),
            F.to_timestamp(F.coalesce(
                F.get_json_object("json_payload", "$.after.segment_created_at"),
                F.get_json_object("json_payload", "$.before.segment_created_at"),
                F.get_json_object("json_payload", "$.segment_created_at")
            )).alias("segment_created_at"),
            F.coalesce(
                F.get_json_object("json_payload", "$.after.lifetime_value"),
                F.get_json_object("json_payload", "$.before.lifetime_value"),
                F.get_json_object("json_payload", "$.lifetime_value")
            ).cast("double").alias("lifetime_value"),
            F.coalesce(
                unix_ms_to_ts(F.get_json_object("json_payload", "$.ts_ms").cast("long")),
                F.col("event_time")
            ).alias("change_ts"),
            "event_time",
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
            F.lit("customer_segments").alias("change_source"),
        )
        .filter(F.col("user_id").isNotNull())
    )
    segments_extended = segments.select(
        "user_id",
        F.lit(None).cast("string").alias("email"),
        F.lit(None).cast("string").alias("country"),
        F.lit(None).cast("timestamp").alias("created_at"),
        "segment",
        "lifetime_value",
        "change_ts",
        "event_time",
        "bronze_partition",
        "bronze_offset",
        "change_source",
    )

    combined = users.unionByName(segments_extended, allowMissingColumns=True)
    consolidated = (combined
        .groupBy("user_id", "change_ts")
        .agg(
            F.last("email", ignorenulls=True).alias("email"),
            F.last("country", ignorenulls=True).alias("country"),
            F.last("created_at", ignorenulls=True).alias("created_at"),
            F.last("segment", ignorenulls=True).alias("segment"),
            F.last("lifetime_value", ignorenulls=True).alias("lifetime_value"),
            F.last("event_time", ignorenulls=True).alias("event_time"),
            F.max("bronze_offset").alias("bronze_offset"),
            F.max("bronze_partition").alias("bronze_partition"),
            F.concat_ws(",", F.collect_set("change_source")).alias("change_source"),
        )
    )
    fill_window = Window.partitionBy("user_id").orderBy("change_ts", "bronze_offset").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    filled = (consolidated
        .repartition(200, "user_id")
        .withColumn("email", F.last("email", ignorenulls=True).over(fill_window))
        .withColumn("country", F.last("country", ignorenulls=True).over(fill_window))
        .withColumn("created_at", F.last("created_at", ignorenulls=True).over(fill_window))
        .withColumn("segment", F.last("segment", ignorenulls=True).over(fill_window))
        .withColumn("lifetime_value", F.last("lifetime_value", ignorenulls=True).over(fill_window))
        .withColumn("valid_from", F.coalesce("change_ts", "event_time", "created_at"))
        .filter(F.col("valid_from").isNotNull())
    )

    scd = scd2_from_events(
        filled,
        key_cols=["user_id"],
        ordering_cols=["valid_from", "bronze_offset"],
        state_cols=["email", "country", "segment", "lifetime_value"],
    )

    return (scd
        .withColumn("is_current", F.col("valid_to") == F.lit("2999-12-31 23:59:59").cast("timestamp"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("customer_sk", surrogate_key(F.col("user_id"), F.col("valid_from"), F.col("bronze_offset")))
        .select(
            "customer_sk",
            F.col("user_id").alias("customer_nk"),
            "user_id",
            "email",
            "country",
            "segment",
            "lifetime_value",
            "created_at",
            "valid_from",
            "valid_to",  
            "is_current",
            "change_source",
            "bronze_partition",
            "bronze_offset",
            "processed_at",
        )
    )
