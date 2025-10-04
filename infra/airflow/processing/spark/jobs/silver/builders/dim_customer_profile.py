"""Customer profile dimension builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_cdc_table, scd2_from_events, surrogate_key, unix_ms_to_ts


def build_dim_customer_profile(spark: SparkSession, _: DataFrame | None) -> DataFrame:
    users_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_users")
    segments_raw = parse_cdc_table(spark, "iceberg.bronze.demo_public_customer_segments")

    users = users_raw.select(
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.user_id")
        ).alias("user_id"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.email")
        ).alias("email"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.country")
        ).alias("country"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.created_at")
        ).alias("created_at_raw"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.ts_ms").isNotNull(),
            unix_ms_to_ts(F.col("payload.ts_ms"))
        ).alias("change_ts"),
        "event_time",
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
        F.lit("users").alias("change_source"),
    ).filter(F.col("user_id").isNotNull())

    users = users.withColumn("created_at", F.to_timestamp("created_at_raw")).drop("created_at_raw")

    segments = segments_raw.select(
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.user_id")
        ).alias("user_id"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.segment")
        ).alias("segment"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.after").isNotNull(),
            F.col("payload.after.lifetime_value")
        ).alias("lifetime_value_raw"),
        F.when(
            F.col("payload").isNotNull() & F.col("payload.ts_ms").isNotNull(),
            unix_ms_to_ts(F.col("payload.ts_ms"))
        ).alias("change_ts"),
        "event_time",
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
        F.lit("customer_segments").alias("change_source"),
    ).filter(F.col("user_id").isNotNull())

    segments = segments.withColumn("lifetime_value", F.col("lifetime_value_raw").cast("double")).drop(
        "lifetime_value_raw"
    )

    combined = users.unionByName(
        segments.select(
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
        ),
        allowMissingColumns=True,
    )

    # First, consolidate multiple changes at the same timestamp to avoid duplicates
    consolidated = (
        combined.groupBy("user_id", "change_ts")
        .agg(
            F.last("email", ignorenulls=True).alias("email"),
            F.last("country", ignorenulls=True).alias("country"),
            F.last("created_at", ignorenulls=True).alias("created_at"),
            F.last("segment", ignorenulls=True).alias("segment"),
            F.last("lifetime_value", ignorenulls=True).alias("lifetime_value"),
            F.last("event_time", ignorenulls=True).alias("event_time"),
            # Use max bronze_offset for deterministic lineage when multiple changes at same timestamp
            F.max("bronze_offset").alias("bronze_offset"),
            F.max("bronze_partition").alias("bronze_partition"),
            F.concat_ws(",", F.collect_set("change_source")).alias("change_source"),
        )
    )

    fill_window = Window.partitionBy("user_id").orderBy("change_ts", "bronze_offset")
    filled = (
        consolidated.withColumn("email", F.last("email", ignorenulls=True).over(fill_window))
        .withColumn("country", F.last("country", ignorenulls=True).over(fill_window))
        .withColumn("created_at", F.last("created_at", ignorenulls=True).over(fill_window))
        .withColumn("segment", F.last("segment", ignorenulls=True).over(fill_window))
        .withColumn("lifetime_value", F.last("lifetime_value", ignorenulls=True).over(fill_window))
        .withColumn(
            "valid_from",
            F.coalesce("change_ts", "event_time", F.col("created_at")),
        )
        .filter(F.col("valid_from").isNotNull())
    )

    staged = filled.select(
        "user_id",
        "email",
        "country",
        "created_at",
        "segment",
        "lifetime_value",
        "valid_from",
        "bronze_partition",
        "bronze_offset",
        "change_source",
    )

    scd = scd2_from_events(
        staged,
        key_cols=["user_id"],
        ordering_cols=["valid_from", "bronze_offset"],
        state_cols=["email", "country", "segment", "lifetime_value"],
    )

    dimensional = scd.withColumn(
        "is_current",
        F.col("valid_to") == F.lit("2999-12-31 23:59:59").cast("timestamp"),
    ).withColumn(
        "processed_at", F.current_timestamp()
    )

    # Generate surrogate key using business key + valid_from + bronze_offset for deterministic uniqueness
    return dimensional.withColumn(
        "customer_sk", surrogate_key(F.col("user_id"), F.col("valid_from"), F.col("bronze_offset"))
    ).select(
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
