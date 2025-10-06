"""Shared utilities for Silver table builders."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.column import Column
from pyspark.sql.window import Window


def with_payload(df: DataFrame) -> DataFrame:
    """Attach a parsed `payload` column derived from `json_payload`."""
    spark = df.sparkSession
    sample_df = (
        df.select("json_payload")
        .where(F.col("json_payload").isNotNull())
        .limit(1)
    )

    if sample_df.rdd.isEmpty():
        return df.withColumn("payload", F.lit(None).cast(T.StructType([])))

    schema = spark.read.json(sample_df.rdd.map(lambda row: row.json_payload)).schema
    return df.withColumn("payload", F.from_json("json_payload", schema))


def parse_bronze_topic(raw_events: DataFrame, topic: str) -> DataFrame:
    """Filter Bronze raw events by topic - no payload parsing for memory efficiency."""
    return raw_events.filter(F.col("event_source") == topic)


def parse_cdc_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a Bronze CDC Iceberg table - no payload parsing for memory efficiency."""
    return spark.table(table)


def unix_ms_to_ts(col: F.Column) -> F.Column:
    """Convert milliseconds since epoch to Spark TIMESTAMP."""

    return F.from_unixtime(col / F.lit(1000.0)).cast("timestamp")


def scd2_from_events(
    events: DataFrame,
    key_cols: list[str] | tuple[str, ...],
    ordering_cols: list[str] | tuple[str, ...],
    state_cols: list[str] | tuple[str, ...],
) -> DataFrame:
    """Derive SCD2 rows by detecting state changes in event streams."""
    order_window = Window.partitionBy(*key_cols).orderBy(*ordering_cols)
    state_hash = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(col).cast("string"), F.lit("")) for col in state_cols]),
        256
    )
    deltas = (events
        .withColumn("state_hash", state_hash)
        .withColumn("prev_state", F.lag("state_hash").over(order_window))
        .filter((F.col("prev_state").isNull()) | (F.col("prev_state") != F.col("state_hash")))
    )
    return (deltas
        .withColumn("valid_to", 
            F.coalesce(
                F.lead(ordering_cols[0]).over(order_window),
                F.lit("2999-12-31 23:59:59")
            ).cast("timestamp"))
        .drop("state_hash", "prev_state")
    )


def surrogate_key(*cols: Column) -> Column:
    """Generate a deterministic surrogate key using Spark's xxhash64."""

    return F.abs(F.xxhash64(*cols))
