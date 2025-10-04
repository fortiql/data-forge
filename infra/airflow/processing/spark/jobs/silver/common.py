"""Shared utilities for Silver table builders."""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.column import Column
from pyspark.sql.window import Window


SchemaCacheKey = str
_SCHEMA_CACHE: Dict[SchemaCacheKey, T.StructType] = {}


def _infer_json_schema(df: DataFrame, cache_key: SchemaCacheKey) -> T.StructType:
    """Infer (and cache) the schema of the `json_payload` column."""

    if cache_key in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[cache_key]

    sample = (
        df.select("json_payload")
        .where(F.col("json_payload").isNotNull())
        .limit(1)
        .collect()
    )
    if not sample:
        schema = T.StructType([])
    else:
        spark = df.sparkSession
        schema = spark.read.json(
            spark.sparkContext.parallelize([sample[0]["json_payload"]])
        ).schema

    _SCHEMA_CACHE[cache_key] = schema
    return schema


def with_payload(df: DataFrame, cache_key: SchemaCacheKey) -> DataFrame:
    """Attach a parsed `payload` column derived from `json_payload`."""

    schema = _infer_json_schema(df, cache_key)
    if len(schema) == 0:
        return df.withColumn("payload", F.lit(None).cast(T.StructType([])))
    return df.withColumn("payload", F.from_json("json_payload", schema))


def parse_bronze_topic(raw_events: DataFrame, topic: str) -> DataFrame:
    """Filter Bronze raw events by topic and decode the payload."""

    return with_payload(raw_events.filter(F.col("event_source") == topic), f"bronze::{topic}")


def parse_cdc_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a Bronze CDC Iceberg table and decode its payload."""

    df = spark.table(table)
    return with_payload(df, f"cdc::{table}")


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
    hashed = events.withColumn(
        "state_hash",
        F.sha2(
            F.concat_ws(
                "||", *[F.coalesce(F.col(col).cast("string"), F.lit("")) for col in state_cols]
            ),
            256,
        ),
    )

    deltas = hashed.withColumn("prev_state", F.lag("state_hash").over(order_window)).filter(
        (F.col("prev_state").isNull()) | (F.col("prev_state") != F.col("state_hash"))
    )

    lead_window = Window.partitionBy(*key_cols).orderBy(*ordering_cols)
    return deltas.withColumn(
        "valid_to",
        F.coalesce(
            F.lead(ordering_cols[0]).over(lead_window),
            F.lit("2999-12-31 23:59:59"),
        ).cast("timestamp"),
    ).drop("state_hash", "prev_state")


def surrogate_key(*cols: Column) -> Column:
    """Generate a deterministic surrogate key using Spark's xxhash64."""

    return F.abs(F.xxhash64(*cols))
