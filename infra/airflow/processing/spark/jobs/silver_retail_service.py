#!/usr/bin/env python3
"""Silver layer builder dispatcher for the Data Forge retail demo."""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_utils import build_spark
from silver import BUILDER_MAP, TABLE_BUILDERS, TableBuilder


APP_NAME = "silver_retail_service"

logger = logging.getLogger(__name__)


def write_snapshot(df: DataFrame, builder: TableBuilder) -> None:
    writer = df.writeTo(builder.table).using("iceberg")
    for column in builder.partition_cols:
        writer = writer.partitionedBy(F.col(column))
    writer.createOrReplace()


def enforce_primary_key(df: DataFrame, keys: Sequence[str], table_name: str) -> None:
    dup_count = (
        df.groupBy(*[F.col(key) for key in keys])
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if dup_count > 0:
        raise ValueError(f"Primary key violation detected for {table_name}")


def materialise_tables(spark: SparkSession, selected: Iterable[str]) -> None:
    order = [builder.identifier for builder in TABLE_BUILDERS]
    selected_ordered = [identifier for identifier in order if identifier in selected]

    raw_events: DataFrame | None = None

    for identifier in selected_ordered:
        builder = BUILDER_MAP[identifier]
        if builder.requires_raw_events and raw_events is None:
            logger.info("Loading raw_events table for fact builders")
            raw_events = spark.table("iceberg.bronze.raw_events")

        logger.info("Building table %s -> %s", builder.identifier, builder.table)
        df = builder.build_fn(spark, raw_events)
        enforce_primary_key(df, builder.primary_key, builder.table)
        write_snapshot(df, builder)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Silver dimensions and facts")
    parser.add_argument(
        "--tables",
        default="all",
        help="Comma-separated list of table identifiers to build (default: all)",
    )
    return parser.parse_args()


def resolve_selection(selection: str) -> set[str]:
    if selection.lower() == "all":
        return set(BUILDER_MAP)

    requested = {item.strip() for item in selection.split(",") if item.strip()}
    unknown = requested - set(BUILDER_MAP)
    if unknown:
        raise ValueError(f"Unknown table identifiers: {', '.join(sorted(unknown))}")
    return requested


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    selected = resolve_selection(args.tables)

    if not selected:
        logger.info("No tables selected; exiting")
        return

    spark = build_spark(APP_NAME)
    spark.sparkContext.setLogLevel("INFO")

    materialise_tables(spark, selected)

    logger.info("Silver layer build completed: %s", ", ".join(sorted(selected)))


if __name__ == "__main__":
    main()
