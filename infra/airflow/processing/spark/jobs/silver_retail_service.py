#!/usr/bin/env python3
"""Silver layer builder for retail analytics.

Transforms Bronze raw events and CDC data into Silver dimensions and facts.
"""

import argparse
import logging
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_utils import build_spark, ensure_schema
from silver import BUILDER_MAP, TableBuilder

logger = logging.getLogger(__name__)


def write_snapshot(df: DataFrame, builder: TableBuilder) -> None:
    """Create or replace Iceberg table with partitioning."""
    writer = df.writeTo(builder.table).using("iceberg")  
    for column in builder.partition_cols:
        writer = writer.partitionedBy(F.col(column))
    writer.createOrReplace()
    
    logger.info("Table written: %s", builder.table)


def enforce_primary_key(df: DataFrame, keys: Sequence[str], table_name: str) -> None:
    """Validate primary key constraints before writing."""    
    dup_count = (
        df.groupBy(*[F.col(key) for key in keys])
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    
    if dup_count > 0:
        raise ValueError(f"Primary key violation in {table_name}: {dup_count} duplicates")


def materialise_tables(
    spark: SparkSession, 
    builders: Iterable[TableBuilder], 
    raw_events: DataFrame | None = None
) -> None:
    """Create or refresh Silver dimensional tables."""
    ensure_schema(spark, "iceberg.silver")
    
    for builder in builders:
        logger.info("Building %s...", builder.identifier)
        df = builder.build_fn(spark, raw_events)
        if "dim_" in builder.identifier and builder.identifier != "dim_customer_profile":
            df.cache()
            logger.info("Cached small dimension: %s", builder.identifier)
        if builder.primary_key:
            enforce_primary_key(df, builder.primary_key, builder.identifier)
        write_snapshot(df, builder)
        if df.is_cached:
            df.unpersist()
            logger.info("Unpersisted cache for: %s", builder.identifier)
        
        logger.info("Completed %s", builder.identifier)


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
    """Main entry point for Silver layer processing."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    
    args = parse_args()
    selected = resolve_selection(args.tables)

    if not selected:
        logger.info("No tables selected")
        return

    app_name = f"silver_retail:{':'.join(sorted(selected))}"
    logger.info("Starting %s with %d tables", app_name, len(selected))

    spark = build_spark(app_name=app_name)
    spark.sparkContext.setLogLevel("WARN")
    
    ensure_schema(spark, "iceberg.silver")

    try:
        selected_builders = [BUILDER_MAP[identifier] for identifier in selected]
        requires_events = any(builder.requires_raw_events for builder in selected_builders)
        raw_events = spark.table("iceberg.bronze.raw_events") if requires_events else None
        
        materialise_tables(spark, selected_builders, raw_events)
        logger.info("Silver job completed successfully")               
    except Exception as e:
        logger.error("Silver job failed: %s", str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
