#!/usr/bin/env python3
"""Silver layer builder for retail analytics.

Transforms Bronze raw events and CDC data into Silver dimensions and facts.
Each builder reads Bronze tables, applies business logic, and writes clean 
analytical datasets with surrogate keys and SCD2 history tracking.
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

from spark_utils import build_spark, ensure_schema
from silver import BUILDER_MAP, TABLE_BUILDERS, TableBuilder


APP_NAME_PREFIX = "silver_retail_service"

logger = logging.getLogger(__name__)


def write_snapshot(df: DataFrame, builder: TableBuilder) -> None:
    """Create or replace Iceberg table with partitioning."""
    start_time = time.time()
    logger.info("TABLE_WRITING | table=%s | mode=createOrReplace", builder.table)
    
    writer = df.writeTo(builder.table).using("iceberg")
    for column in builder.partition_cols:
        writer = writer.partitionedBy(F.col(column))
    writer.createOrReplace()
    
    duration = time.time() - start_time
    row_count = df.count()
    logger.info("TABLE_WRITTEN | table=%s | rows=%d | duration=%.2fs | status=success", 
               builder.table, row_count, duration)


def enforce_primary_key(df: DataFrame, keys: Sequence[str], table_name: str) -> None:
    """Validate primary key constraints before writing."""
    logger.info("PK_VALIDATION | table=%s | keys=[%s] | status=checking", 
               table_name, ", ".join(keys))
    
    dup_count = (
        df.groupBy(*[F.col(key) for key in keys])
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    
    if dup_count > 0:
        logger.error("PK_VIOLATION | table=%s | duplicates=%d | status=failed", 
                    table_name, dup_count)
        raise ValueError(f"Primary key violation detected for {table_name}")
    
    logger.info("PK_VALIDATION | table=%s | status=passed", table_name)


def materialise_tables(spark: SparkSession, selected: Iterable[str]) -> None:
    """Build selected Silver tables in dependency order."""
    order = [builder.identifier for builder in TABLE_BUILDERS]
    selected_ordered = [identifier for identifier in order if identifier in selected]
    
    logger.info("BUILD_PLAN | tables=[%s] | count=%d", 
               ", ".join(selected_ordered), len(selected_ordered))

    raw_events: DataFrame | None = None

    for identifier in selected_ordered:
        builder = BUILDER_MAP[identifier]
        table_start = time.time()
        
        logger.info("TABLE_BUILDING | identifier=%s | table=%s | requires_events=%s | status=starting", 
                   builder.identifier, builder.table, builder.requires_raw_events)
        
        # Load raw_events once for all fact builders
        if builder.requires_raw_events and raw_events is None:
            events_start = time.time()
            logger.info("EVENTS_LOADING | table=iceberg.bronze.raw_events | status=starting")
            raw_events = spark.table("iceberg.bronze.raw_events")
            events_duration = time.time() - events_start
            logger.info("EVENTS_LOADED | table=iceberg.bronze.raw_events | duration=%.2fs | status=success", 
                       events_duration)

        try:
            df = builder.build_fn(spark, raw_events)
            enforce_primary_key(df, builder.primary_key, builder.table)
            write_snapshot(df, builder)
            
            table_duration = time.time() - table_start
            logger.info("TABLE_COMPLETED | identifier=%s | table=%s | duration=%.2fs | status=success", 
                       builder.identifier, builder.table, table_duration)
                       
        except Exception as e:
            table_duration = time.time() - table_start
            logger.error("TABLE_FAILED | identifier=%s | table=%s | duration=%.2fs | error=%s | status=failed", 
                        builder.identifier, builder.table, table_duration, str(e))
            raise


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
    # Configure logging for Airflow-friendly output
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    args = parse_args()
    selected = resolve_selection(args.tables)

    if not selected:
        logger.info("JOB_SKIPPED | reason=no_tables_selected | status=completed")
        return

    app_name = f"{APP_NAME_PREFIX}:{':'.join(sorted(selected))}"
    logger.info("JOB_STARTING | app=%s | tables=[%s] | count=%d", 
               app_name, ", ".join(sorted(selected)), len(selected))

    spark = build_spark(app_name=app_name)
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark verbosity for cleaner Airflow logs

    # Ensure Silver schema exists before building tables
    ensure_schema(spark, "iceberg.silver")

    try:
        job_start = time.time()
        materialise_tables(spark, selected)
        job_duration = time.time() - job_start
        
        logger.info("JOB_SUCCESS | app=%s | tables=[%s] | duration=%.2fs | status=completed", 
                   app_name, ", ".join(sorted(selected)), job_duration)
                   
    except Exception as e:
        logger.error("JOB_FAILED | app=%s | error=%s | status=failed", app_name, str(e))
        raise
    finally:
        spark.stop()
        logger.info("JOB_CLEANUP | spark_session=stopped")


if __name__ == "__main__":
    main()
