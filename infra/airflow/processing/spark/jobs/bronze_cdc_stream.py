#!/usr/bin/env python3
"""Single-topic Bronze ingestion for CDC streams.

Reads one Kafka topic, decodes Confluent Avro payloads, and appends the raw
event into a dedicated Iceberg Bronze table.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from spark_utils import (
        build_spark,
        decode_confluent_avro,
        ensure_iceberg_table,
        ensure_schema,
        payload_size_expr,
        schema_id_expr,
        warn_if_checkpoint_exists,
    )


APP_NAME_PREFIX = "bronze_cdc_stream"

logger = logging.getLogger(__name__)


def build_stream(
    spark: SparkSession,
    *,
    topic: str,
    kafka_bootstrap: str,
    schema_registry_url: str,
    starting_offsets: str,
    batch_size: int,
) -> DataFrame:
    src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(batch_size))
        .option("failOnDataLoss", "false")
        .load()
    )

    decode_udf = F.udf(lambda value: decode_confluent_avro(value, schema_registry_url), T.StringType())

    enriched = (
        src.select(
            F.col("topic").alias("event_source"),
            F.col("timestamp").alias("event_time"),
            F.col("partition"),
            F.col("offset"),
            F.col("value"),
        )
        .withColumn("schema_id", schema_id_expr("value"))
        .withColumn("payload_size", payload_size_expr("value"))
        .withColumn("json_payload", decode_udf(F.col("value")))
        .drop("value")
    )

    ordered = enriched.select(
        "event_source",
        "event_time",
        "schema_id",
        "payload_size",
        "json_payload",
        "partition",
        "offset",
    )
    schema_fields = [f"{field.name}:{field.dataType.simpleString()}" for field in ordered.schema.fields]
    logger.info("STREAM_CONFIGURED | topic=%s | fields=[%s]", topic, ", ".join(schema_fields))
    return ordered


def write_stream(df: DataFrame, *, table: str, checkpoint: str) -> None:
    start_time = time.time()
    logger.info("STREAM_STARTING | table=%s | checkpoint=%s | mode=availableNow", table, checkpoint)
    
    query = (
        df.writeStream.format("iceberg")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    
    logger.info("STREAM_RUNNING | query_id=%s | status=waiting_for_completion", query.id)
    query.awaitTermination()
    
    duration = time.time() - start_time
    progress = query.lastProgress
    
    if progress:
        records_processed = progress.get("inputRowsPerSecond", 0) * duration if progress.get("inputRowsPerSecond") else 0
        logger.info("STREAM_COMPLETED | table=%s | duration=%.2fs | records_processed=%d | status=success", 
                   table, duration, int(records_processed))
    else:
        logger.info("STREAM_COMPLETED | table=%s | duration=%.2fs | status=success", table, duration)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AvailableNow Bronze ingestion for a single Kafka topic")
    parser.add_argument("--topic", required=True, help="Kafka topic to ingest")
    parser.add_argument("--table", required=True, help="Target Iceberg table (catalog.schema.table)")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint location (s3a://…)")
    parser.add_argument("--batch-size", type=int, default=5000, help="maxOffsetsPerTrigger")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    args = parse_args()

    app_name = f"{APP_NAME_PREFIX}:{args.table.replace('.', ':')}"
    logger.info("JOB_STARTING | app=%s | topic=%s | table=%s", app_name, args.topic, args.table)
    
    spark = build_spark(app_name=app_name)
    spark.sparkContext.setLogLevel("WARN")
    ensure_schema(spark, "iceberg.bronze")

    ensure_iceberg_table(
        spark,
        args.table,
        columns_sql="""
            event_source STRING,
            event_time TIMESTAMP,
            schema_id INT,
            payload_size INT,
            json_payload STRING,
            partition INT,
            offset BIGINT
        """,
    )

    kafka_bootstrap = spark.conf.get("spark.dataforge.kafka.bootstrap", "kafka:9092")
    schema_registry_url = spark.conf.get("spark.dataforge.schema.registry", "http://schema-registry:8081")

    logger.info("CONFIG_LOADED | topic=%s | table=%s | starting_offsets=%s | batch_size=%d | kafka=%s | schema_registry=%s", 
               args.topic, args.table, args.starting_offsets, args.batch_size, kafka_bootstrap, schema_registry_url)

    if args.starting_offsets == "earliest":
        warn_if_checkpoint_exists(spark, args.checkpoint, logger=logger)
    
    logger.info("CHECKPOINT_INFO | location=%s | starting_offsets=%s", args.checkpoint, args.starting_offsets)

    try:
        df = build_stream(
            spark,
            topic=args.topic,
            kafka_bootstrap=kafka_bootstrap,
            schema_registry_url=schema_registry_url,
            starting_offsets=args.starting_offsets,
            batch_size=args.batch_size,
        )

        write_stream(df, table=args.table, checkpoint=args.checkpoint)
        logger.info("JOB_SUCCESS | topic=%s | table=%s | status=completed", args.topic, args.table)
        
    except Exception as e:
        logger.error("JOB_FAILED | topic=%s | table=%s | error=%s | status=failed", 
                    args.topic, args.table, str(e))
        raise
    finally:
        spark.stop()
        logger.info("JOB_CLEANUP | spark_session=stopped")


if __name__ == "__main__":
    main()
