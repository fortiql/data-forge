#!/usr/bin/env python3
"""AvailableNow ingestion from Kafka topics into a shared Bronze table."""

import argparse
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener

from spark_utils import (
        build_spark,
        decode_confluent_avro,
        ensure_iceberg_table,
        ensure_schema,
        payload_size_expr,
        schema_id_expr,
        warn_if_checkpoint_exists,
    )


APP_NAME = "bronze_events_kafka_stream"

logger = logging.getLogger(__name__)


def build_stream(
    spark: SparkSession,
    *,
    topics_csv: str,
    kafka_bootstrap: str,
    schema_registry_url: str,
    starting_offsets: str,
    batch_size: int,
) -> DataFrame:
    topics = ",".join([t.strip() for t in topics_csv.split(",") if t.strip()])

    src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topics)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(batch_size))
        .option("failOnDataLoss", "false")
        .load()
        .withWatermark("timestamp", "1 minute")
    )

    decode_udf = F.udf(lambda value: decode_confluent_avro(value, schema_registry_url), T.StringType())

    df = (
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

    ordered = df.select(
        "event_source",
        "event_time",
        "schema_id",
        "payload_size",
        "json_payload",
        "partition",
        "offset",
    )
    logger.info("Output schema: %s", ordered.schema.simpleString())
    return ordered


def write_events(df: DataFrame, *, table: str, checkpoint: str) -> None:
    query = (
        df.writeStream.format("iceberg")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    logger.info("Query started. Awaiting termination...")
    query.awaitTermination()
    logger.info("Query finished.")


class _ProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        logger.info("Streaming started: id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event):
        try:
            progress = json.loads(event.progress.prettyJson)
            logger.info(
                "progress: inputRows=%s rowsPerSec=%s batchId=%s",
                progress.get("numInputRows"),
                progress.get("inputRowsPerSecond"),
                progress.get("batchId"),
            )
        except Exception:  # pragma: no cover - defensive
            logger.info("progress: %s", event.progress.prettyJson)

    def onQueryTerminated(self, event):
        logger.info("Streaming terminated: id=%s exception=%s", event.id, event.exception)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AvailableNow bronze ingestion")
    parser.add_argument("--topics", required=True, help="Comma-separated Kafka topics")
    parser.add_argument("--checkpoint", required=True, help="Checkpoint path (s3a://…)")
    parser.add_argument("--batch-size", type=int, default=10000, help="maxOffsetsPerTrigger")
    parser.add_argument("--starting-offsets", default="latest", choices=["earliest", "latest"])
    parser.add_argument("--table", default="iceberg.bronze.raw_events")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    spark = build_spark(APP_NAME)
    spark.sparkContext.setLogLevel("INFO")
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
    spark.streams.addListener(_ProgressListener())
    kafka_bootstrap = spark.conf.get("spark.dataforge.kafka.bootstrap", "kafka:9092")
    schema_registry_url = spark.conf.get("spark.dataforge.schema.registry", "http://schema-registry:8081")

    logger.info(
        "Starting AvailableNow: topics=%s batch_size=%s checkpoint=%s starting_offsets=%s table=%s",
        args.topics,
        args.batch_size,
        args.checkpoint,
        args.starting_offsets,
        args.table,
    )

    if args.starting_offsets == "earliest":
        warn_if_checkpoint_exists(spark, args.checkpoint, logger=logger)

    df = build_stream(
        spark,
        topics_csv=args.topics,
        kafka_bootstrap=kafka_bootstrap,
        schema_registry_url=schema_registry_url,
        starting_offsets=args.starting_offsets,
        batch_size=args.batch_size,
    )
    write_events(df, table=args.table, checkpoint=args.checkpoint)


if __name__ == "__main__":
    main()
