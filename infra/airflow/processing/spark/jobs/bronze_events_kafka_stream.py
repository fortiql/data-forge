#!/usr/bin/env python3
import argparse
import datetime
import io
import json
import logging
from typing import Optional

import decimal
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from pyspark.sql.streaming import StreamingQueryListener


APP_NAME = "bronze_events_kafka_stream"

logger = logging.getLogger(__name__)


def build_spark(app_name: str = APP_NAME) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def _json_default(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return obj.hex()
    return str(obj)


def _decode_udf_fn(value: Optional[bytes], sr_url: str) -> Optional[str]:
    """Decode Confluent Avro binary to JSON.
    """
    if value is None or len(value) < 5:
        return None
    if value[0] != 0:
        return None
    schema_id = int.from_bytes(value[1:5], byteorder="big", signed=False)
    payload = memoryview(value)[5:].tobytes()

    from confluent_kafka.schema_registry import SchemaRegistryClient
    from fastavro import schemaless_reader

    client = SchemaRegistryClient({"url": sr_url})
    schema_str = client.get_schema(schema_id).schema_str
    schema = json.loads(schema_str)
    rec = schemaless_reader(io.BytesIO(payload), schema)
    return json.dumps(rec, separators=(",", ":"), default=_json_default)


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
        .load().withWatermark("timestamp", "1 minute")
    )

    decode_udf = F.udf(_decode_udf_fn, T.StringType())

    df = (
        src.select(
            F.col("topic").alias("event_source"),
            F.col("timestamp").alias("event_time"),
            F.col("partition"),
            F.col("offset"),
            F.col("value"),
        )
        .withColumn(
            "schema_id",
            F.when(
                F.length("value") >= 5,
                F.conv(F.hex(F.expr("substring(value, 2, 4)")), 16, 10).cast("int"),
            ).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "payload_size",
            F.when(F.col("value").isNotNull(), F.length("value") - F.lit(5)).otherwise(F.lit(None)),
        )
        .withColumn("json_payload", decode_udf(F.col("value"), F.lit(schema_registry_url)))
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

def create_database_if_not_exists(spark: SparkSession, db_name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    logger.info("Database ensured: %s", db_name)

def create_table_if_not_exists(spark: SparkSession, table: str) -> None:
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError(f"Table name must be in format catalog.db.table, got: {table}")
    catalog, db_name, table_name = parts
    create_database_if_not_exists(spark, db_name)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
            event_source STRING,
            event_time TIMESTAMP,
            schema_id INT,
            payload_size INT,
            json_payload STRING,
            partition INT,
            offset BIGINT
            )
        USING iceberg
        PARTITIONED BY (event_source)
        TBLPROPERTIES ('write.format.default'='parquet')
        """
    )
    logger.info("Table ensured: %s", table)


def write_events(df: DataFrame, *, table: str, checkpoint: str) -> None:
    q = (
        df.writeStream.format("iceberg")
        .option("path", table)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    logger.info("Query started. Awaiting termination...")
    q.awaitTermination()
    logger.info("Query finished.")


class _ProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        logger.info("Streaming started: id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event):
        try:
            p = json.loads(event.progress.prettyJson)
            logger.info(
                "progress: inputRows=%s rowsPerSec=%s batchId=%s",
                p.get("numInputRows"),
                p.get("inputRowsPerSecond"),
                p.get("batchId"),
            )
        except Exception:
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

    spark = build_spark()
    spark.sparkContext.setLogLevel("INFO")
    create_database_if_not_exists(spark, "bronze")
    create_table_if_not_exists(spark, args.table)
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

    try:
        if args.starting_offsets == "earliest":
            jvm = spark._jvm
            hconf = spark.sparkContext._jsc.hadoopConfiguration()
            p = jvm.org.apache.hadoop.fs.Path(args.checkpoint)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(p.toUri(), hconf)
            if fs.exists(p):
                logger.warning(
                    "starting_offsets=earliest requested, but checkpoint exists at %s. "
                    "Spark will ignore startingOffsets and resume from the checkpoint. "
                    "To truly replay from earliest, use a new checkpoint path (and consider rebuilding or writing to a new table to avoid duplicates).",
                    args.checkpoint,
                )
    except Exception as e:
        logger.info("Unable to verify checkpoint existence (%s); continuing.", e)

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
