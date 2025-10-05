"""Shared helpers for Spark ingestion jobs."""

from __future__ import annotations

import datetime
import decimal
import io
import json
import logging
from typing import Dict, Optional

from pyspark.sql import Column, SparkSession, functions as F


LOG = logging.getLogger(__name__)


def build_spark(app_name: str) -> SparkSession:
    """Create or reuse a Spark session with the given application name and memory optimizations."""
    
    builder = (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") 
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        .config("spark.sql.shuffle.partitions", "400")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
    )
    
    return builder.getOrCreate()


def _json_default(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return obj.hex()
    return str(obj)


def decode_confluent_avro(value: Optional[bytes], schema_registry_url: str) -> Optional[str]:
    """Decode Confluent Avro binary payload into JSON using Schema Registry."""

    if value is None or len(value) < 5:
        return None
    if value[0] != 0:
        return None

    schema_id = int.from_bytes(value[1:5], byteorder="big", signed=False)
    payload = memoryview(value)[5:].tobytes()

    from confluent_kafka.schema_registry import SchemaRegistryClient
    from fastavro import schemaless_reader

    client = SchemaRegistryClient({"url": schema_registry_url})
    schema_str = client.get_schema(schema_id).schema_str
    schema = json.loads(schema_str)
    record = schemaless_reader(io.BytesIO(payload), schema)
    return json.dumps(record, separators=(",", ":"), default=_json_default)


def schema_id_expr(col: str) -> Column:
    """Extract Schema Registry ID from a Confluent payload column."""

    return F.when(
        (F.col(col).isNotNull()) & (F.length(col) >= 5),
        F.conv(F.hex(F.expr(f"substring({col}, 2, 4)")), 16, 10).cast("int"),
    ).otherwise(F.lit(None).cast("int"))


def payload_size_expr(col: str) -> Column:
    """Compute payload size excluding the magic byte and schema id header."""

    return F.when(F.col(col).isNotNull(), F.length(col) - F.lit(5)).otherwise(F.lit(None))


def ensure_schema(spark: SparkSession, schema_name: str) -> None:
    """Ensure the specified schema exists in the catalog.
    
    Args:
        spark: SparkSession instance
        schema_name: Full schema name (e.g., 'iceberg.silver', 'iceberg.bronze')
    """
    try:
        # Try to show schemas to check if it exists
        schemas_df = spark.sql("SHOW SCHEMAS")
        existing_schemas = [row.databaseName for row in schemas_df.collect()]
        
        # Extract just the schema part (e.g., 'bronze' from 'iceberg.bronze')
        if "." in schema_name:
            _, schema_part = schema_name.split(".", 1)
        else:
            schema_part = schema_name
            
        if schema_part in existing_schemas:
            LOG.info("SCHEMA_EXISTS | schema=%s | status=confirmed", schema_name)
        else:
            # Create schema using full qualified name
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            LOG.info("SCHEMA_CREATED | schema=%s | status=success", schema_name)
            
    except Exception as e:
        # Fallback: just try to create the schema
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            LOG.info("SCHEMA_CREATED | schema=%s | status=success", schema_name)
        except Exception as create_error:
            LOG.warning("SCHEMA_CREATE_FAILED | schema=%s | error=%s", schema_name, str(create_error))


def ensure_iceberg_table(
    spark: SparkSession,
    table: str,
    columns_sql: str,
    *,
    partition_field: str = "event_source",
    table_properties: Optional[Dict[str, str]] = None,
) -> None:
    """Create the Iceberg table if it does not exist."""

    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError("Table must be catalog.schema.table (e.g. iceberg.bronze.raw_events)")

    catalog, schema, _ = parts
    ensure_schema(spark, f"{catalog}.{schema}")

    properties = {"write.format.default": "parquet"}
    if table_properties:
        properties.update(table_properties)
    props_str = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns_sql}
        )
        USING iceberg
        PARTITIONED BY ({partition_field})
        TBLPROPERTIES ({props_str})
        """
    )
    LOG.info("TABLE_READY | table=%s | partition_by=%s | format=iceberg", table, partition_field)


def warn_if_checkpoint_exists(spark: SparkSession, checkpoint: str, *, logger: logging.Logger) -> None:
    """Warn when replay from earliest is requested but checkpoint already exists."""

    try:
        jvm = spark._jvm
        hconf = spark.sparkContext._jsc.hadoopConfiguration()
        path = jvm.org.apache.hadoop.fs.Path(checkpoint)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(path.toUri(), hconf)
        if fs.exists(path):
            logger.warning("CHECKPOINT_EXISTS | location=%s | starting_offsets=earliest | behavior=resume_from_checkpoint | " +
                          "note=use_new_checkpoint_path_to_replay_from_earliest", checkpoint)
    except Exception as exc:  # pragma: no cover - defensive
        logger.info("CHECKPOINT_CHECK_FAILED | location=%s | error=%s | behavior=continuing", checkpoint, str(exc))
