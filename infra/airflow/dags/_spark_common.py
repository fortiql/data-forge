"""Shared utilities for Airflo    # Resource Configuration
    "spark.executor.instances": "2",
    "spark.executor.cores": "1", 
    "spark.executor.memory": "1g",
    "spark.executor.memoryFraction": "0.8",
    "spark.driver.memory": "1g",
    "spark.driver.maxResultSize": "512m",that submit Spark jobs."""

from __future__ import annotations

import os
from typing import Iterable

from airflow.datasets import Dataset


_PACKAGES: list[str] = []

_SPARK_HOME = os.getenv("SPARK_HOME", "/opt/spark")
_JAR_DIR = os.getenv("SPARK_JARS_DIR", os.path.join(_SPARK_HOME, "jars"))
_EXTRA_JARS = [
    os.path.join(_JAR_DIR, "spark-sql-kafka-0-10_2.12-3.5.0.jar"),
    os.path.join(_JAR_DIR, "spark-token-provider-kafka-0-10_2.12-3.5.0.jar"),
    os.path.join(_JAR_DIR, "kafka-clients-3.5.0.jar"),
    os.path.join(_JAR_DIR, "commons-pool2-2.12.0.jar"),
    os.path.join(_JAR_DIR, "iceberg-spark-runtime-3.5_2.12-1.9.2.jar"),
    os.path.join(_JAR_DIR, "iceberg-aws-bundle-1.9.2.jar"),
    os.path.join(_JAR_DIR, "hadoop-aws-3.3.4.jar"),
    os.path.join(_JAR_DIR, "aws-java-sdk-bundle-1.12.791.jar"),
]

_BASE_CONF = {
    "hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.master": "spark://spark-master:7077",
    "spark.submit.deployMode": "client",
    # Resource Configuration
    "spark.executor.instances": "2",
    "spark.executor.cores": "1", 
    "spark.executor.memory": "1G",
    "spark.executor.memoryFraction": "0.8",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    # Storage Configuration
    "spark.sql.warehouse.dir": "s3a://iceberg/warehouse",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": "iceberg",
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "rest",
    "spark.sql.catalog.iceberg.uri": "http://hive-metastore:9001/iceberg",
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg/warehouse",
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    "spark.sql.catalog.iceberg.s3.region": "us-east-1",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.dataforge.kafka.bootstrap": "kafka:9092",
    "spark.dataforge.schema.registry": "http://schema-registry:8081",
    "spark.jars": ",".join(_EXTRA_JARS),
    # Additional resource and stability configuration
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.kryoserializer.buffer.max": "256m",
}


def spark_packages() -> str | None:
    """Return the standard list of Spark packages used across DAGs."""

    return ",".join(_PACKAGES) if _PACKAGES else None


def spark_base_conf() -> dict[str, str]:
    """Return baseline Spark configuration shared by DAGs."""

    return dict(_BASE_CONF)


def spark_env_vars() -> dict[str, str]:
    """Return environment variables required for Spark submissions."""

    return {
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "minio"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minio"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
        "S3_ENDPOINT": os.getenv("S3_ENDPOINT", os.getenv("MINIO_ENDPOINT", "minio:9000")),
        "SPARK_HOME": os.getenv("SPARK_HOME", "/opt/spark"),
        "SPARK_JARS_DIR": os.getenv("SPARK_JARS_DIR", "/opt/spark/jars"),
    }


def spark_job_base() -> str:
    """Return the base path where Spark job files are mounted."""

    return os.getenv("SPARK_JOB_BASE", "/opt/spark/jobs")


def spark_utils_py_files() -> str:
    """Return the comma-separated list of py_files distributed with Spark jobs."""

    utils_path = os.getenv("SPARK_UTILS_PATH", os.path.join(spark_job_base(), "spark_utils.py"))
    return ",".join([utils_path])


def iceberg_dataset(table: str) -> Dataset:
    """Translate an Iceberg table into the S3 dataset path Airflow tracks."""

    catalog, schema, tbl = table.split(".")
    return Dataset(f"s3://iceberg/warehouse/{schema}.db/{tbl}/")


def iceberg_maintenance(table: str, expire_days: str = "7d") -> None:
    """Run Iceberg maintenance operations via Trino for the provided table."""

    import trino

    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected table as catalog.schema.table, got: {table}")
    catalog, schema, tbl = parts
    conn = trino.dbapi.connect(host="trino", port=8080, user="airflow")
    cur = conn.cursor()
    fqtn = f"{catalog}.{schema}.{tbl}"
    cur.execute(f"ALTER TABLE {fqtn} EXECUTE optimize")
    _ = cur.fetchall()
    cur.execute(
        f"ALTER TABLE {fqtn} EXECUTE expire_snapshots(retention_threshold => '{expire_days}')"
    )
    _ = cur.fetchall()
    cur.execute(f"ALTER TABLE {fqtn} EXECUTE remove_orphan_files")
    _ = cur.fetchall()


def maintenance_tasks_for(dag, upstream_task, tables: Iterable[str], expire_days: str = "7d") -> None:
    """Create maintenance PythonOperator tasks for each table and chain them."""

    from airflow.providers.standard.operators.python import PythonOperator

    for table in tables:
        maintenance = PythonOperator(
            task_id=f"iceberg_maintenance_{table.split('.')[-1]}",
            python_callable=iceberg_maintenance,
            op_kwargs={"table": table, "expire_days": expire_days},
        )
        upstream_task >> maintenance
