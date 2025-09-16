from datetime import datetime
import os
from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator


default_args = {"owner": "data-platform", "depends_on_past": False, "retries": 0}

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.791",
])

BASE_CONF = {
    "hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    "spark.master": "spark://spark-master:7077",
    "spark.submit.deployMode": "client",
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
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
    "spark.cleaner.referenceTracking.cleanCheckpoints": "true",
}

ENV_VARS = {
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "AWS_DEFAULT_REGION": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ROOT_USER", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minio"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    "S3_ENDPOINT": os.getenv("S3_ENDPOINT", os.getenv("MINIO_ENDPOINT", "minio:9000")),
}

with DAG(
    dag_id="event_driven_bronze_available_now",
    description="Event-triggered AvailableNow bounded ingest to Iceberg + maintenance",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={
        "topics": ["orders.v1", "payments.v1", "shipments.v1"],
        "batch_size": 10000,
        "checkpoint": "s3a://checkpoints/spark/bronze_raw_events",
        "starting_offsets": "latest",
        "table": "iceberg.bronze_example.raw_events",
        "expire_days": 1,
    },
    tags=["streaming", "availableNow", "iceberg", "event-driven"],
) as dag:
    bounded_ingest = SparkSubmitOperator(
        task_id="bounded_ingest",
        conn_id="spark_default",
        application="/opt/spark/jobs/bronze_available_now.py",
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=[
            "--topics",
            "{{ (dag_run.conf.topics if dag_run and dag_run.conf and dag_run.conf.topics is not none else params.topics) | join(',') }}",
            "--batch-size",
            "{{ dag_run.conf.batch_size if dag_run and dag_run.conf and dag_run.conf.batch_size is not none else params.batch_size }}",
            "--checkpoint",
            "{{ dag_run.conf.checkpoint if dag_run and dag_run.conf and dag_run.conf.checkpoint is not none else params.checkpoint }}",
            "--starting-offsets",
            "{{ dag_run.conf.starting_offsets if dag_run and dag_run.conf and dag_run.conf.starting_offsets is not none else params.starting_offsets }}",
            "--table",
            "{{ dag_run.conf.table if dag_run and dag_run.conf and dag_run.conf.table is not none else params.table }}",
        ],
        verbose=True,
    )

    def _run_trino_maintenance(table: str):
        import trino
        # Expect fully qualified table: catalog.schema.table
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
            f"ALTER TABLE {fqtn} EXECUTE expire_snapshots(retention_threshold => '7d')"
        )
        _ = cur.fetchall()
        cur.execute(f"ALTER TABLE {fqtn} EXECUTE remove_orphan_files")
        _ = cur.fetchall()

    iceberg_maintenance = PythonOperator(
        task_id="iceberg_maintenance",
        python_callable=_run_trino_maintenance,
        op_kwargs={
            "table": "{{ dag_run.conf.table if dag_run and dag_run.conf and dag_run.conf.table is not none else params.table }}",
            "expire_days": "{{ dag_run.conf.expire_days if dag_run and dag_run.conf and dag_run.conf.expire_days is not none else params.expire_days }}",
        },
    )

    bounded_ingest >> iceberg_maintenance
