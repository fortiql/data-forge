import os
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator


default_args = {"owner": "DataForge", "depends_on_past": False, "retries": 0}

PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.791",
    ]
)

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

SPARK_JOB_BASE = os.getenv("SPARK_JOB_BASE", "/opt/spark/jobs")
AGG_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze_events_kafka_stream.py")
TOPIC_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze_cdc_stream.py")
SPARK_UTILS = os.getenv("SPARK_UTILS_PATH", os.path.join(SPARK_JOB_BASE, "spark_utils.py"))
SPARK_PY_FILES = ",".join([SPARK_UTILS])

DEFAULT_BATCH_SIZE = 5000
DEFAULT_STARTING_OFFSETS = "latest"
DEFAULT_EXPIRE_DAYS = "7d"

CDC_STREAMS = [
    {
        "name": "users",
        "topic": "demo.public.users",
        "table": "iceberg.bronze.demo_public_users",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_users",
    },
    {
        "name": "products",
        "topic": "demo.public.products",
        "table": "iceberg.bronze.demo_public_products",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_products",
    },
    {
        "name": "inventory",
        "topic": "demo.public.inventory",
        "table": "iceberg.bronze.demo_public_inventory",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_inventory",
    },
    {
        "name": "warehouse_inventory",
        "topic": "demo.public.warehouse_inventory",
        "table": "iceberg.bronze.demo_public_warehouse_inventory",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_warehouse_inventory",
    },
    {
        "name": "suppliers",
        "topic": "demo.public.suppliers",
        "table": "iceberg.bronze.demo_public_suppliers",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_suppliers",
    },
    {
        "name": "customer_segments",
        "topic": "demo.public.customer_segments",
        "table": "iceberg.bronze.demo_public_customer_segments",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_customer_segments",
    },
]

DEFAULT_PARAMS = {
    "topics": [
        "orders.v1",
        "payments.v1",
        "shipments.v1",
        "inventory-changes.v1",
        "customer-interactions.v1",
    ],
    "batch_size": 10000,
    "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/raw_events",
    "starting_offsets": "latest",
    "table": "iceberg.bronze.raw_events",
    "expire_days": "7d",
}


def table_to_dataset(table: str) -> Dataset:
    catalog, schema, tbl = table.split(".")
    return Dataset(f"s3://iceberg/warehouse/{schema}.db/{tbl}/")


def iceberg_maintenance(table: str, expire_days: str) -> None:
    """Run Iceberg maintenance operations via Trino."""

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


with DAG(
    dag_id="bronze_events_kafka_stream",
    description="Ingest Kafka demo streams and Debezium CDC topics into Bronze",
    doc_md="""\
        #### Bronze Ingestion
        - `bounded_ingest` captures demo generator topics into a shared Bronze table.
        - CDC tasks capture each Debezium topic into its own Bronze table.
        - Each table receives Iceberg maintenance after ingestion.
        """,
    start_date=None,
    max_active_tasks=2,
    schedule=None,
    catchup=False,
    default_args=default_args,
    params=DEFAULT_PARAMS,
    tags=["streaming", "cdc", "iceberg"],
) as dag:
    bounded_ingest = SparkSubmitOperator(
        task_id="bounded_ingest",
        conn_id="spark_default",
        application=AGG_APPLICATION,
        py_files=SPARK_PY_FILES,
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
        outlets=[Dataset("s3://iceberg/warehouse/bronze.db/raw_events/")],
    )

    bounded_maintenance = PythonOperator(
        task_id="iceberg_maintenance_bronze_raw_events",
        python_callable=iceberg_maintenance,
        op_kwargs={
            "table": "{{ dag_run.conf.table if dag_run and dag_run.conf and dag_run.conf.table is not none else params.table }}",
            "expire_days": "{{ dag_run.conf.expire_days if dag_run and dag_run.conf and dag_run.conf.expire_days is not none else params.expire_days }}",
        },
    )

    bounded_ingest >> bounded_maintenance

    for stream in CDC_STREAMS:
        application_args = [
            "--topic",
            stream["topic"],
            "--table",
            stream["table"],
            "--checkpoint",
            stream["checkpoint"],
            "--batch-size",
            str(stream.get("batch_size", DEFAULT_BATCH_SIZE)),
            "--starting-offsets",
            stream.get("starting_offsets", DEFAULT_STARTING_OFFSETS),
        ]

        ingest = SparkSubmitOperator(
            task_id=f"ingest_{stream['name']}",
            conn_id="spark_default",
            application=TOPIC_APPLICATION,
            py_files=SPARK_PY_FILES,
            packages=PACKAGES,
            env_vars=ENV_VARS,
            conf=BASE_CONF,
            application_args=application_args,
            verbose=True,
            outlets=[table_to_dataset(stream["table"])],
        )

        maintenance = PythonOperator(
            task_id=f"iceberg_maintenance_{stream['name']}",
            python_callable=iceberg_maintenance,
            op_kwargs={
                "table": stream["table"],
                "expire_days": stream.get("expire_days", DEFAULT_EXPIRE_DAYS),
            },
        )

        ingest >> maintenance
