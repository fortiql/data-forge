import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

from _spark_common import (
    iceberg_dataset,
    iceberg_maintenance,
    spark_base_conf,
    spark_env_vars,
    spark_job_base,
    spark_packages,
    spark_utils_py_files,
)


default_args = {"owner": "DataForge", "depends_on_past": False, "retries": 0}

PACKAGES = spark_packages()
BASE_CONF = spark_base_conf()
ENV_VARS = spark_env_vars()

SPARK_JOB_BASE = spark_job_base()
AGG_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze_events_kafka_stream.py")
TOPIC_APPLICATION = os.path.join(SPARK_JOB_BASE, "bronze_cdc_stream.py")
SPARK_PY_FILES = spark_utils_py_files()

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
    "batch_size": 5000,
    "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/raw_events",
    "starting_offsets": "latest",
    "table": "iceberg.bronze.raw_events",
    "expire_days": "7d",
}


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
        outlets=[iceberg_dataset("iceberg.bronze.raw_events")],
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
            outlets=[iceberg_dataset(stream["table"])],
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
