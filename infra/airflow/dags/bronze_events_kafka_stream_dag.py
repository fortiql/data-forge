import os
from datetime import datetime
from typing import Any

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

DEFAULT_CONFIG = {
    "batch_size": 5000,
    "starting_offsets": "earliest",
    "expire_days": "7d",
}

BRONZE_STREAMS = {
    "raw_events": {
        "type": "multi_topic",
        "topics": [
            "orders.v1",
            "payments.v1", 
            "shipments.v1",
            "inventory-changes.v1",
            "customer-interactions.v1",
        ],
        "table": "iceberg.bronze.raw_events",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/raw_events",
        "application": AGG_APPLICATION,
        "batch_size": 50000,
        "starting_offsets": "latest",
        "expire_days": "7d",
    },
    "cdc_users": {
        "type": "single_topic",
        "name": "users",
        "topic": "demo.public.users",
        "table": "iceberg.bronze.demo_public_users",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_users",
        "application": TOPIC_APPLICATION,
    },
    "cdc_products": {
        "type": "single_topic",
        "name": "products",
        "topic": "demo.public.products", 
        "table": "iceberg.bronze.demo_public_products",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_products",
        "application": TOPIC_APPLICATION,
    },
    "cdc_inventory": {
        "type": "single_topic",
        "name": "inventory",
        "topic": "demo.public.inventory",
        "table": "iceberg.bronze.demo_public_inventory",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_inventory",
        "application": TOPIC_APPLICATION,
    },
    "cdc_warehouse_inventory": {
        "type": "single_topic",
        "name": "warehouse_inventory",
        "topic": "demo.public.warehouse_inventory",
        "table": "iceberg.bronze.demo_public_warehouse_inventory",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_warehouse_inventory",
        "application": TOPIC_APPLICATION,
    },
    "cdc_suppliers": {
        "type": "single_topic",
        "name": "suppliers",
        "topic": "demo.public.suppliers",
        "table": "iceberg.bronze.demo_public_suppliers",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_suppliers",
        "application": TOPIC_APPLICATION,
    },
    "cdc_customer_segments": {
        "type": "single_topic", 
        "name": "customer_segments",
        "topic": "demo.public.customer_segments",
        "table": "iceberg.bronze.demo_public_customer_segments",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_customer_segments",
        "application": TOPIC_APPLICATION,
    },
    "cdc_warehouses": {
        "type": "single_topic",
        "name": "warehouses", 
        "topic": "demo.public.warehouses",
        "table": "iceberg.bronze.demo_public_warehouses",
        "checkpoint": "s3a://checkpoints/spark/iceberg/bronze/cdc/demo_public_warehouses",
        "application": TOPIC_APPLICATION,
    },
}

def build_default_params(streams: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Compose default DAG params for multi-topic and CDC streams."""

    defaults: dict[str, Any] = {"streams": {}}

    for key, conf in streams.items():
        stream_defaults: dict[str, Any] = {
            "table": conf["table"],
            "checkpoint": conf["checkpoint"],
            "batch_size": conf.get("batch_size", DEFAULT_CONFIG["batch_size"]),
            "starting_offsets": conf.get("starting_offsets", DEFAULT_CONFIG["starting_offsets"]),
            "expire_days": conf.get("expire_days", DEFAULT_CONFIG["expire_days"]),
        }

        if conf["type"] == "multi_topic":
            stream_defaults["topics"] = list(conf.get("topics", []))
        elif conf["type"] == "single_topic":
            stream_defaults["topic"] = conf["topic"]
        else:
            raise ValueError(f"Unsupported stream type for defaults: {conf['type']}")

        defaults["streams"][key] = stream_defaults
    defaults.update(defaults["streams"]["raw_events"])
    return defaults


DEFAULT_PARAMS = build_default_params(BRONZE_STREAMS)


def create_stream_tasks(stream_key, stream_config, dag):
    """
    Factory function to create ingestion and maintenance tasks for a stream.
    
    Args:
        stream_key: Unique identifier for the stream
        stream_config: Stream configuration dictionary
        dag: Airflow DAG instance
        
    Returns:
        Tuple of (ingest_task, maintenance_task)
    """
    config = {**DEFAULT_CONFIG, **stream_config}
    
    if config["type"] == "multi_topic":
        task_id = "bounded_ingest"
        maintenance_task_id = "iceberg_maintenance_bronze_raw_events"
        application_args = [
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
        ]
        table_ref = "{{ dag_run.conf.table if dag_run and dag_run.conf and dag_run.conf.table is not none else params.table }}"
        expire_days_ref = "{{ dag_run.conf.expire_days if dag_run and dag_run.conf and dag_run.conf.expire_days is not none else params.expire_days }}"
        
    elif config["type"] == "single_topic":
        name = config["name"]
        task_id = f"ingest_{name}"
        maintenance_task_id = f"iceberg_maintenance_{name}"
        application_args = [
            "--topic", config["topic"],
            "--table", config["table"],
            "--checkpoint", config["checkpoint"],
            "--batch-size", str(config["batch_size"]),
            "--starting-offsets", config["starting_offsets"],
        ]
        table_ref = config["table"]
        expire_days_ref = config["expire_days"]
        
    else:
        raise ValueError(f"Unknown stream type: {config['type']}")
    ingest_task = SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=config["application"],
        py_files=SPARK_PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=application_args,
        verbose=True,
        outlets=[iceberg_dataset(config["table"])],
        dag=dag,
    )  
    maintenance_task = PythonOperator(
        task_id=maintenance_task_id,
        python_callable=iceberg_maintenance,
        op_kwargs={
            "table": table_ref,
            "expire_days": expire_days_ref,
        },
        dag=dag,
    )
    ingest_task >> maintenance_task
    
    return ingest_task, maintenance_task


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
    for stream_key, stream_config in BRONZE_STREAMS.items():
        create_stream_tasks(stream_key, stream_config, dag)
