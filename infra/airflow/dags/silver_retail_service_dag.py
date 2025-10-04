import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from _spark_common import (
    iceberg_dataset,
    maintenance_tasks_for,
    spark_base_conf,
    spark_env_vars,
    spark_job_base,
    spark_packages,
    spark_utils_py_files,
)
from silver import TABLE_BUILDERS


default_args = {"owner": "DataForge", "depends_on_past": False, "retries": 0}

PACKAGES = spark_packages()
BASE_CONF = spark_base_conf()
ENV_VARS = spark_env_vars()

SPARK_JOB_BASE = spark_job_base()
SILVER_APPLICATION = os.path.join(SPARK_JOB_BASE, "silver_retail_service.py")
SPARK_PY_FILES = spark_utils_py_files()


SILVER_TABLES = {builder.identifier: builder.table for builder in TABLE_BUILDERS}

TRIGGER_DATASETS = [
    iceberg_dataset("iceberg.bronze.raw_events"),
    iceberg_dataset("iceberg.bronze.demo_public_users"),
    iceberg_dataset("iceberg.bronze.demo_public_products"),
    iceberg_dataset("iceberg.bronze.demo_public_inventory"),
    iceberg_dataset("iceberg.bronze.demo_public_suppliers"),
    iceberg_dataset("iceberg.bronze.demo_public_customer_segments"),
    iceberg_dataset("iceberg.bronze.demo_public_warehouse_inventory"),
]


with DAG(
    dag_id="silver_retail_service",
    description="Build Silver dimensions and facts from Bronze retail data",
    doc_md="""\
        ### Silver Retail Service DAG
        - Dedicated tasks build each Silver table via `--tables` argument on the Spark job.
        - Outputs publish Iceberg datasets and trigger maintenance per table.
        - The DAG fires whenever Bronze datasets update.
        """,
    start_date=None,
    schedule=TRIGGER_DATASETS,
    catchup=False,
    default_args=default_args,
    max_active_tasks=6,
    tags=["silver", "iceberg", "retail"],
) as dag:
    build_tasks: dict[str, SparkSubmitOperator] = {}

    for identifier, table in SILVER_TABLES.items():
        build_task = SparkSubmitOperator(
            task_id=f"build_{identifier}",
            conn_id="spark_default",
            application=SILVER_APPLICATION,
            py_files=SPARK_PY_FILES,
            packages=PACKAGES,
            env_vars=ENV_VARS,
            conf=BASE_CONF,
            application_args=["--tables", identifier],
            verbose=True,
            outlets=[iceberg_dataset(table)],
        )

        build_tasks[identifier] = build_task
        maintenance_tasks_for(dag, build_task, [table], expire_days="14d")

    fact_dependencies = {
        "fact_order_service": ["dim_customer_profile", "dim_product_catalog"],
        "fact_inventory_position": ["dim_product_catalog", "dim_warehouse"],
        "fact_customer_engagement": ["dim_customer_profile", "dim_product_catalog"],
    }

    for fact_identifier, deps in fact_dependencies.items():
        if fact_identifier not in build_tasks:
            continue
        for dep in deps:
            if dep in build_tasks:
                build_tasks[fact_identifier].set_upstream(build_tasks[dep])
