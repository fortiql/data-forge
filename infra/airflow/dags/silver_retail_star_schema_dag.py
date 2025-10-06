import os
from typing import Dict

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from _spark_common import (
    iceberg_dataset,
    iceberg_maintenance,
    spark_base_conf,
    spark_env_vars,
    spark_job_base,
    spark_packages,
    spark_utils_py_files,
)
from silver import TABLE_BUILDERS, TableBuilder


default_args = {"owner": "DataForge", "depends_on_past": False, "retries": 1}

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

DEFAULT_CONFIG = {
    "expire_days": "14d",
    "max_active_tasks": 2,
}


def create_silver_task(builder: TableBuilder, dag) -> tuple[SparkSubmitOperator, object]:
    """
    Factory function to create build and maintenance tasks for a Silver table.
    
    Args:
        builder: TableBuilder configuration
        dag: Airflow DAG instance
        
    Returns:
        Tuple of (build_task, maintenance_task)
    """
    build_task = SparkSubmitOperator(
        task_id=f"build_{builder.identifier}",
        conn_id="spark_default",
        application=SILVER_APPLICATION,
        py_files=SPARK_PY_FILES,
        packages=PACKAGES,
        env_vars=ENV_VARS,
        conf=BASE_CONF,
        application_args=["--tables", builder.identifier],
        verbose=True,
        outlets=[iceberg_dataset(builder.table)],
        dag=dag,
    )
    from airflow.providers.standard.operators.python import PythonOperator
    maintenance_task = PythonOperator(
        task_id=f"iceberg_maintenance_{builder.identifier}",
        python_callable=iceberg_maintenance,
        op_kwargs={
            "table": builder.table,
            "expire_days": DEFAULT_CONFIG["expire_days"],
        },
        dag=dag,
    )
    build_task >> maintenance_task
    
    return build_task, maintenance_task


with DAG(
    dag_id="silver_retail_star_schema",
    description="Build Kimball-compliant Silver star schema from Bronze retail data",
    doc_md="""\
        #### Silver Retail Service - Kimball Star Schema
        - **Dimensions**: Customer, Product, Supplier, Warehouse, Date (SCD2 with surrogate keys)
        - **Facts**: Order Service, Inventory Position, Customer Engagement (surrogate keys only)
        - **Dependencies**: Date dimension loads first, then other dims, finally facts
        - **Kimball Compliance**: Proper surrogate keys, no natural keys in facts, conformed dimensions
        - **Maintenance**: OPTIMIZE, EXPIRE_SNAPSHOTS, and REMOVE_ORPHANS post-build
        - **Trigger**: When Bronze datasets update
        """,
    start_date=None,
    schedule=TRIGGER_DATASETS,
    catchup=False,
    default_args=default_args,
    max_active_tasks=DEFAULT_CONFIG["max_active_tasks"],
    tags=["silver", "iceberg", "retail"],
) as dag:
    build_tasks: Dict[str, SparkSubmitOperator] = {}
    maintenance_tasks: Dict[str, object] = {}

    for builder in TABLE_BUILDERS:
        build_task, maintenance_task = create_silver_task(builder, dag)
        build_tasks[builder.identifier] = build_task
        maintenance_tasks[builder.identifier] = maintenance_task
    fact_dependencies = {
        "fact_order_service": ["dim_date", "dim_customer_profile", "dim_product_catalog"],
        "fact_inventory_position": ["dim_product_catalog", "dim_warehouse"],
        "fact_customer_engagement": ["dim_date", "dim_customer_profile", "dim_product_catalog"],
    }

    for fact_identifier, deps in fact_dependencies.items():
        if fact_identifier not in build_tasks:
            continue
        for dep in deps:
            if dep in build_tasks:
                build_tasks[dep] >> build_tasks[fact_identifier]
