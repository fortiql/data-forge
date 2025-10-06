"""Registry of Silver table builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Sequence

from pyspark.sql import DataFrame, SparkSession

from silver.builders import (
    build_dim_customer_profile,
    build_dim_date,
    build_dim_product_catalog,
    build_dim_supplier,
    build_dim_warehouse,
    build_fact_customer_engagement,
    build_fact_inventory_position,
    build_fact_order_service,
)


@dataclass(frozen=True)
class TableBuilder:
    identifier: str
    table: str
    build_fn: Callable[[SparkSession, DataFrame | None], DataFrame]
    primary_key: Sequence[str]
    partition_cols: Sequence[str] = ()
    requires_raw_events: bool = False


TABLE_BUILDERS: Sequence[TableBuilder] = (
    # Dimensions (build first - no dependencies)
    TableBuilder(
        identifier="dim_date",
        table="iceberg.silver.dim_date",
        build_fn=build_dim_date,
        primary_key=("date_sk",),
    ),
    TableBuilder(
        identifier="dim_customer_profile",
        table="iceberg.silver.dim_customer_profile",
        build_fn=build_dim_customer_profile,
        primary_key=("customer_sk",),
    ),
    TableBuilder(
        identifier="dim_product_catalog",
        table="iceberg.silver.dim_product_catalog",
        build_fn=build_dim_product_catalog,
        primary_key=("product_sk",),
    ),
    TableBuilder(
        identifier="dim_supplier",
        table="iceberg.silver.dim_supplier",
        build_fn=build_dim_supplier,
        primary_key=("supplier_sk",),
    ),
    TableBuilder(
        identifier="dim_warehouse",
        table="iceberg.silver.dim_warehouse",
        build_fn=build_dim_warehouse,
        primary_key=("warehouse_sk",),
    ),
    # Facts (build after dimensions - have dependencies)
    TableBuilder(
        identifier="fact_order_service",
        table="iceberg.silver.fact_order_service",
        build_fn=build_fact_order_service,
        primary_key=("order_sk",),
        partition_cols=("date_sk",),
        requires_raw_events=True,
    ),
    TableBuilder(
        identifier="fact_inventory_position",
        table="iceberg.silver.fact_inventory_position",
        build_fn=build_fact_inventory_position,
        primary_key=("inventory_sk",),
        partition_cols=("warehouse_sk",),
        requires_raw_events=True,
    ),
    TableBuilder(
        identifier="fact_customer_engagement",
        table="iceberg.silver.fact_customer_engagement",
        build_fn=build_fact_customer_engagement,
        primary_key=("engagement_sk",),
        partition_cols=("date_sk",),
        requires_raw_events=True,
    ),
)

BUILDER_MAP = {builder.identifier: builder for builder in TABLE_BUILDERS}

__all__ = ["TableBuilder", "TABLE_BUILDERS", "BUILDER_MAP"]
