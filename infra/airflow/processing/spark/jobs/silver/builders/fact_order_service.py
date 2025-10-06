"""Order service fact builder."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from silver.common import parse_bronze_topic, surrogate_key


def build_fact_order_service(spark: SparkSession, raw_events: DataFrame | None) -> DataFrame:
    if raw_events is None:
        raise ValueError("raw_events dataframe is required for fact_order_service")

    def extract_latest_by_topic(topic: str, extract_fields: dict) -> DataFrame:
        """Extract and get latest event by order_id for a given topic."""
        events = parse_bronze_topic(raw_events, topic).filter(F.col("json_payload").isNotNull())
        
        extracted = events.select(
            *[F.get_json_object("json_payload", f"$.{field}").alias(alias) 
              for field, alias in extract_fields.items()],
            F.col("event_time"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        ).filter(F.col("order_id").isNotNull())
        if topic == "orders.v1":
            return extracted
        ts_col = f"{topic.split('.')[0].rstrip('s')}_ts"
        return (extracted
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("order_id").orderBy(F.desc(ts_col))))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

    orders = extract_latest_by_topic("orders.v1", {
        "order_id": "order_id",
        "user_id": "user_id", 
        "product_id": "product_id",
        "amount": "order_amount",
        "currency": "order_currency",
        "ts": "order_ts"
    }).withColumn("order_amount", F.col("order_amount").cast("decimal(10,2)")) \
     .withColumn("order_ts", F.to_timestamp("order_ts"))

    payments = extract_latest_by_topic("payments.v1", {
        "order_id": "order_id",
        "payment_id": "payment_id",
        "method": "payment_method",
        "status": "payment_status", 
        "ts": "payment_ts"
    }).withColumn("payment_ts", F.to_timestamp("payment_ts"))

    shipments = extract_latest_by_topic("shipments.v1", {
        "order_id": "order_id",
        "shipment_id": "shipment_id",
        "carrier": "shipment_carrier",
        "eta_days": "shipment_eta_days",
        "ts": "shipment_ts"
    }).withColumn("shipment_eta_days", F.col("shipment_eta_days").cast("int")) \
     .withColumn("shipment_ts", F.to_timestamp("shipment_ts"))
    fact = (orders.alias("o")
        .join(payments.alias("p"), F.col("o.order_id") == F.col("p.order_id"), "left")
        .join(shipments.alias("s"), F.col("o.order_id") == F.col("s.order_id"), "left")
        .select(
            F.col("o.order_id"),
            F.col("o.user_id"),
            F.col("o.product_id"),
            F.col("o.order_amount"),
            F.col("o.order_currency"),
            F.col("o.order_ts"),
            F.col("o.event_time"),
            F.col("o.bronze_partition"),
            F.col("o.bronze_offset"),
            F.col("p.payment_id"),
            F.col("p.payment_method"),
            F.col("p.payment_status"),
            F.col("p.payment_ts"),
            F.col("s.shipment_id"),
            F.col("s.shipment_carrier"),
            F.col("s.shipment_eta_days"),
            F.col("s.shipment_ts"),
        )
        .withColumn("order_ts", F.coalesce("order_ts", "event_time"))
        .withColumn("order_date", F.to_date("order_ts"))
    )
    customers = (spark.table("iceberg.silver.dim_customer_profile")
        .select("customer_sk", "user_id", "valid_from", "valid_to")
        .alias("cust")
    )

    products = (spark.table("iceberg.silver.dim_product_catalog")
        .select("product_sk", "product_id", "valid_from", "valid_to")
        .alias("prod")
    )

    dates = (spark.table("iceberg.silver.dim_date")
        .select("date_sk", "date_key")
        .alias("d")
    )

    fact = fact.alias("f")

    enriched = (fact
        .join(
            customers,
            (F.col("f.user_id") == F.col("cust.user_id"))
            & (F.col("f.order_ts") >= F.col("cust.valid_from"))
            & (F.col("f.order_ts") < F.col("cust.valid_to")),
            "left",
        )
        .join(
            products,
            (F.col("f.product_id") == F.col("prod.product_id"))
            & (F.col("f.order_ts") >= F.col("prod.valid_from"))
            & (F.col("f.order_ts") < F.col("prod.valid_to")),
            "left",
        )
        .join(dates, F.col("f.order_date") == F.col("d.date_key"), "left")
        .withColumn("processed_at", F.current_timestamp())
    )

    return (enriched
        .withColumn(
            "order_sk",
            surrogate_key(
                F.col("f.order_id"),
                F.col("f.order_ts"),
                F.col("f.bronze_offset"),
            ),
        )
        .select(
            "order_sk",
            F.col("d.date_sk").alias("date_sk"),
            F.col("cust.customer_sk").alias("customer_sk"),
            F.col("prod.product_sk").alias("product_sk"),
            F.col("f.order_id").alias("order_id"),
            F.col("f.order_ts").alias("order_ts"),
            F.col("f.order_amount").alias("order_amount"),
            F.col("f.order_currency").alias("order_currency"),
            F.col("f.payment_id").alias("payment_id"),
            F.col("f.payment_method").alias("payment_method"),
            F.col("f.payment_status").alias("payment_status"),
            F.col("f.payment_ts").alias("payment_ts"),
            F.col("f.shipment_id").alias("shipment_id"),
            F.col("f.shipment_carrier").alias("shipment_carrier"),
            F.col("f.shipment_eta_days").alias("shipment_eta_days"),
            F.col("f.shipment_ts").alias("shipment_ts"),
            F.col("f.bronze_partition").alias("bronze_partition"),
            F.col("f.bronze_offset").alias("bronze_offset"),
            "processed_at",
        )
    )
