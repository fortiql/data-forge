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
        events = parse_bronze_topic(raw_events, topic).filter(F.col("payload").isNotNull())
        
        extracted = events.select(
            *[F.get_json_object("json_payload", f"$.{field}").alias(alias) 
              for field, alias in extract_fields.items()],
            F.col("event_time"),
            F.col("partition").alias("bronze_partition"),
            F.col("offset").alias("bronze_offset"),
        ).filter(F.col("order_id").isNotNull())
        
        # For orders, no need to get latest since order_id should be unique
        if topic == "orders.v1":
            return extracted
            
        # For payments and shipments, get latest by timestamp
        ts_col = f"{topic.split('.')[0].rstrip('s')}_ts"  # payments -> payment_ts
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

    # Join all events together - explicitly select columns to avoid ambiguity
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

    # Lookup dimension keys
    customers = spark.table("iceberg.silver.dim_customer_profile").select(
        "customer_sk", "user_id", "valid_from", "valid_to"
    )
    
    products = (spark.table("iceberg.silver.dim_product_catalog")
        .where(F.col("is_current"))
        .select("product_sk", "product_id")
    )

    # Lookup date dimension
    dates = spark.table("iceberg.silver.dim_date").select("date_sk", "date_key")
    
    # Join with dimensions using SCD2 temporal join for customers
    enriched = (fact
        .join(customers, 
            (fact.user_id == customers.user_id) & 
            (fact.order_ts >= customers.valid_from) & 
            (fact.order_ts < customers.valid_to), "left")
        .join(products, fact.product_id == products.product_id, "left")
        .join(dates, fact.order_date == dates.date_key, "left")
        .withColumn("processed_at", F.current_timestamp())
        .drop(customers.user_id, products.product_id, dates.date_key)  # Remove duplicate columns
    )

    # Generate fact surrogate key and return Kimball-compliant fact table
    return (enriched
        .withColumn("order_sk", surrogate_key(F.col("order_id"), F.col("order_ts"), F.col("bronze_offset")))
        .select(
            "order_sk",           # Fact surrogate key
            "date_sk",            # Date dimension FK
            "customer_sk",        # Customer dimension FK  
            "product_sk",         # Product dimension FK
            "order_id",           # Degenerate dimension
            "order_ts",           # Measures and attributes
            "order_amount",
            "order_currency",
            "payment_id",         # Degenerate dimensions
            "payment_method",
            "payment_status", 
            "payment_ts",
            "shipment_id",
            "shipment_carrier",
            "shipment_eta_days",
            "shipment_ts",
            "bronze_partition",   # Audit fields
            "bronze_offset",
            "processed_at",
        )
    )
