"""SRP: one place to parse and hold configuration.

Keep it simple; no side effects beyond reading environment.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """DIP: business consumes Config, not raw env.

    Values mirror previous globals to preserve behavior.
    """

    # Kafka
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    schema_registry: str = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    topic_orders: str = os.getenv("TOPIC_ORDERS", "orders.v1")
    topic_payments: str = os.getenv("TOPIC_PAYMENTS", "payments.v1")
    topic_shipments: str = os.getenv("TOPIC_SHIPMENTS", "shipments.v1")
    topic_inventory_changes: str = os.getenv("TOPIC_INVENTORY_CHANGES", "inventory-changes.v1")
    topic_customer_interactions: str = os.getenv("TOPIC_CUSTOMER_INTERACTIONS", "customer-interactions.v1")

    # Postgres
    pg_dsn: str = os.getenv(
        "PG_DSN", "host=postgres port=5432 dbname=demo user=admin password=admin"
    )

    # Canonical inventory policy
    canon_inventory: str = os.getenv("CANON_INVENTORY", "postgres")  # "postgres" | "kafka"
    mirror_inventory_to_db: bool = os.getenv("MIRROR_INVENTORY_TO_DB", "false").lower() == "true"

    # Event rates / weights
    target_eps: float = float(os.getenv("TARGET_EPS", "120"))
    weight_orders: float = float(os.getenv("WEIGHT_ORDERS", "0.35"))
    weight_interactions: float = float(os.getenv("WEIGHT_INTERACTIONS", "0.45"))
    weight_inventory_chg: float = float(os.getenv("WEIGHT_INVENTORY_CHG", "0.20"))

    # Seeds
    seed_users: int = int(os.getenv("SEED_USERS", "500"))
    seed_products: int = int(os.getenv("SEED_PRODUCTS", "200"))
    seed_warehouses: int = int(os.getenv("SEED_WAREHOUSES", "5"))
    seed_suppliers: int = int(os.getenv("SEED_SUPPLIERS", "20"))

    # Probabilities
    p_order_has_payment: float = float(os.getenv("P_ORDER_HAS_PAYMENT", "0.7"))
    p_order_has_shipment: float = float(os.getenv("P_ORDER_HAS_SHIPMENT", "0.6"))
    p_update_inventory: float = float(os.getenv("P_UPDATE_INVENTORY", "0.12"))
    p_update_price: float = float(os.getenv("P_UPDATE_PRICE", "0.04"))

    # Late / bad
    p_late_event: float = float(os.getenv("P_LATE_EVENT", "0.05"))
    max_late_minutes: int = int(os.getenv("MAX_LATE_MINUTES", "25"))
    p_bad_record: float = float(os.getenv("P_BAD_RECORD", "0.01"))

