"""Kafka Factory – SRP: build producer and encoders.

DIP: callers receive ports, not concrete libs.
"""
from __future__ import annotations

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from config import Config
from adapters.kafka.publisher import KafkaPublisher
from adapters.kafka.serializers import AvroSchemaEncoder


def build_kafka(config: Config):
    sr_client = SchemaRegistryClient({"url": config.schema_registry})

    producer = SerializingProducer(
        {
            "bootstrap.servers": config.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 25,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "key.serializer": StringSerializer("utf_8"),
            "queue.buffering.max.messages": 200000,
        }
    )

    # Embedded Avro schemas (preserved from legacy main)
    orders_schema = '{"type":"record","name":"Order","namespace":"demo","fields":[{"name":"order_id","type":"string"},{"name":"user_id","type":"string"},{"name":"product_id","type":"string"},{"name":"amount","type":"double"},{"name":"currency","type":"string","default":"USD"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    payments_schema = '{"type":"record","name":"Payment","namespace":"demo","fields":[{"name":"payment_id","type":"string"},{"name":"order_id","type":"string"},{"name":"method","type":{"type":"enum","name":"Method","symbols":["CARD","APPLE_PAY","PAYPAL"]}},{"name":"status","type":{"type":"enum","name":"Status","symbols":["PENDING","SETTLED","FAILED","UNKNOWN"]}},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    shipments_schema = '{"type":"record","name":"Shipment","namespace":"demo","fields":[{"name":"shipment_id","type":"string"},{"name":"order_id","type":"string"},{"name":"carrier","type":"string"},{"name":"eta_days","type":"int","default":3},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    invchg_schema = '{"type":"record","name":"InventoryChange","namespace":"demo","fields":[{"name":"change_id","type":"string"},{"name":"warehouse_id","type":"string"},{"name":"product_id","type":"string"},{"name":"change_type","type":{"type":"enum","name":"ChangeType","symbols":["RESTOCK","SALE","DAMAGE","RETURN","TRANSFER","ADJUSTMENT"]}},{"name":"quantity_delta","type":"int"},{"name":"previous_qty","type":"int"},{"name":"new_qty","type":"int"},{"name":"reason","type":["null","string"],"default":null},{"name":"order_id","type":["null","string"],"default":null},{"name":"supplier_id","type":["null","string"],"default":null},{"name":"cost_per_unit","type":["null","double"],"default":null},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'
    interactions_schema = '{"type":"record","name":"CustomerInteraction","namespace":"demo","fields":[{"name":"interaction_id","type":"string"},{"name":"user_id","type":"string"},{"name":"session_id","type":"string"},{"name":"interaction_type","type":{"type":"enum","name":"InteractionType","symbols":["PAGE_VIEW","SEARCH","CART_ADD","CART_REMOVE","WISHLIST_ADD","REVIEW","SUPPORT_CHAT"]}},{"name":"product_id","type":["null","string"],"default":null},{"name":"search_query","type":["null","string"],"default":null},{"name":"page_url","type":["null","string"],"default":null},{"name":"duration_ms","type":["null","long"],"default":null},{"name":"user_agent","type":"string"},{"name":"ip_address","type":"string"},{"name":"country","type":"string"},{"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}]}'

    encoders = {
        "orders": AvroSchemaEncoder(sr_client, f"{config.topic_orders}-value", orders_schema),
        "payments": AvroSchemaEncoder(sr_client, f"{config.topic_payments}-value", payments_schema),
        "shipments": AvroSchemaEncoder(sr_client, f"{config.topic_shipments}-value", shipments_schema),
        "invchg": AvroSchemaEncoder(
            sr_client, f"{config.topic_inventory_changes}-value", invchg_schema
        ),
        "interactions": AvroSchemaEncoder(
            sr_client, f"{config.topic_customer_interactions}-value", interactions_schema
        ),
    }

    return KafkaPublisher(producer), encoders
