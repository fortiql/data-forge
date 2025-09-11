"""Kafka Schema Encoders – DIP adapters for SchemaEncoder.

Fallback to embedded Avro when registry has no subject.
"""
from __future__ import annotations

from typing import Any

from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient

try:
    from confluent_kafka.schema_registry.avro import AvroSerializer
except Exception:  # pragma: no cover
    AvroSerializer = None  # type: ignore

from ports.schema_encoder import SchemaEncoder


class AvroSchemaEncoder(SchemaEncoder):
    """SRP: encode dicts to Avro bytes for a topic."""

    def __init__(self, sr: SchemaRegistryClient, subject: str, embedded_schema: str) -> None:
        if AvroSerializer is None:
            raise RuntimeError(
                "AvroSerializer not available; install confluent-kafka[avro]"
            )
        try:
            meta = sr.get_latest_version(subject)
            schema_str = meta.schema.schema_str
        except Exception:
            schema_str = embedded_schema
        self._sr = sr
        self._serializer = AvroSerializer(sr, schema_str)
        self._subject = subject

    def encode(self, topic: str, value: Any) -> bytes:
        return self._serializer(value, SerializationContext(topic, MessageField.VALUE))
