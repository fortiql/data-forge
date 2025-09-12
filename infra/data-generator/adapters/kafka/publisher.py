"""Kafka Publisher – DIP adapter for EventPublisher.

Backpressure-safe publish wrapping SerializingProducer.
"""
from __future__ import annotations

from typing import Mapping, Optional

from confluent_kafka import SerializingProducer

from ports.event_publisher import EventPublisher


class KafkaPublisher(EventPublisher):
    """SRP: only concern is delivery to Kafka.

    ISP: keep interface narrow.
    """

    def __init__(self, producer: SerializingProducer) -> None:
        self._producer = producer

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    headers=list((headers or {}).items()),
                    on_delivery=lambda err, _msg: print(f"[deliver-err] {err}") if err else None,
                )
                break
            except BufferError:
                self._producer.poll(0.05)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout: float = 15.0) -> None:
        self._producer.flush(timeout)
