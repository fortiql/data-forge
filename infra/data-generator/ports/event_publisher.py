"""DIP Port – EventPublisher.

Speak in the language of the domain. The infrastructure implements.
"""
from typing import Mapping, Optional


class EventPublisher:
    """ISP: a narrow interface sufficient for services.

    publish: send encoded message with headers to a topic.
    """

    def publish(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:  # pragma: no cover - interface only
        raise NotImplementedError

