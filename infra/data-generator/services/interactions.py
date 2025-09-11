"""Interactions Service – SRP: build and publish CustomerInteraction events.

DIP: depends on EventPublisher + SchemaEncoder + policies.
"""
from __future__ import annotations

import random

from config import Config
from domain.enums import COUNTRIES, INTERACTION_TYPES, OS_LIST, BROWSERS
from domain.policies import FaultPolicy
from ports.event_publisher import EventPublisher
from ports.schema_encoder import SchemaEncoder
from services.common import HotCache, now_ms


class InteractionService:
    """OCP: extend behaviors via policies and encoders without changing this class."""

    def __init__(
        self,
        cfg: Config,
        cache: HotCache,
        publisher: EventPublisher,
        encoder: SchemaEncoder,
    ) -> None:
        self.cfg = cfg
        self.cache = cache
        self.publisher = publisher
        self.encoder = encoder
        self.faults = FaultPolicy(cfg.p_bad_record)

    def emit(self) -> None:
        ts = now_ms()
        user_id = self.cache.pick_user()
        session_id = self.cache.get_or_create_session(user_id)
        itype = random.choice(INTERACTION_TYPES)
        interaction = {
            "interaction_id": f"int_{session_id[:6]}_{random.randint(1000,9999)}",
            "user_id": user_id,
            "session_id": session_id,
            "interaction_type": itype,
            "product_id": self.cache.pick_product()
            if itype in {"PAGE_VIEW", "CART_ADD", "CART_REMOVE", "WISHLIST_ADD", "REVIEW"}
            else None,
            "search_query": f"search_{random.randint(100,999)}" if itype == "SEARCH" else None,
            "page_url": f"/products/{random.randint(1000,9999)}" if itype == "PAGE_VIEW" else None,
            "duration_ms": random.randint(1000, 300000) if itype == "PAGE_VIEW" else None,
            "user_agent": random.choice(BROWSERS),
            "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "country": random.choice(COUNTRIES),
            "ts": ts,
        }
        interaction = self.faults.apply(interaction)
        payload = self.encoder.encode(self.cfg.topic_customer_interactions, interaction)
        self.publisher.publish(
            self.cfg.topic_customer_interactions,
            key=session_id,
            value=payload,
            headers={"entity": "customer_interaction", "source": "fakegen"},
        )
