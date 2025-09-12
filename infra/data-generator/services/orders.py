"""Orders Service – SRP: build and publish Order -> Payment -> Shipment.

OCP: swap encoders/publishers without code changes here.
"""
from __future__ import annotations

import random

from config import Config
from domain.enums import CARRIERS, CURRENCIES, PAYMENT_METHODS, PAYMENT_STATUS
from domain.policies import FaultPolicy
from ports.event_publisher import EventPublisher
from ports.schema_encoder import SchemaEncoder
from services.common import (
    HotCache,
    now_ms,
    rid,
    ensure_str,
    ensure_float,
    ensure_int,
    clamp_enum,
)


class OrderService:
    def __init__(
        self,
        cfg: Config,
        cache: HotCache,
        publisher: EventPublisher,
        order_enc: SchemaEncoder,
        payment_enc: SchemaEncoder,
        shipment_enc: SchemaEncoder,
    ) -> None:
        self.cfg = cfg
        self.cache = cache
        self.publisher = publisher
        self.order_enc = order_enc
        self.payment_enc = payment_enc
        self.shipment_enc = shipment_enc
        self.faults = FaultPolicy(cfg.p_bad_record)

    def emit(self) -> None:
        base_ts = now_ms()
        order_ts = base_ts
        order_id = rid("ord")
        user_id = self.cache.pick_user()
        product_id = self.cache.pick_product()
        amount = round(random.uniform(5.0, 900.0), 2)
        trace_id = rid("tr")

        if not user_id or not product_id:
            return

        order = {
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product_id,
            "amount": amount,
            "currency": random.choice(CURRENCIES),
            "ts": order_ts,
        }
        order = self.faults.apply(order)
        order["order_id"] = ensure_str(order.get("order_id"), order_id)
        order["user_id"] = ensure_str(order.get("user_id"), user_id)
        order["product_id"] = ensure_str(order.get("product_id"), product_id)
        order["amount"] = ensure_float(order.get("amount"), amount)
        order["currency"] = ensure_str(order.get("currency"), order.get("currency", "USD"))
        order["ts"] = ensure_int(order.get("ts"), order_ts)
        self.publisher.publish(
            self.cfg.topic_orders,
            key=order_id,
            value=self.order_enc.encode(self.cfg.topic_orders, order),
            headers={"trace_id": trace_id, "entity": "order", "source": "fakegen"},
        )
        self.cache.orders.append(order_id)

        if random.random() < self.cfg.p_order_has_payment:
            pay_ts = base_ts + random.randint(10, 2000)
            pay_status = random.choices(PAYMENT_STATUS, weights=[2, 5, 1])[0]
            pay_method = random.choice(PAYMENT_METHODS)
            pay = {
                "payment_id": rid("pay"),
                "order_id": order_id,
                "method": pay_method,
                "status": pay_status,
                "ts": pay_ts,
            }
            pay = self.faults.apply(pay)
            pay["payment_id"] = ensure_str(pay.get("payment_id"), pay["payment_id"])
            pay["order_id"] = ensure_str(pay.get("order_id"), order_id)
            pay["method"] = clamp_enum(pay.get("method"), PAYMENT_METHODS, pay_method)
            pay["status"] = clamp_enum(pay.get("status"), PAYMENT_STATUS, pay_status)
            pay["ts"] = ensure_int(pay.get("ts"), pay_ts)
            self.publisher.publish(
                self.cfg.topic_payments,
                key=pay["payment_id"],
                value=self.payment_enc.encode(self.cfg.topic_payments, pay),
                headers={"trace_id": trace_id, "entity": "payment", "source": "fakegen"},
            )

            if pay["status"] == "SETTLED" and random.random() < self.cfg.p_order_has_shipment:
                shp_ts = base_ts + random.randint(5000, 60000)
                carrier = random.choice(CARRIERS)
                eta = random.choice([1, 2, 3, 4, 5])
                shp = {
                    "shipment_id": rid("shp"),
                    "order_id": order_id,
                    "carrier": carrier,
                    "eta_days": eta,
                    "ts": shp_ts,
                }
                shp = self.faults.apply(shp)
                shp["shipment_id"] = ensure_str(shp.get("shipment_id"), shp["shipment_id"])
                shp["order_id"] = ensure_str(shp.get("order_id"), order_id)
                shp["carrier"] = ensure_str(shp.get("carrier"), carrier)
                shp["eta_days"] = ensure_int(shp.get("eta_days"), eta)
                shp["ts"] = ensure_int(shp.get("ts"), shp_ts)
                self.publisher.publish(
                    self.cfg.topic_shipments,
                    key=shp["shipment_id"],
                    value=self.shipment_enc.encode(self.cfg.topic_shipments, shp),
                    headers={"trace_id": trace_id, "entity": "shipment", "source": "fakegen"},
                )
