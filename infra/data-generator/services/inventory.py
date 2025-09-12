"""Inventory Service – SRP: build and publish inventory change events.

DIP: depends on repositories, encoder, publisher, and policy.
"""
from __future__ import annotations

import random
from typing import Tuple

from config import Config
from domain.enums import INVENTORY_CHANGE_TYPES
from domain.policies import CanonicalInventory, FaultPolicy
from ports.event_publisher import EventPublisher
from ports.repositories import InventoryRepository
from ports.schema_encoder import SchemaEncoder
from services.common import HotCache, now_ms, rid, ensure_str, ensure_int


class InventoryService:
    def __init__(
        self,
        cfg: Config,
        cache: HotCache,
        repo: InventoryRepository,
        publisher: EventPublisher,
        encoder: SchemaEncoder,
    ) -> None:
        self.cfg = cfg
        self.cache = cache
        self.repo = repo
        self.publisher = publisher
        self.encoder = encoder
        self.faults = FaultPolicy(cfg.p_bad_record)
        self.canon = CanonicalInventory(cfg.canon_inventory)

    def _delta_reason_supplier(self, change_type: str, prev: int) -> Tuple[int, str, str | None, float | None]:
        if change_type == "RESTOCK":
            delta = random.randint(50, 200)
            return delta, "Supplier delivery", self.cache.pick_supplier(), round(random.uniform(10.0, 200.0), 2)
        if change_type == "SALE":
            delta = -random.randint(1, max(1, min(10, prev)))
            return delta, "Customer order fulfillment", None, None
        if change_type == "DAMAGE":
            delta = -random.randint(1, max(1, min(5, prev)))
            return delta, "Warehouse damage", None, None
        if change_type == "RETURN":
            delta = random.randint(1, 5)
            return delta, "Customer return", None, None
        if change_type == "TRANSFER":
            delta = -random.randint(1, max(1, min(20, prev))) if prev > 0 else 0
            return delta, f"Transfer to {self.cache.pick_warehouse()}", None, None
        delta = random.randint(-10, 10)
        return delta, "Inventory audit", None, None

    def _apply_reserved_first(self, delta: int, reserved: int) -> tuple[int, int]:
        if delta < 0:
            use_res = min(reserved, -delta)
            reserved -= use_res
            delta += use_res
        return delta, reserved

    def emit(self) -> None:
        wid = self.cache.pick_warehouse()
        pid = self.cache.pick_product()
        ctype = random.choice(INVENTORY_CHANGE_TYPES)
        if self.canon.is_postgres():
            prev, reserved = self.repo.read_qty_reserved(wid, pid)
        else:
            prev, reserved = random.randint(0, 100), 0
        delta, reason, supplier_id, cost_per_unit = self._delta_reason_supplier(ctype, prev)
        delta, reserved = self._apply_reserved_first(delta, reserved)
        new_qty = max(0, prev + delta)

        change = {
            "change_id": rid("chg"),
            "warehouse_id": wid,
            "product_id": pid,
            "change_type": ctype,
            "quantity_delta": delta,
            "previous_qty": prev,
            "new_qty": new_qty,
            "reason": reason,
            "order_id": (self.cache.orders[-1] if (ctype == "SALE" and self.cache.orders) else None),
            "supplier_id": supplier_id,
            "cost_per_unit": cost_per_unit,
            "ts": now_ms(),
        }
        change = self.faults.apply(change)
        change["change_id"] = ensure_str(change.get("change_id"), change["change_id"])
        change["warehouse_id"] = ensure_str(change.get("warehouse_id"), wid)
        change["product_id"] = ensure_str(change.get("product_id"), pid)
        change["change_type"] = ensure_str(change.get("change_type"), ctype)
        change["quantity_delta"] = ensure_int(change.get("quantity_delta"), delta)
        change["previous_qty"] = ensure_int(change.get("previous_qty"), prev)
        change["new_qty"] = ensure_int(change.get("new_qty"), new_qty)
        change["ts"] = ensure_int(change.get("ts"), change["ts"])

        payload = self.encoder.encode(self.cfg.topic_inventory_changes, change)
        self.publisher.publish(
            self.cfg.topic_inventory_changes,
            key=f"{wid}:{pid}",
            value=payload,
            headers={"entity": "inventory_change", "source": "fakegen"},
        )

        if self.canon.is_postgres() or self.cfg.mirror_inventory_to_db:
            self.repo.upsert_qty_reserved(wid, pid, new_qty, reserved)
