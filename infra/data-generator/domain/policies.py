"""Policies – OCP: compose alternative behaviors without changing services.

Contains FaultPolicy (bad-record injection) and CanonicalInventory.
"""
from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class FaultPolicy:
    """SRP: holds probabilities for fault injection.

    keep decisions explicit, small, and testable.
    """

    p_bad_record: float = 0.0

    def apply(self, obj: dict) -> dict:
        if self.p_bad_record <= 0:
            return obj
        if random.random() >= self.p_bad_record:
            return obj
        choice = random.choice(["drop_amount", "weird_enum", "null_product"])  # mirrors legacy behavior
        out = dict(obj)
        if choice == "drop_amount" and "amount" in out:
            out["amount"] = None
        elif choice == "weird_enum" and "status" in out:
            out["status"] = "UNKNOWN"
        elif choice == "null_product" and "product_id" in out:
            out["product_id"] = None
        return out


@dataclass(frozen=True)
class CanonicalInventory:
    """Enumerates canonical source. Simplicity protects clarity."""

    source: str  # "postgres" | "kafka"

    def is_postgres(self) -> bool:
        return self.source == "postgres"

