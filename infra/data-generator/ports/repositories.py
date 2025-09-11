"""DIP Ports – repositories for Postgres access.

SRP: define needs; implementations live under adapters/postgres.
"""
from typing import Iterable, Optional, Tuple


class InventoryRepository:
    def read_qty_reserved(self, warehouse_id: str, product_id: str) -> Tuple[int, int]:  # (qty, reserved)
        raise NotImplementedError

    def upsert_qty_reserved(self, warehouse_id: str, product_id: str, qty: int, reserved: int) -> None:
        raise NotImplementedError

    def maybe_update_random_inventory(self) -> None:
        """Side utility retained for parity with legacy random updates."""
        raise NotImplementedError


class PricingRepository:
    def maybe_update_random_price(self) -> None:
        raise NotImplementedError


class ProductRepository:
    def preload_recent_products(self, limit: int) -> Iterable[str]:
        raise NotImplementedError

