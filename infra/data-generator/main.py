"""Data Forge Fake Generator – SOLID refactor

purpose: generate realistic events with clear boundaries.
SRP: this module only wires and runs the app; logic lives in services.
DIP: relies on ports and adapters, not concrete libs.
OCP: add streams by registering services; no core changes.
"""
from __future__ import annotations

import random
import signal
from time import sleep

from confluent_kafka.schema_registry import SchemaRegistryClient
import psycopg2

from config import Config
from adapters.kafka.factory import build_kafka
from adapters.kafka.topics import clear_kafka as kafka_clear
from adapters.postgres.repositories import (
    PgInventoryRepository,
    PgPricingRepository,
    PgProductRepository,
    pg_connect,
)
from adapters.postgres.seed import seed_postgres
from adapters.postgres.maintenance import clear_postgres
from services.common import HotCache
from services.interactions import InteractionService
from services.inventory import InventoryService
from services.orders import OrderService
from util.rate_limit import TokenBucket


def wait_for_pg(dsn: str, max_wait_s: int = 60) -> None:
    from time import monotonic

    start = monotonic()
    while True:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return
        except Exception:
            if monotonic() - start > max_wait_s:
                raise
            sleep(1)


def wait_for_sr(url: str, max_wait_s: int = 60) -> None:
    from time import monotonic

    start = monotonic()
    client = SchemaRegistryClient({"url": url})
    while True:
        try:
            client.get_subjects()
            return
        except Exception:
            if monotonic() - start > max_wait_s:
                raise
            sleep(1)


class App:
    """SRP: orchestrate lifecycle. No business logic here."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.running = True
        self.cache = HotCache()

    def _setup(self):
        print("[fakegen] waiting for Postgres…", flush=True)
        wait_for_pg(self.cfg.pg_dsn)
        self.conn = pg_connect(self.cfg)

        # Seed DB and warm caches
        seed_postgres(
            self.conn,
            self.cfg,
            {
                "users": self.cache.users,
                "products": self.cache.products,
                "warehouses": self.cache.warehouses,
                "suppliers": self.cache.suppliers,
            },
        )
        prod_repo = PgProductRepository(self.conn)
        for pid in prod_repo.preload_recent_products(limit=self.cfg.seed_products):
            self.cache.products.append(pid)

        print("[fakegen] waiting for Schema Registry…", flush=True)
        wait_for_sr(self.cfg.schema_registry)
        self.publisher, encoders = build_kafka(self.cfg)

        # Repos and services
        inv_repo = PgInventoryRepository(self.conn)
        price_repo = PgPricingRepository(self.conn)
        self.services = [
            (
                self.cfg.weight_orders,
                OrderService(
                    self.cfg,
                    self.cache,
                    self.publisher,
                    encoders["orders"],
                    encoders["payments"],
                    encoders["shipments"],
                ),
            ),
            (
                self.cfg.weight_interactions,
                InteractionService(
                    self.cfg, self.cache, self.publisher, encoders["interactions"]
                ),
            ),
            (
                self.cfg.weight_inventory_chg,
                InventoryService(
                    self.cfg, self.cache, inv_repo, self.publisher, encoders["invchg"]
                ),
            ),
        ]
        self.inv_repo = inv_repo
        self.price_repo = price_repo

    def _loop(self):
        bucket = TokenBucket(rate=self.cfg.target_eps)
        print(
            f"[fakegen] target EPS={self.cfg.target_eps} (orders={self.cfg.weight_orders}, interactions={self.cfg.weight_interactions}, invchg={self.cfg.weight_inventory_chg})",
            flush=True,
        )
        while self.running:
            bucket.refill()
            # Legacy background mutations retained for parity
            if random.random() < self.cfg.p_update_inventory:
                self.inv_repo.maybe_update_random_inventory()
            if random.random() < self.cfg.p_update_price:
                self.price_repo.maybe_update_random_price()
            while bucket.try_consume(1.0):
                r = random.random()
                acc = 0.0
                for weight, svc in self.services:
                    acc += weight
                    if r < acc:
                        svc.emit()
                        break
            self.publisher.poll()
            sleep(0.005)

    def _teardown(self):
        print("[fakegen] flushing…", flush=True)
        try:
            self.publisher.flush(15)
        finally:
            self.conn.close()

    def run(self):
        self._setup()
        try:
            self._loop()
        finally:
            self._teardown()


def main():
    cfg = Config()
    app = App(cfg)

    def _sig(*_):
        app.running = False
        print("\n🛑 Shutdown signal received – cleaning up playground…")
        try:
            clear_postgres(app.conn)
        except Exception as e:
            print(f"⚠️ Postgres cleanup failed: {e}")
        try:
            kafka_clear(cfg)
        except Exception as e:
            print(f"⚠️ Kafka cleanup failed: {e}")

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    app.run()


if __name__ == "__main__":
    main()
