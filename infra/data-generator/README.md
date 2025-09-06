# 🧩 Data Generator

Realistic retail data generator producing 5 Kafka topics and 8 Postgres tables with configurable event rates and business logic.

## ⚙️ Configuration

Core settings via environment variables:

```bash
TARGET_EPS=10                    # Events per second (base rate)
WEIGHT_ORDERS=0.6               # Proportion for order/payment/shipment events
WEIGHT_INTERACTIONS=0.3         # Proportion for customer interactions
WEIGHT_INVENTORY_CHG=0.1        # Proportion for inventory changes

CANON_INVENTORY=postgres        # Inventory source of truth: postgres|kafka
MIRROR_INVENTORY_TO_DB=false    # Mirror Kafka inventory changes to DB
```

Performance tuning:
```bash
P_ORDER_HAS_PAYMENT=0.85        # Probability order generates payment
P_ORDER_HAS_SHIPMENT=0.75       # Probability order generates shipment
P_UPDATE_INVENTORY=0.02         # Background inventory mutation rate
P_UPDATE_PRICE=0.001            # Background price change rate
P_LATE_EVENT=0.05               # Probability of out-of-order events
P_BAD_RECORD=0.001              # Data quality issues for testing
```

## 🚀 Running

**Standalone:**
```bash
docker compose --profile datagen up -d
```

**With full stack:**
```bash
docker compose --profile core --profile datagen up -d
```

🛑 **Dependencies:** postgres (healthy), kafka (healthy), schema-registry (started)

## 🧩 Data Model

**Kafka Topics (Avro):**
- `orders` → Order events with user/product/amount
- `payments` → Payment status for orders (CARD/APPLE_PAY/PAYPAL)
- `shipments` → Shipping events with carrier/ETA
- `inventory_changes` → Stock movements (RESTOCK/SALE/DAMAGE/RETURN)
- `customer_interactions` → User behavior (PAGE_VIEW/SEARCH/CART_ADD)

**Postgres Tables:**
- `users`, `products`, `warehouses`, `suppliers` → Reference data
- `customer_segments`, `product_suppliers` → Enrichment data
- `inventory`, `warehouse_inventory` → Stock tracking

## ⚙️ Business Logic

**Realistic Patterns:**
- Diurnal traffic variation (0.4x to 1.4x base rate)
- LRU caches for entity relationships (recent users/products)
- Session-aware customer interactions
- Multi-warehouse inventory distribution
- Supplier cost/lead time modeling

**Data Quality Features:**
- 0.1% bad records for dead letter testing
- 5% out-of-order events for late data handling
- Configurable inventory consistency (Postgres vs Kafka canonical)
- Automatic schema registration with fallback schemas

## 🛑 Notes

- **Memory:** ~200MB baseline, scales with entity cache sizes
- **Network:** Burst-capable producer with backpressure handling
- **Postgres:** Seeding is idempotent; safe to restart
- **Performance:** Token bucket rate limiting with configurable burst capacity

Event distribution adapts to time-of-day patterns. Inventory can be Postgres-canonical (read-modify-write) or Kafka-canonical (event-sourced) depending on architecture needs.
