# 🔥 Data Forge - Data Engeneering Playground

**Your modern data stack playground — a self-contained environment where you can spin up the core building blocks of a real data engineering and analytics platform and practice end-to-end workflows.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-20.10+-blue.svg)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2.0+-blue.svg)](https://docs.docker.com/compose/)

Instead of just reading about "data lakes" or "lakehouses," you actually get to run them. Think of it as a **gym for data engineers** without cloud bills or production risk.

---

## 🎯 What's Inside

Data Forge includes a complete modern data stack with industry-standard tools:

### 🗄️ Storage & Catalog
- **MinIO** → S3-compatible object storage for data lakes
- **Hive Metastore** → Centralized metadata catalog for tables and schemas

### ⚡ Compute Engines  
- **Trino** → Interactive SQL query engine for federated analytics
- **Apache Spark** → Distributed processing for batch and streaming workloads

### 🌊 Streaming & CDC
- **Apache Kafka** → Event streaming platform
- **Schema Registry** → Schema evolution and compatibility  
- **Debezium** → Change data capture from databases

### 🗃️ Databases
- **PostgreSQL** → Primary OLTP database (source system)
- **ClickHouse** → Columnar analytics database (sink)

### 🔄 Orchestration
- **Apache Airflow** → Workflow orchestration with Celery workers
- **Redis** → Message broker and caching layer

### 📊 Visualization & Exploration
- **Apache Superset** → Modern BI and data visualization
- **JupyterLab** → Interactive data science environment

### 🔧 Support Services

### 🏭 Data Generation
- **Data Generator** → Realistic retail data producer for Kafka topics and Postgres tables
- Health checks, monitoring, and modular startup profiles

---

## 🚀 Quick Start

### Prerequisites

- **Docker** 20.10+ 
- **Docker Compose** 2.0+
- **8GB+ RAM** recommended
- **20GB+ disk space** for all services

### 1. Clone & Configure

```bash
git clone https://github.com/fortiql/data-forge.git
cd data-forge

# Copy environment template
cp .env.example .env

# Review and adjust settings
nano .env
```

### 2. Start Core Services

```bash
# Start essential data stack (MinIO, Postgres, ClickHouse, etc.)
docker compose --profile core up -d

# Wait for services to be healthy
docker compose ps
```

### 3. Add Compute & Orchestration

```bash
# Add Airflow for orchestration
docker compose --profile airflow up -d

# Add exploration tools
docker compose --profile explore up -d

# Add realistic data generation
docker compose --profile datagen up -d
```

### 4. Access the Stack

| Service | URL | Default Login |
|---------|-----|---------------|
|**Kafka UI**|http://localhost:8082 |No auth|
| **Airflow** | http://localhost:8085 | `airlfow` / `airflow` |
| **Superset** | http://localhost:8089 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | `minio` / `minio123` |
| **Trino** | http://localhost:8080 | No auth |

---

## 🧩 Architecture Profiles

Data Forge uses Docker Compose profiles for modular deployment:

### `core` Profile
Essential data infrastructure:
- MinIO (object storage)
- PostgreSQL (source database)  
- ClickHouse (analytics database)
- Hive Metastore (catalog)
- Kafka + Schema Registry (streaming)
- Redis (caching)

### `airflow` Profile  
Workflow orchestration:
- Airflow Webserver, Scheduler, Worker
- Celery Executor with Redis backend
- Pre-configured connections to core services

### `explore` Profile
Data exploration and visualization:
- **JupyterLab**: Interactive notebooks with PySpark, Trino, and analytics libraries pre-installed

Start with:
```bash
docker compose --profile explore up -d
```

Access:
- JupyterLab: [http://localhost:8888](http://localhost:8888)

### `datagen` Profile
Realistic data generation:
- Data Generator with 5 Kafka topics (orders, payments, shipments, inventory, interactions)
- 8 Postgres reference tables with realistic business relationships
- Configurable event rates and data quality patterns
Data exploration and visualization:
- JupyterLab with PySpark, Trino, and analytics libraries
- Apache Superset for dashboards and BI

---

## 📚 Learning Path

- **Quick Connections Example**  
  _notebooks/examples/quick-connections.ipynb_  
  Connect to core services (Spark, Trino, MinIO, etc.) and validate your environment.

- **Streaming Fundamentals**  
  _notebooks/lessons/streaming/streaming-fundamentals-lesson.ipynb_  
  Learn the basics of streaming data, Kafka, and Spark.

- **Multi-Topic Streaming with Schema Registry**  
  _notebooks/lessons/streaming/multi-topic-streaming-lesson.ipynb_  
  Ingest and validate events from multiple Kafka topics using Spark and Schema Registry.

- **Bronze Layer with Iceberg**  
  _notebooks/lessons/streaming/bronze-layer-iceberg-example.ipynb_  
  Build a production-grade raw events (bronze) table using Iceberg, Kafka, and Avro.

- **Spark DML and Iceberg Time Travel**  
  _notebooks/lessons/iceberg/spark-iceberg-dml.ipynb_  
  Perform DML operations, time travel, and table optimization in Iceberg.

> See the `notebooks/` directory for hands-on Jupyter notebooks and step-by-step guides.

---

## 🛠️ Development

### Project Structure

```
data-forge/
├── infra/              # Service configurations
│   ├── airflow/        # Airflow setup, DAGs, plugins
│   ├── jupyterlab/     # Jupyter config, notebooks
│   ├── superset/       # Superset config, dashboards
│   ├── data-generator/  # Realistic retail data generator
│   └── */              # Other service configs
├── notebooks/          # Sample Jupyter notebooks
├── docs/               # Documentation
├── docker-compose.yml  # Service definitions
└── .env               # Environment variables
```

### Adding New Services

1. Create service directory in `infra`
2. Add Dockerfile and configuration
3. Update `docker-compose.yml` with service definition
4. Add to appropriate profile (`core`, `airflow`, `explore`)
5. Update documentation

### Environment Variables

Data Forge uses environment variables for service configuration. Copy the example file to get started:

```bash
cp .env.example .env
```

Key variables you might want to customize:

```bash
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
CLICKHOUSE_USER=admin  
CLICKHOUSE_PASSWORD=admin

MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123

AIRFLOW_ADMIN_USERNAME=airflow
AIRFLOW_ADMIN_PASSWORD=airflow

SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
```
---

## 🤝 Contributing

We welcome contributions! Here's how to get started:

### 🐛 Bug Reports & Feature Requests
- Check existing [issues](https://github.com/fortiql/data-forge/issues)
- Create detailed issue with reproduction steps
- Use issue templates for consistency

### 🛠️ Code Contributions
1. **Fork** the repository
2. **Create** feature branch: `git checkout -b feature/amazing-feature`
3. **Follow** our [documentation guidelines](docs/guidelines.md)
4. **Test** your changes with `docker compose up`
5. **Submit** pull request with clear description

### 📖 Documentation
- Follow the [documentation guidelines](docs/guidelines.md)
- Update relevant docs when adding features
- Keep examples practical and tested

### 🎯 Areas We Need Help
- Additional sample notebooks and tutorials
- Performance optimization guides
- Integration with more data tools
- CI/CD improvements
- Security hardening

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

### Third-Party Licenses

Data Forge integrates multiple open-source projects, each with their own licenses:

- **Apache Airflow** - Apache License 2.0
- **Apache Spark** - Apache License 2.0  
- **Apache Kafka** - Apache License 2.0
- **Trino** - Apache License 2.0
- **ClickHouse** - Apache License 2.0
- **PostgreSQL** - PostgreSQL License
- **MinIO** - GNU AGPL v3.0 (server), Apache 2.0 (client libraries)
- **JupyterLab** - BSD 3-Clause License
- **Apache Superset** - Apache License 2.0
- **Redis** - Redis Source Available License 2.0 (RSALv2)

See individual service documentation for complete license information.

---

## 🌟 Resources

### 📚 Documentation
- [Service Documentation](docs/) - Individual service guides  

### 🎓 Learning Resources  
- [Sample Notebooks](notebooks/) - Ready-to-run Jupyter examples
- [Tutorial Series](docs/tutorials/) - Step-by-step learning paths

### 🗺️ Roadmap
- [Project Roadmap](https://github.com/fortiql/data-forge/projects) - Planned features
- [Release Notes](https://github.com/fortiql/data-forge/releases) - What's new

---

## 🙏 Acknowledgments

Data Forge is built on the shoulders of giants. Special thanks to:

- The **Apache Software Foundation** for Airflow, Spark, Kafka, and Trino
- The **ClickHouse** team for their blazing-fast analytics database
- **MinIO** for S3-compatible object storage
- The **Jupyter** project for interactive computing
- **Preset** and the Superset community for modern BI tools
- All the maintainers and contributors of the open-source data ecosystem

---

*The project name "Forge" fits: it's a place where raw metal (data) is hammered into something structured and useful, with you as the smith learning the craft.* ⚒️

---

## 🏭 Data Generation

Data Forge includes a realistic retail data generator that produces streaming events and reference data for testing and development.

### 🚀 Quick Start with Data Generation

```bash
# Start core services with data generation
docker compose --profile core --profile datagen up -d

# Watch live data flowing
docker compose logs -f data-generator
```

### 📊 Generated Data

The data generator produces realistic retail data across **5 Kafka topics** and **8 Postgres tables**:

**Streaming Events (Kafka + Avro):**
- `orders` → Customer orders with products and amounts
- `payments` → Payment processing (CARD/APPLE_PAY/PAYPAL)  
- `shipments` → Shipping events with carriers and ETAs
- `inventory_changes` → Stock movements (RESTOCK/SALE/DAMAGE/RETURN)
- `customer_interactions` → User behavior (PAGE_VIEW/SEARCH/CART_ADD)

**Reference Data (Postgres):**
- `users`, `products`, `warehouses`, `suppliers` → Core entities
- `customer_segments`, `product_suppliers` → Business relationships
- `inventory`, `warehouse_inventory` → Stock tracking

### ⚙️ Realistic Business Logic

- **Diurnal patterns** → Traffic varies 0.4x to 1.4x throughout the day
- **Entity relationships** → Users have sessions, orders link to payments/shipments
- **Multi-warehouse** → Inventory distributed across regions with supplier relationships
- **Data quality** → 0.1% bad records and 5% late events for testing resilience

### 🎛️ Configuration

Control data generation via environment variables:

```bash
# Event rates (events per second)
TARGET_EPS=10

# Event distribution weights
WEIGHT_ORDERS=0.6          # Order/payment/shipment events  
WEIGHT_INTERACTIONS=0.3    # Customer behavior events
WEIGHT_INVENTORY_CHG=0.1   # Inventory change events

# Data architecture patterns
CANON_INVENTORY=postgres   # Inventory source of truth: postgres|kafka
MIRROR_INVENTORY_TO_DB=false  # Mirror Kafka events to database
```

See [Data Generator documentation](infra/data-generator/README.md) for complete configuration options.

