# 🔥 Data Forge — Data Engineering Playground

Your modern data stack playground. Spin up core components of a real data platform and practice end‑to‑end workflows locally.

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
- **Apache Airflow 3** → Workflow orchestration

### 📊 Visualization & Exploration
- **Apache Superset** → Modern BI and data visualization
- **JupyterLab** → Interactive data science environment

### 🏭 Data Generation
- **Data Generator** → Realistic retail data producer for Kafka topics and Postgres tables (see [infra/data-generator/README.md](infra/data-generator/README.md))

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

See [docs/architecture.md](docs/architecture.md) for profile details and commands.

---

## 📚 Learning Path

Follow [docs/learning-path.md](docs/learning-path.md) for a concise, runnable sequence of notebooks.

---

## 🛠️ Development

See [docs/development.md](docs/development.md) for project layout, env vars, and contribution tips.
---

## 🤝 Contributing

- Open issues or PRs with clear scope and steps.
- Follow the docs style: [docs/guidelines.md](docs/guidelines.md).
- Test changes with the relevant compose profiles.

---

## 📄 License

MIT — see `LICENSE`. See service docs for third‑party licenses.

---

## 🌟 Resources

- Docs entrypoint: [docs/](docs/)
- Architecture: [docs/architecture.md](docs/architecture.md)
- Learning Path: [docs/learning-path.md](docs/learning-path.md)
- Development: [docs/development.md](docs/development.md)
- Troubleshooting: [docs/troubleshooting.md](docs/troubleshooting.md)
- Service docs index: [docs/services.md](docs/services.md)
- Guidelines (please read): [docs/guidelines.md](docs/guidelines.md)

Service docs (direct links):
- MinIO: [infra/minio/README.md](infra/minio/README.md)
- Trino: [infra/trino/README.md](infra/trino/README.md)
- Spark: [infra/spark/README.md](infra/spark/README.md)
- Airflow: [infra/airflow/README.md](infra/airflow/README.md)
- ClickHouse: [infra/clickhouse/README.md](infra/clickhouse/README.md)
- Kafka: [infra/kafka/README.md](infra/kafka/README.md)
- Schema Registry: [infra/schema-registry/README.md](infra/schema-registry/README.md)
- Hive Metastore: [infra/hive-metastore/README.md](infra/hive-metastore/README.md)
- JupyterLab: [infra/jupyterlab/README.md](infra/jupyterlab/README.md)
- Superset: [infra/superset/README.md](infra/superset/README.md)
- Postgres: [infra/postgres/README.md](infra/postgres/README.md)
- Redis: [infra/redis/README.md](infra/redis/README.md)
- Debezium: [infra/debezium/README.md](infra/debezium/README.md)
- Kafka UI: [infra/kafka-ui/README.md](infra/kafka-ui/README.md)

---

## 🙏 Acknowledgments

Built on the shoulders of open‑source communities: Apache (Airflow, Spark, Kafka, Trino), ClickHouse, MinIO, Jupyter, Superset, Redis.

---

*The project name "Forge" fits: it's a place where raw metal (data) is hammered into something structured and useful, with you as the smith learning the craft.* ⚒️

---

## 🏭 Data Generation

See [infra/data-generator/README.md](infra/data-generator/README.md) for usage and configuration.
