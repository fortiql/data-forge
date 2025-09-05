# 🔥 Data Forge

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
```

### 4. Access the Stack

| Service | URL | Default Login |
|---------|-----|---------------|
| **Airflow** | http://localhost:8080 | \`admin\` / \`admin\` |
| **Superset** | http://localhost:8088 | \`admin\` / \`admin\` |
| **JupyterLab** | http://localhost:8888 | Token in logs |
| **MinIO Console** | http://localhost:9001 | \`minio\` / \`minio123\` |
| **Trino** | http://localhost:8081 | No auth |

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
- JupyterLab with PySpark, Trino, and analytics libraries
- Apache Superset for dashboards and BI

---

## 📚 Learning Paths

### 🔰 Beginner: Data Pipeline Basics

TBD

### 🎓 Intermediate: CDC & Real-time Analytics  

TBD

### 🚀 Advanced: Data Lake & Lakehouse

TBD

---

## 🛠️ Development

### Project Structure

```
data-forge/
├── infra/              # Service configurations
│   ├── airflow/        # Airflow setup, DAGs, plugins
│   ├── jupyterlab/     # Jupyter config, notebooks
│   ├── superset/       # Superset config, dashboards
│   └── */              # Other service configs
├── notebooks/          # Sample Jupyter notebooks
├── docs/               # Documentation
├── docker-compose.yml  # Service definitions
└── .env               # Environment variables
```

### Adding New Services

1. Create service directory in \`infra/\`
2. Add Dockerfile and configuration
3. Update \`docker-compose.yml\` with service definition
4. Add to appropriate profile (\`core\`, \`airflow\`, \`explore\`)
5. Update documentation

### Environment Variables

Data Forge uses environment variables for service configuration. Copy the example file to get started:

```bash
cp .env.example .env
```

Key variables you might want to customize:

```bash
# Database Credentials
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
CLICKHOUSE_USER=admin  
CLICKHOUSE_PASSWORD=admin

# Object Storage
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123

# Airflow Admin User
AIRFLOW_ADMIN_USERNAME=airflow
AIRFLOW_ADMIN_PASSWORD=airflow

# Superset Admin User  
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
2. **Create** feature branch: \`git checkout -b feature/amazing-feature\`
3. **Follow** our [documentation guidelines](docs/guidelines.md)
4. **Test** your changes with \`docker compose up\`
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
