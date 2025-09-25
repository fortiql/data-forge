# ⚙️ Development Setup

Why: Set up, extend, and debug the stack efficiently.

## 🚀 How

- Clone and configure env:
  - `git clone https://github.com/fortiql/data-forge.git && cd data-forge`
  - `cp .env.example .env` and adjust to your machine.

- Project structure (high level):
  - [infra/](../infra/) → service configs and Dockerfiles
  - [notebooks/](../notebooks/) → examples and lessons
  - [docs/](./) → documentation
  - [`docker-compose.yml`](../docker-compose.yml) → services and profiles

- Build and run (example):
  - `docker compose --profile core up -d`
  - `docker compose ps` to check health

- Environment variables (common):
  - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
  - `POSTGRES_CDC_USER`, `POSTGRES_CDC_PASSWORD`
  - `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`
  - `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
  - Airflow/Superset admin credentials

## 📝 Notes

- Keep changes minimal and tested with the compose profiles you affect.
- For docs, follow [guidelines.md](guidelines.md).
- Open an issue for substantial changes or new services.
