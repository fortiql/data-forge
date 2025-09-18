# 🧩 Apache Airflow

Why: Orchestrate workflows and schedule data pipelines.

## ⚙️ Profile

- `airflow`

## 🔗 Dependencies

- PostgreSQL, Redis

## 🚀 How

- Start Airflow components:
  - `docker compose --profile airflow up -d`

- API Server: `http://localhost:8085`
- Admin user: from `.env` (`AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`)

- Init job runs migrations, creates admin, and configures `spark_default` connection.

## 📝 Notes

- Wait for `airflow-init` to complete before workers start.
- DAGs, logs, plugins are mounted from [dags](dags), [logs](logs), and [plugins](plugins).
