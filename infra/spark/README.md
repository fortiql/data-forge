# 🧩 Apache Spark

Why: Distributed compute engine for batch and streaming jobs.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- MinIO (S3A), Hive Metastore, Kafka (for streaming jobs)

## 🚀 How

- Start master and workers:
  - `docker compose --profile core up -d spark-master spark-worker-1 spark-worker-2`

- Master UI: `http://localhost:8088`
- Master URL: `spark://spark-master:7077`

- Submit jobs (inside stack): point to master URL and use S3A with MinIO creds.

## 📝 Notes

- Example jobs are mounted under `./spark-jobs`.
- S3A uses `minio:9000` with path‑style access; credentials from `.env`.
- Ensure Hive Metastore is healthy before running Iceberg jobs.

