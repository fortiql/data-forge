# 🧩 MinIO

Why: S3‑compatible object storage for Spark, Trino, and Iceberg tables.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- None (other services depend on MinIO)

## 🚀 How

- Start service:
  - `docker compose --profile core up -d minio`

- Console: `http://localhost:9001`
- S3 endpoint: `http://localhost:9000`
- Credentials: from `.env` → `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`

- Spark S3A (typical settings in this stack):
  - Endpoint: `minio:9000`
  - Path‑style access enabled
  - Access/secret from MinIO creds

- Trino S3 (iceberg.properties):
  - `s3.endpoint=http://minio:9000`
  - `s3.aws-access-key=minio`
  - `s3.aws-secret-key=minio123`

## 📝 Notes

- Local dev: path‑style access and http (no TLS) for simplicity.
- Rotate credentials if you expose services beyond localhost.

— Docs: see [docs/services.md](../../docs/services.md) and [docs/guidelines.md](../../docs/guidelines.md).
