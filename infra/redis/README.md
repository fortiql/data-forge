# 🧩 Redis

Why: Message broker and caching layer; Airflow Celery backend.

## ⚙️ Profile

- `core`, `airflow`

## 🔗 Dependencies

- None

## 🚀 How

- Start service:
  - `docker compose --profile core up -d redis`

- Port: `6379`

## 📝 Notes

- Used as Celery broker/result backend for Airflow.

