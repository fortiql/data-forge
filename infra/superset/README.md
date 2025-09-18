# 🧩 Apache Superset

Why: Modern BI and data visualization for dashboards and SQL exploration.

## ⚙️ Profile

- `core`

## 🔗 Dependencies

- Trino (as a default SQL engine)

## 🚀 How

- Start service:
  - `docker compose --profile core up -d superset`

- UI: `http://localhost:8089` (maps container 8088)
- Admin user: from `.env` (`SUPERSET_ADMIN_USERNAME` / `SUPERSET_ADMIN_PASSWORD`)

## 📝 Notes

- Entrypoint creates the admin user on first run.
- Provisioning mounted under [provisioning](provisioning).
