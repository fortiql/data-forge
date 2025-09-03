#!/bin/bash
set -e

# Install packages compatible with SQLAlchemy v1.4.54 (used by Superset)
pip install --user "trino<0.320" "sqlalchemy-trino<0.5.0"

# Ensure the user-installed packages are in the Python path
export PYTHONPATH="/app/superset_home/.local/lib/python3.10/site-packages:$PYTHONPATH"

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-User}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true
superset init
superset import_datasources -p /app/provisioning/databases.yaml || true
exec superset run -h 0.0.0.0 -p 8088
