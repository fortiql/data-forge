#!/bin/bash
set -euo pipefail

CONFIG_DIR="${DEBEZIUM_CONNECTOR_CONFIG_DIR:-/kafka/connectors}"
CONNECT_HOST="${DEBEZIUM_CONNECT_HOST:-localhost}"
CONNECT_PORT="${DEBEZIUM_CONNECT_PORT:-8083}"
MAX_ATTEMPTS="${DEBEZIUM_CONNECT_WAIT_ATTEMPTS:-60}"
SLEEP_SECONDS="${DEBEZIUM_CONNECT_WAIT_INTERVAL:-5}"

/docker-entrypoint.sh start &
CONNECT_PID=$!
trap 'kill -TERM ${CONNECT_PID} >/dev/null 2>&1 || true' TERM INT

available=false
for ((attempt=1; attempt<=MAX_ATTEMPTS; attempt++)); do
  if curl -sSf "http://${CONNECT_HOST}:${CONNECT_PORT}/connectors" >/dev/null 2>&1; then
    available=true
    break
  fi
  echo "Waiting for Kafka Connect to be ready (${attempt}/${MAX_ATTEMPTS})..."
  sleep "${SLEEP_SECONDS}"
done

if ! $available; then
  echo "Kafka Connect not ready; skipping connector provisioning." >&2
else
  if [ -d "${CONFIG_DIR}" ] && compgen -G "${CONFIG_DIR}"'/*.json' >/dev/null 2>&1; then
    for config_path in "${CONFIG_DIR}"/*.json; do
      [ -f "${config_path}" ] || continue
      connector_name="$(basename "${config_path}" .json)"
      echo "Applying connector config: ${connector_name}"
      http_code=$(curl -sS -o /tmp/connector-response -w "%{http_code}" \
        -X PUT "http://${CONNECT_HOST}:${CONNECT_PORT}/connectors/${connector_name}/config" \
        -H "Content-Type: application/json" \
        --data "@${config_path}" || true)
      if [[ "${http_code}" =~ ^2 ]]; then
        cat /tmp/connector-response
      else
        echo "Connector ${connector_name} provisioning failed (HTTP ${http_code})." >&2
        cat /tmp/connector-response >&2
      fi
      rm -f /tmp/connector-response
    done
  else
    echo "No connector configuration files found in ${CONFIG_DIR}."
  fi
fi

wait ${CONNECT_PID}
