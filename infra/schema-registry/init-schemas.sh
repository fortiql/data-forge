#!/bin/bash

set -e

SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-"http://localhost:8081"}
SCHEMAS_DIR="/opt/schemas"

echo "Waiting for Schema Registry to be ready..."
while ! curl -f "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; do
    echo "Schema Registry not ready, waiting..."
    sleep 5
done

echo "Registering demo schemas..."

register_schema() {
    local schema_file="$1"
    local subject="$2"
    
    echo "Registering $schema_file as $subject..."
    schema_content=$(cat "$schema_file" | jq -c . | jq -R .)
    payload=$(jq -n --argjson schema "$schema_content" '{"schema": $schema}')
    curl -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        --data "$payload" \
        "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions"
    
    echo " ✓ Registered $subject"
}

register_schema "${SCHEMAS_DIR}/orders-value.avsc" "orders.v1-value"
register_schema "${SCHEMAS_DIR}/payments-value.avsc" "payments.v1-value"
register_schema "${SCHEMAS_DIR}/shipments-value.avsc" "shipments.v1-value"

echo "Schema registration complete!"

# Register new analytics schemas
register_schema "${SCHEMAS_DIR}/inventory-changes-value.avsc" "inventory-changes.v1-value"
register_schema "${SCHEMAS_DIR}/customer-interactions-value.avsc" "customer-interactions.v1-value"

echo "Extended analytics schemas registered!"
