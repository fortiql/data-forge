#!/bin/bash

/etc/confluent/docker/run &
SCHEMA_REGISTRY_PID=$!

echo "Starting schema registration in background..."
nohup bash -c '
    sleep 10  # Give Schema Registry time to start
    /opt/init-schemas.sh
' > /tmp/schema-init.log 2>&1 &

wait $SCHEMA_REGISTRY_PID
