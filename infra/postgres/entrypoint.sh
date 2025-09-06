#!/bin/bash
docker-entrypoint.sh postgres &
POSTGRES_PID=$!

/init-databases.sh

wait $POSTGRES_PID
