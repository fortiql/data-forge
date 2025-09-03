#!/bin/bash
# Install trino if not already installed
/usr/local/bin/pip install trino --target=/app/.venv/lib/python3.10/site-packages

# Start superset normally
exec "$@"
