#!/bin/bash
echo "🚀 Starting JupyterLab for Data Engineering Stack..."
export SPARK_HOME=/usr/local/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_OPTS="--driver-java-options=-Xms512m --driver-java-options=-Xmx1g"
export PYSPARK_SUBMIT_ARGS="--driver-memory 1g --executor-memory 1g pyspark-shell"

echo "☕ Java Home: $JAVA_HOME"
echo "⚡ Spark Home: $SPARK_HOME"
echo "🐍 Python Path: $PYTHONPATH"
java -version 2>&1 | head -1
mkdir -p /home/jovyan/work/examples
echo "📦 Installing additional extensions..."
pip install --quiet --no-cache-dir \
    jupyterlab-drawio 2>/dev/null || echo "  ⚠️  drawio extension not available" \
    && pip install --quiet --no-cache-dir jupyterlab-spellchecker 2>/dev/null || echo "  ⚠️  spellchecker extension not available" \
    && pip install --quiet --no-cache-dir aquirdturtle_collapsible_headings 2>/dev/null || echo "  ⚠️  collapsible headings not available"

jupyter server extension enable --py nbresuse --sys-prefix 2>/dev/null || true
git config --global user.name "Data Engineer"
git config --global user.email "engineer@dataforge.local"
git config --global init.defaultBranch main
cat > /home/jovyan/work/examples/quick-connections.py << 'EOF'
"""
Quick Connection Examples for Data Engineering Stack

This file provides ready-to-use connection examples for all services
in your data engineering stack.
"""

# Database Connections
TRINO_CONFIG = {
    'host': 'trino',
    'port': 8080,
    'user': 'user',
    'catalog': 'iceberg',
    'schema': 'default'
}

CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'port': 8123,
    'username': 'admin',
    'password': 'admin'
}

POSTGRES_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'postgres',
    'user': 'admin',
    'password': 'admin'
}

REDIS_CONFIG = {
    'host': 'redis',
    'port': 6379,
    'db': 0
}

# Streaming
KAFKA_CONFIG = {
    'bootstrap_servers': ['kafka:9092']
}

# Object Storage
MINIO_CONFIG = {
    'endpoint_url': 'http://minio:9000',
    'aws_access_key_id': 'minio',
    'aws_secret_access_key': 'minio123'
}

# Example connection functions
def get_trino_connection():
    import trino
    return trino.dbapi.connect(**TRINO_CONFIG)

def get_clickhouse_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

def get_postgres_connection():
    import psycopg2
    return psycopg2.connect(**POSTGRES_CONFIG)

def get_redis_client():
    import redis
    return redis.Redis(**REDIS_CONFIG)

def get_kafka_producer():
    from kafka import KafkaProducer
    import json
    return KafkaProducer(
        **KAFKA_CONFIG,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_minio_client():
    import boto3
    from botocore.client import Config
    return boto3.client('s3', **MINIO_CONFIG, config=Config(signature_version='s3v4'))

print("🔗 Connection examples loaded! Use help(get_trino_connection) for details.")
EOF

echo "✅ JupyterLab startup completed!"
echo "📊 Available services:"
echo "  - Trino Query Engine: http://trino:8080"
echo "  - ClickHouse Analytics: http://clickhouse:8123"
echo "  - PostgreSQL Database: postgres:5432"
echo "  - Redis Cache: redis:6379"
echo "  - Kafka Streaming: kafka:9092"
echo "  - MinIO Storage: http://minio:9000"
echo "  - Superset BI: http://superset:8088"
exec jupyter lab --config=/home/jovyan/.jupyter/jupyter_lab_config.py
