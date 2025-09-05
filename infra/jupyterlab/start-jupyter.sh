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
