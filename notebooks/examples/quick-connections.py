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
