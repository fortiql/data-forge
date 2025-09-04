# Data Forge Notebooks

This directory contains example notebooks and templates for working with the Data Forge platform.

## 📁 Directory Structure

```
notebooks/
├── examples/                          # Example notebooks
│   ├── service-connections-validation.ipynb  # Complete service validation
│   ├── quick-connections.ipynb       # Ready-to-use connection snippets
│   └── quick-connections.py          # Python script version
├── data-engineering/                 # Data engineering workflows
└── analytics/                        # Data analysis notebooks
```

## 🚀 Getting Started

### 1. Start JupyterLab
```bash
# Start with exploration profile
docker compose --profile core --profile explore up -d

# Access JupyterLab at: http://localhost:8888
```

### 2. Run Connection Validation
Open `examples/service-connections-validation.ipynb` to:
- ✅ Test all service connections
- 📊 Generate connectivity report
- 🔧 Get troubleshooting guidance

### 3. Use Quick Connections
Open `examples/quick-connections.ipynb` for:
- 🔌 Ready-to-use connection code
- 📚 Code snippets for each service
- ⚡ Quick health checks

## 🔌 Available Connections

| Service | Purpose | Default Port | Connection Method |
|---------|---------|--------------|-------------------|
| **PostgreSQL** | Primary database | 5432 | psycopg2, SQLAlchemy |
| **ClickHouse** | Analytics database | 8123 | clickhouse-connect |
| **MinIO** | Object storage | 9000 | boto3 (S3 API) |
| **Kafka** | Message streaming | 9092 | kafka-python |
| **Redis** | Cache layer | 6379 | redis-py |
| **Trino** | SQL query engine | 8080 | trino-python-client |
| **Spark** | Distributed computing | 7077 | PySpark |

## 🌍 Environment Variables

All connection credentials are automatically available in JupyterLab:

```python
import os

# Database credentials
POSTGRES_USER = os.getenv('POSTGRES_USER')        # admin
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')  # admin
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')    # admin

# Object storage
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER')    # minio
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD')  # minio123

# Service URLs
TRINO_URL = os.getenv('TRINO_URL')                 # http://trino:8080
SPARK_MASTER = os.getenv('SPARK_MASTER_URL')      # spark://spark-master:7077
```

## 📊 Pre-installed Packages

JupyterLab comes with these data engineering packages:

### Database Connectors
- `psycopg2-binary` - PostgreSQL
- `clickhouse-connect` - ClickHouse
- `trino[sqlalchemy]` - Trino
- `redis` - Redis
- `sqlalchemy` - SQL toolkit

### Data Processing
- `pandas` - Data analysis
- `polars` - Fast DataFrames
- `pyarrow` - Columnar data
- `pyspark` - Apache Spark
- `duckdb` - Embedded analytics

### Streaming & Messaging
- `kafka-python` - Kafka client
- `confluent-kafka` - Confluent Kafka

### Object Storage
- `boto3` - AWS S3 API
- `s3fs` - S3 filesystem

### Visualization
- `plotly` - Interactive plots
- `matplotlib` - Static plots
- `seaborn` - Statistical plots
- `bokeh` - Web-ready plots

### JupyterLab Extensions
- `jupyterlab-git` - Git integration
- `jupyterlab-lsp` - Language server
- `black` - Code formatting
- `plotly` - Interactive widgets
- `nbresuse` - Resource monitoring

## 🏃‍♂️ Quick Start Examples

### Load Data from PostgreSQL
```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres:5432/{POSTGRES_DB}")
df = pd.read_sql("SELECT * FROM your_table", engine)
```

### Query ClickHouse
```python
import clickhouse_connect

client = clickhouse_connect.get_client(host='clickhouse', username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
result = client.query("SELECT * FROM your_table LIMIT 10")
df = result.result_as_dataframe()
```

### Use MinIO S3 Storage
```python
import boto3

s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                  aws_access_key_id=MINIO_ROOT_USER,
                  aws_secret_access_key=MINIO_ROOT_PASSWORD)
                  
# Upload file
s3.put_object(Bucket='my-bucket', Key='data.csv', Body=df.to_csv())
```

### Spark Processing
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataForge").master(SPARK_MASTER).getOrCreate()
df_spark = spark.read.csv("s3a://my-bucket/data.csv", header=True)
```

### Stream with Kafka
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                        value_serializer=lambda x: json.dumps(x).encode())
producer.send('my-topic', {'message': 'Hello Kafka!'})
```

## 🔧 Troubleshooting

### Service Not Accessible?
1. Check if services are running: `docker compose ps`
2. Check service logs: `docker compose logs [service-name]`
3. Verify network connectivity from JupyterLab container

### Connection Errors?
1. Ensure environment variables are loaded
2. Check service health in validation notebook
3. Restart services if needed: `docker compose restart [service-name]`

### Missing Packages?
```python
# Install additional packages in notebook
!pip install package-name
```

## 📚 Additional Resources

- [Trino Documentation](https://trino.io/docs/)
- [Apache Spark Guide](https://spark.apache.org/docs/latest/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
- [ClickHouse Python Driver](https://clickhouse.com/docs/en/integrations/python)
- [MinIO Python SDK](https://docs.min.io/docs/python-client-quickstart-guide.html)
