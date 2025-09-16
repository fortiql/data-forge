FROM apache/airflow:3.0.4-python3.12
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark==3.5.6 trino[sqlalchemy]

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl tar && \
    rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.5.6
RUN set -eux; \
    curl -fSL --connect-timeout 20 --max-time 900 --retry 5 --retry-connrefused \
    -o /tmp/spark.tgz "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark; \
    rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

USER airflow
