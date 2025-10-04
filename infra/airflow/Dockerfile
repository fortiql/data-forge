FROM apache/airflow:3.0.6-python3.12
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
RUN set -eux; \
    mkdir -p /opt/spark/jars; \
    curl -fSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"; \
    curl -fSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"; \
    curl -fSL -o /opt/spark/jars/kafka-clients-3.5.0.jar "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"; \
    curl -fSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar"; \
    curl -fSL -o /opt/spark/jars/iceberg-aws-bundle-1.9.2.jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar"; \
    curl -fSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"; \
    curl -fSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.791.jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar"; \
    curl -fSL -o /opt/spark/jars/commons-pool2-2.12.0.jar "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"; 
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV SPARK_JARS_DIR=/opt/spark/jars
ENV PYTHONPATH="/opt/spark/jobs:${PYTHONPATH}"

USER airflow
