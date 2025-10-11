FROM bitnamilegacy/spark:3.5.6

USER 0

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bzip2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG PYTHON_VERSION=3.12
ENV MAMBA_ROOT_PREFIX=/opt/micromamba

RUN curl -Ls "https://micro.mamba.pm/api/micromamba/linux-64/latest" | tar -xj -C /tmp && \
    install -m 0755 /tmp/bin/micromamba /usr/local/bin/micromamba && \
    rm -rf /tmp/bin

RUN micromamba create -y -p /opt/python python=${PYTHON_VERSION} pip && \
    micromamba clean -afy

ENV PATH="/opt/python/bin:${PATH}"

RUN /opt/python/bin/pip install --no-cache-dir pyarrow pandas fastavro confluent-kafka[avro]

RUN set -eux; \
    mkdir -p "${SPARK_HOME}/jars"; \
    curl -fSL -o "${SPARK_HOME}/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/kafka-clients-3.5.0.jar" "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/commons-pool2-2.12.0.jar" "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/iceberg-aws-bundle-1.9.2.jar" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"; \
    curl -fSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.791.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.791/aws-java-sdk-bundle-1.12.791.jar"

COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

RUN chown -R 1001:1001 ${SPARK_HOME}/conf

ENV SPARK_JARS_DIR="${SPARK_HOME}/jars"
ENV PYTHONPATH="/opt/spark/jobs:${PYTHONPATH}"
ENV PYSPARK_PYTHON="/opt/python/bin/python"
ENV PYSPARK_DRIVER_PYTHON="/opt/python/bin/python"

USER 1001
