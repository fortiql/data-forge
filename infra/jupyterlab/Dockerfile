FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.5.6
ARG PYTHON_VERSION=3.12
RUN set -eux; \
    curl -fSL --connect-timeout 20 --max-time 900 --retry 5 --retry-connrefused \
    -o /tmp/spark.tgz "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark; \
    rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

SHELL ["bash","-o","pipefail","-c"]

ENV CONDA_ENV_PATH=/opt/conda/envs/notebook

RUN mamba create -y -n notebook python=${PYTHON_VERSION} pip jupyterlab notebook && \
    mamba clean -afy

ENV PATH="${CONDA_ENV_PATH}/bin:${PATH}"
ENV CONDA_DEFAULT_ENV=notebook
ENV PYSPARK_PYTHON="${CONDA_ENV_PATH}/bin/python"
ENV PYSPARK_DRIVER_PYTHON="${CONDA_ENV_PATH}/bin/python"

USER $NB_UID
RUN pip install --no-cache-dir \
    trino[sqlalchemy] \
    sqlalchemy-trino \
    confluent-kafka[avro] \
    avro-python3 \
    requests \
    psycopg2-binary \
    redis \
    clickhouse-connect \
    clickhouse-driver \
    kafka-python \
    confluent-kafka \
    boto3 \
    s3fs \
    pyarrow \
    pandas \
    polars \
    duckdb \
    plotly \
    bokeh \
    altair \
    seaborn \
    matplotlib \
    pyspark==3.5.6 \
    delta-spark==3.0.0

RUN pip install --no-cache-dir \
    jupyterlab-git \
    jupyterlab-lsp \
    'python-lsp-server[all]' \
    black \
    isort \
    plotly \
    ipywidgets \
    nbresuse \
    jupyterlab-execute-time

RUN jupyter server extension enable --py jupyterlab_git --sys-prefix

COPY jupyter_lab_config.py /home/jovyan/.jupyter/
COPY start-jupyter.sh /usr/local/bin/

RUN mkdir -p /home/jovyan/work/examples
RUN mkdir -p /home/jovyan/work/data-engineering
RUN mkdir -p /home/jovyan/work/analytics

USER root
RUN chown -R jovyan:users /home/jovyan/.jupyter && \
    chmod +x /usr/local/bin/start-jupyter.sh
USER $NB_UID

WORKDIR /home/jovyan/work

CMD ["start-jupyter.sh"]
