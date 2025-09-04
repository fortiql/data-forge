FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER $NB_UID
RUN pip install --no-cache-dir \
    trino[sqlalchemy] \
    sqlalchemy-trino \
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
