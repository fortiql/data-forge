#!/bin/bash

set -x

: "${DB_DRIVER:=derby}"

SKIP_SCHEMA_INIT="${IS_RESUME:-false}"
[[ $VERBOSE = "true" ]] && VERBOSE_MODE="--verbose" || VERBOSE_MODE=""

function initialize_hive {
  COMMAND="-initOrUpgradeSchema"
  if [ "$(echo "$HIVE_VER" | cut -d '.' -f1)" -lt "4" ]; then
     COMMAND="-${SCHEMA_COMMAND:-initSchema}"
  fi
  "$HIVE_HOME/bin/schematool" -dbType "$DB_DRIVER" "$COMMAND" "$VERBOSE_MODE"
  if [ $? -eq 0 ]; then
    echo "Initialized Hive Metastore Server schema successfully.."
  else
    echo "Hive Metastore Server schema initialization failed!"
    exit 1
  fi
}

export HIVE_CONF_DIR=$HIVE_HOME/conf
if [ -d "${HIVE_CUSTOM_CONF_DIR:-}" ]; then
  find "${HIVE_CUSTOM_CONF_DIR}" -type f -exec \
    ln -sfn {} "${HIVE_CONF_DIR}"/ \;
  export HADOOP_CONF_DIR=$HIVE_CONF_DIR
fi

export HADOOP_CLASSPATH="/opt/hive/lib/hadoop-aws-3.3.4.jar:/opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar:/opt/hive/lib/iceberg-hive-runtime-1.9.2.jar:$HADOOP_CLASSPATH"

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS \
  -Dfs.s3a.threads.keepalivetime=60000 \
  -Dfs.s3a.connection.timeout=200000 \
  -Dfs.s3a.connection.establish.timeout=120000 \
  -Dfs.s3a.connection.request.timeout=60000 \
  -Dfs.s3a.executor.capacity=16 \
  -Dfs.s3a.threads.core=15 \
  -Dfs.s3a.threads.max=96 \
  -Dfs.s3a.max.total.tasks=50 \
  -Dfs.s3a.retry.limit=3 \
  -Dfs.s3a.retry.interval=500 \
  -Dfs.s3a.socket.recv.buffer=65536 \
  -Dfs.s3a.socket.send.buffer=65536 \
  -Dfs.s3a.connection.ttl=60000 \
  -Dfs.s3a.connection.idle.timeout=30000 \
  -Dfs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  -Dfs.s3a.committer.threads.keepalivetime=60000 \
  -Dfs.s3a.prefetch.block.size=8388608 \
  -Dfs.s3a.readahead.range=65536 \
  -Dfs.s3a.endpoint=http://minio:9000 \
  -Dfs.s3a.access.key=minio \
  -Dfs.s3a.secret.key=minio123 \
  -Dfs.s3a.path.style.access=true \
  -Dfs.s3a.connection.ssl.enabled=false \
  -Dfs.s3a.multipart.purge=false \
  -Dfs.s3a.multipart.purge.age=0 \
  -Dfs.s3a.multipart.size=67108864 \
  -Dfs.s3a.multipart.threshold=134217728 \
  -Dfs.s3a.assumed.role.session.duration=3600 \
  -Dfs.s3a.session.token= \
  -Dfs.s3a.listing.page.size=5000 \
  -Dfs.s3a.change.detection.mode=none \
  -Dfs.s3a.change.detection.version.required=false \
  -Dfs.s3a.change.detection.source=none"

if [[ "${SKIP_SCHEMA_INIT}" == "false" ]]; then
  initialize_hive
fi

export METASTORE_PORT=${METASTORE_PORT:-9083}
exec "$HIVE_HOME/bin/start-metastore"
