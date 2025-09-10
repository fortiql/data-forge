FROM apache/hive:standalone-metastore-4.1.0

USER root

RUN mkdir -p /tmp/deps && \
    curl -o /tmp/deps/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /tmp/deps/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o /tmp/deps/iceberg-hive-runtime-1.9.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.9.2/iceberg-hive-runtime-1.9.2.jar && \
    cp /tmp/deps/*.jar /opt/hive/lib/ && \
    cp /tmp/deps/*.jar /opt/hadoop/share/hadoop/common/lib/ && \
    rm -rf /tmp/deps

COPY config/hive-site.xml /opt/hive/conf/hive-site.xml
COPY config/core-site.xml /opt/hive/conf/core-site.xml
COPY entrypoint.sh /entrypoint.sh

RUN chmod 644 /opt/hive/lib/hadoop-aws-3.3.4.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar /opt/hive/lib/iceberg-hive-runtime-1.9.2.jar /opt/hadoop/share/hadoop/common/lib/hadoop-aws-3.3.4.jar /opt/hadoop/share/hadoop/common/lib/aws-java-sdk-bundle-1.12.262.jar /opt/hadoop/share/hadoop/common/lib/iceberg-hive-runtime-1.9.2.jar /opt/hive/conf/hive-site.xml /opt/hive/conf/core-site.xml && \
    chmod +x /entrypoint.sh

USER hive

ENTRYPOINT ["/entrypoint.sh"]
