FROM debezium/connect:3.0.0.Final

USER root

RUN microdnf install -y unzip \
 && curl -fsSL https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/8.0.0/confluentinc-kafka-connect-avro-converter-8.0.0.zip -o /tmp/avro-converter.zip \
 && unzip -q /tmp/avro-converter.zip -d /kafka/connect \
 && rm -f /tmp/avro-converter.zip \
 && microdnf clean all

COPY config /kafka/connectors
COPY start-with-connectors.sh /usr/local/bin/start-with-connectors.sh

RUN chmod +x /usr/local/bin/start-with-connectors.sh \
 && chown -R kafka:kafka /kafka/connectors /usr/local/bin/start-with-connectors.sh

USER kafka

ENTRYPOINT ["/usr/local/bin/start-with-connectors.sh"]
