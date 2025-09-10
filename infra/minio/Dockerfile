FROM minio/minio:latest

COPY config/ /root/.minio/
COPY setup-minio.sh /usr/local/bin/setup-minio.sh

RUN chmod +x /usr/local/bin/setup-minio.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
