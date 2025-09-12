FROM minio/mc:latest AS mc

FROM minio/minio:latest

COPY --from=mc /usr/bin/mc /usr/bin/mc

COPY config/ /root/.minio/
COPY setup-minio.sh /usr/local/bin/setup-minio.sh

RUN chmod +x /usr/local/bin/setup-minio.sh
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
