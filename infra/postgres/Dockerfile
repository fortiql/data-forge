FROM postgres:16

COPY init-databases.sh /init-databases.sh
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /init-databases.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
