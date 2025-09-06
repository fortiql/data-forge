FROM confluentinc/cp-schema-registry:latest

USER root
RUN yum install -y --allowerasing curl jq && yum clean all

COPY schemas/ /opt/schemas/
COPY init-schemas.sh /opt/init-schemas.sh

COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/init-schemas.sh /opt/entrypoint.sh

USER appuser

ENTRYPOINT ["/opt/entrypoint.sh"]
