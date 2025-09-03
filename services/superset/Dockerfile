FROM apache/superset:latest

# Copy initialization script
COPY create_admin.sh /app/pythonpath/create_admin.sh
RUN chmod +x /app/pythonpath/create_admin.sh

# Copy provisioning files
COPY provisioning/ /app/provisioning/

ENTRYPOINT ["/app/pythonpath/create_admin.sh"]
