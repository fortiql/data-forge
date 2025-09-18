#!/bin/bash
set -e

echo "Starting MinIO setup..."

MAX_RETRIES=30
RETRY_COUNT=0

echo "Waiting for MinIO to be ready..."
until /usr/bin/mc alias set minio http://localhost:9000 minio minio123; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "ERROR: MinIO failed to start after $MAX_RETRIES attempts"
    exit 1
  fi
  echo "Waiting for MinIO... (attempt $RETRY_COUNT/$MAX_RETRIES)"
  sleep 2
done

echo "MinIO is ready. Setting up buckets and policies..."

BUCKETS=(
  "iceberg"
  "sandbox"
  "checkpoints"
)

for bucket in "${BUCKETS[@]}"; do
  if ! /usr/bin/mc ls "minio/$bucket" >/dev/null 2>&1; then
    echo "Creating bucket: $bucket"
    /usr/bin/mc mb "minio/$bucket"
  else
    echo "Bucket already exists: $bucket"
  fi
  echo "Setting public policy for bucket: $bucket"
  /usr/bin/mc anonymous set public "minio/$bucket" >/dev/null 2>&1 || true
done

echo ""
echo "Verifying bucket setup..."
/usr/bin/mc ls minio/

echo ""
echo "✅ MinIO setup complete! Created/verified buckets:"
for bucket in "${BUCKETS[@]}"; do
  echo "  - $bucket (public access enabled)"
done
