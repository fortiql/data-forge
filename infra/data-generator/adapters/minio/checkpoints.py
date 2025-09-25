"""Utilities to clear Spark checkpoint prefixes stored in MinIO (S3-compatible)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Iterable

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


def _parse_s3a_path(path: str) -> tuple[str, str]:
    if not path.startswith("s3a://"):
        raise ValueError(f"Expected s3a:// path, got: {path}")
    without_scheme = path[len("s3a://") :]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix.rstrip("/") + "/"


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    secure: bool = False

    @classmethod
    def from_env(cls) -> "MinioConfig":
        import os

        endpoint = os.getenv("S3_ENDPOINT", os.getenv("MINIO_ENDPOINT", "minio:9000"))
        access_key = os.getenv("MINIO_ROOT_USER", "minio")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
        region = os.getenv("AWS_REGION", "us-east-1")
        secure = os.getenv("S3_SECURE", "false").lower() == "true"
        return cls(endpoint=endpoint, access_key=access_key, secret_key=secret_key, region=region, secure=secure)


def _build_client(cfg: MinioConfig):
    scheme = "https" if cfg.secure else "http"
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        endpoint_url=f"{scheme}://{cfg.endpoint}",
        region_name=cfg.region,
        config=BotoConfig(signature_version="s3v4"),
    )


def clear_checkpoints(paths: Iterable[str], *, config: MinioConfig | None = None) -> None:
    cfg = config or MinioConfig.from_env()
    client = _build_client(cfg)

    for path in paths:
        bucket, prefix = _parse_s3a_path(path)
        if prefix == "/":
            prefix = ""
        print(f"🧹 Clearing checkpoint prefix s3a://{bucket}/{prefix}")

        try:
            paginator = client.get_paginator("list_objects_v2")
            deleted = 0
            pending: list[dict[str, str]] = []

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                contents = page.get("Contents", [])
                if not contents and deleted == 0:
                    continue
                for obj in contents:
                    pending.append({"Key": obj["Key"]})
                    if len(pending) == 1000:
                        client.delete_objects(Bucket=bucket, Delete={"Objects": pending})
                        deleted += len(pending)
                        pending = []

            if pending:
                client.delete_objects(Bucket=bucket, Delete={"Objects": pending})
                deleted += len(pending)

            if deleted:
                print(f"  ✅ Deleted {deleted} objects")
            else:
                print("  ℹ️  Nothing to delete")
        except ClientError as exc:  # pragma: no cover - network failures logged
            print(f"  ⚠️ Unable to clear {path}: {exc}", file=sys.stderr)
