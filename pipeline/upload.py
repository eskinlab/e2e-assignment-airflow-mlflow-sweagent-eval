"""Upload a completed run folder to S3-compatible object storage (the `upload` step)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def upload_artifacts(run_config: dict[str, Any], run_dir: Path, only: list[str] | None = None) -> dict[str, Any]:
    """Upload run_dir's files to S3. `only`, if given, re-uploads just those relative
    paths instead of walking the whole tree - used to re-upload manifest.json after it's
    patched with this very call's remote_artifact_uri, so the copy in object storage
    doesn't permanently say `null` for the one field that's supposed to point back at it.
    """
    bucket = os.environ["S3_BUCKET"]
    client = _s3_client()
    _ensure_bucket(client, bucket)

    run_id = run_config["run_id"]
    prefix = f"runs/{run_id}/"
    paths = [run_dir / rel for rel in only] if only is not None else run_dir.rglob("*")
    for path in paths:
        if path.is_file():
            key = prefix + path.relative_to(run_dir).as_posix()
            client.upload_file(str(path), bucket, key)

    return {"remote_artifact_uri": f"s3://{bucket}/{prefix}"}
