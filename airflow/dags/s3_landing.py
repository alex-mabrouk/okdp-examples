"""
Landing remote files in the object store
Streams a download straight into S3, without staging it on disk, for the bronze
DAGs. Reads the storage identity the Airflow package gives the task pods.
"""
import os
from pathlib import Path
from urllib.parse import urlparse

import spark_submit


def s3_client():
    import boto3
    from botocore.config import Config

    missing = [v for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"{', '.join(missing)} missing: the task pod has no object-store identity"
        )
    return boto3.client(
        "s3",
        endpoint_url=spark_submit.s3_endpoint(),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        # Landing the whole of France means 8 MB parts that outlast botocore's
        # 60 s read timeout, and nothing retries them by default.
        config=Config(
            connect_timeout=60,
            read_timeout=600,
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def _already_landed(s3, bucket, key, expected_size):
    """A source file is immutable for a given millésime: same size means done."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except Exception:
        return False
    return expected_size is not None and head["ContentLength"] == expected_size


def land_url(s3, url, bucket, prefix):
    """Stream one remote file to s3://<bucket>/<prefix>/<its file name>; return the key."""
    import requests
    from boto3.s3.transfer import TransferConfig

    with requests.get(url, stream=True, timeout=(30, 600)) as response:
        response.raise_for_status()
        file_name = Path(urlparse(response.url).path).name
        size = response.headers.get("Content-Length")
        size = int(size) if size else None
        key = f"{prefix}/{file_name}"

        if _already_landed(s3, bucket, key, size):
            print(f"already landed, skipping: s3://{bucket}/{key}")
            return key

        pretty = f"{size / 1e6:.1f} MB" if size else "unknown size"
        print(f"landing {response.url} ({pretty}) -> s3://{bucket}/{key}")
        response.raw.decode_content = True
        # Fewer, larger parts off a single non-seekable stream: 940 MB goes from
        # ~113 parts across 10 threads down to ~15 across 4.
        s3.upload_fileobj(
            response.raw,
            bucket,
            key,
            Config=TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=4),
        )

    print(f"landed s3://{bucket}/{key}")
    return key


def delete_prefix(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
