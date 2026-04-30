from __future__ import annotations

from pathlib import Path


def download_to_path(*, bucket: str, key: str, dest: str | Path) -> Path:
    """Download an S3 object to a local file.

    Credentials are resolved by boto3's default chain (env, task role, etc.).
    """

    import boto3  # type: ignore

    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3")
    s3.download_file(bucket, key, str(dest_path))
    return dest_path

