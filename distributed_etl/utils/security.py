from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


class SecretNotFound(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SecretProvider:
    """Fetch secrets from AWS Secrets Manager with env fallback."""

    aws_region: str | None = None

    def get(self, name: str, *, env_fallback: str | None = None) -> str:
        if env_fallback is not None:
            v = os.getenv(env_fallback)
            if v:
                return v
            raise SecretNotFound(
                f"Env var '{env_fallback}' is not set (needed for '{name}'. "
                "Add it to your .env locally, or set AWS_REGION and use Secrets Manager)."
            )

        allow_aws_without_env_fallback = (
            os.getenv("USE_AWS_SECRETS", "").strip().lower() in {"1", "true", "yes"}
        )
        if not allow_aws_without_env_fallback:
            raise SecretNotFound(
                "No env fallback env var provided and AWS Secrets Manager lookups are disabled. "
                "Pass env_fallback='SUPABASE_URL' (etc.), set the env vars, "
                "or enable AWS with USE_AWS_SECRETS=true plus AWS_REGION and credentials."
            )

        try:
            import boto3  # type: ignore
        except Exception as e:  # pragma: no cover
            raise SecretNotFound(
                f"Secret '{name}' not available from env and boto3 unavailable for AWS lookup"
            ) from e

        region = self.aws_region or os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if not region:
            raise SecretNotFound(
                "AWS Secrets Manager lookups require AWS region (set AWS_REGION or AWS_DEFAULT_REGION)"
            )

        client = boto3.client("secretsmanager", region_name=region)
        try:
            resp = client.get_secret_value(SecretId=name)
        except Exception as e:  # pragma: no cover
            raise SecretNotFound(f"Unable to fetch secret '{name}' from AWS") from e

        if "SecretString" in resp and resp["SecretString"]:
            return str(resp["SecretString"])
        if "SecretBinary" in resp and resp["SecretBinary"]:
            return resp["SecretBinary"].decode("utf-8")

        raise SecretNotFound(f"Secret '{name}' had no usable value")

    def get_json(self, name: str, *, env_fallback: str | None = None) -> dict[str, Any]:
        return json.loads(self.get(name, env_fallback=env_fallback))

