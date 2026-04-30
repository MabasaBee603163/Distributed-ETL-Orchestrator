from __future__ import annotations

import json
import os
from typing import Any

from prefect import flow, get_run_logger, task

from distributed_etl.core.extractors import APIExtractor
from distributed_etl.core.loaders import SupabaseLoader
from distributed_etl.core.transformers import BasicCleaningTransformer
from distributed_etl.utils.env import load_project_dotenv
from distributed_etl.utils.logger import configure_logging
from distributed_etl.utils.security import SecretProvider


def _parse_headers(raw: str | None) -> dict[str, str] | None:
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError("API_HEADERS_JSON must be valid JSON object, e.g. {\"Authorization\":\"Bearer ...\"}") from e

    if not isinstance(obj, dict):
        raise ValueError("API_HEADERS_JSON must be a JSON object mapping string->string")

    out: dict[str, str] = {}
    for k, v in obj.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError("API_HEADERS_JSON must map strings to strings only")
        out[k] = v
    return out


@task
def extract_api(
    *,
    url: str,
    method: str,
    headers: dict[str, str] | None,
    params: dict[str, Any] | None,
    json_body: Any | None,
    timeout_s: float,
) -> list[dict]:
    return APIExtractor(
        url=url,
        method=method,
        headers=headers,
        params=params,
        json_body=json_body,
        timeout_s=timeout_s,
    ).extract().rows


@task
def transform_rows(rows: list[dict], required_columns: tuple[str, ...] = ()) -> list[dict]:
    return BasicCleaningTransformer(required_columns=required_columns).transform(rows)


@task
def load_supabase(*, rows: list[dict], url: str, key: str, table: str, upsert: bool) -> int:
    return SupabaseLoader(url=url, key=key, table=table, upsert=upsert).load(rows).rows_loaded


@flow(name="api-to-supabase")
def api_to_supabase_flow(
    *,
    api_url: str,
    supabase_table: str,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: Any | None = None,
    timeout_s: float = 30.0,
    required_columns: tuple[str, ...] = (),
    upsert: bool = True,
    supabase_url_secret_name: str = "supabase/url",
    supabase_key_secret_name: str = "supabase/key",
    supabase_url_env_fallback: str = "SUPABASE_URL",
    supabase_key_env_fallback: str = "SUPABASE_KEY",
) -> int:
    load_project_dotenv()
    configure_logging()

    logger = get_run_logger()
    logger.info(
        "starting flow",
        extra={"api_url": api_url, "supabase_table": supabase_table, "method": method, "upsert": upsert},
    )

    secrets = SecretProvider()
    url = secrets.get(supabase_url_secret_name, env_fallback=supabase_url_env_fallback)
    key = secrets.get(supabase_key_secret_name, env_fallback=supabase_key_env_fallback)

    rows = extract_api(
        url=api_url,
        method=method,
        headers=headers,
        params=params,
        json_body=json_body,
        timeout_s=timeout_s,
    )
    rows = transform_rows(rows, required_columns=required_columns)
    loaded = load_supabase(rows=rows, url=url, key=key, table=supabase_table, upsert=upsert)

    logger.info("completed flow", extra={"rows_loaded": loaded})
    return loaded


def api_to_supabase_from_env() -> int:
    """Convenience entrypoint driven by environment variables."""

    load_project_dotenv()

    api_url = os.getenv("API_URL", "").strip()
    if not api_url:
        raise RuntimeError("API_URL is required for API→Supabase")

    method = os.getenv("API_METHOD", "GET").strip().upper()
    headers = _parse_headers(os.getenv("API_HEADERS_JSON"))
    timeout_s = float(os.getenv("API_TIMEOUT_S", "30"))

    params_raw = os.getenv("API_PARAMS_JSON")
    params: dict[str, Any] | None = None
    if params_raw:
        try:
            obj = json.loads(params_raw)
        except json.JSONDecodeError as e:
            raise ValueError("API_PARAMS_JSON must be valid JSON object") from e
        if not isinstance(obj, dict):
            raise ValueError("API_PARAMS_JSON must be a JSON object")
        params = obj

    json_body_raw = os.getenv("API_JSON_BODY_JSON")
    json_body: Any | None = None
    if json_body_raw:
        json_body = json.loads(json_body_raw)

    supabase_table = os.getenv("SUPABASE_TABLE", "my_table").strip()
    upsert = os.getenv("UPSERT", "true").strip().lower() in {"1", "true", "yes", "y", "on"}

    required_columns = tuple(
        c.strip() for c in os.getenv("REQUIRED_COLUMNS", "").split(",") if c.strip()
    )

    return api_to_supabase_flow(
        api_url=api_url,
        supabase_table=supabase_table,
        method=method,
        headers=headers,
        params=params,
        json_body=json_body,
        timeout_s=timeout_s,
        required_columns=required_columns,
        upsert=upsert,
    )
