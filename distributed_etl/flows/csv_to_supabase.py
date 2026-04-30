from __future__ import annotations

from pathlib import Path

from prefect import flow, get_run_logger, task

from distributed_etl.core.extractors import CSVExtractor
from distributed_etl.core.loaders import SupabaseLoader
from distributed_etl.core.transformers import BasicCleaningTransformer
from distributed_etl.utils.env import load_project_dotenv
from distributed_etl.utils.logger import configure_logging
from distributed_etl.utils.security import SecretProvider


@task
def extract_csv(path: str) -> list[dict]:
    return CSVExtractor(path=path).extract().rows


@task
def transform_rows(rows: list[dict], required_columns: tuple[str, ...] = ()) -> list[dict]:
    return BasicCleaningTransformer(required_columns=required_columns).transform(rows)


@task
def load_supabase(*, rows: list[dict], url: str, key: str, table: str, upsert: bool) -> int:
    return SupabaseLoader(url=url, key=key, table=table, upsert=upsert).load(rows).rows_loaded


@flow(name="csv-to-supabase")
def csv_to_supabase_flow(
    *,
    csv_path: str,
    supabase_table: str,
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
        extra={"csv_path": csv_path, "supabase_table": supabase_table, "upsert": upsert},
    )

    secrets = SecretProvider()
    url = secrets.get(supabase_url_secret_name, env_fallback=supabase_url_env_fallback)
    key = secrets.get(supabase_key_secret_name, env_fallback=supabase_key_env_fallback)

    rows = extract_csv(str(Path(csv_path)))
    rows = transform_rows(rows, required_columns=required_columns)
    loaded = load_supabase(rows=rows, url=url, key=key, table=supabase_table, upsert=upsert)

    logger.info("completed flow", extra={"rows_loaded": loaded})
    return loaded

