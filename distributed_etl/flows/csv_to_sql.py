from __future__ import annotations

from pathlib import Path

from prefect import flow, get_run_logger, task

from distributed_etl.core.extractors import CSVExtractor
from distributed_etl.core.loaders import SQLServerLoader
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
def load_mssql(rows: list[dict], connection_string: str, table: str) -> int:
    return SQLServerLoader(connection_string=connection_string, table=table).load(rows).rows_loaded


@flow(name="csv-to-sql")
def csv_to_sql_flow(
    *,
    csv_path: str,
    sql_table: str,
    required_columns: tuple[str, ...] = (),
    connection_secret_name: str = "mssql/connection_string",
    connection_env_fallback: str = "MSSQL_CONNECTION_STRING",
) -> int:
    load_project_dotenv()
    configure_logging()

    prefect_logger = get_run_logger()
    prefect_logger.info("starting flow", extra={"csv_path": csv_path, "sql_table": sql_table})

    secrets = SecretProvider()
    conn_str = secrets.get(connection_secret_name, env_fallback=connection_env_fallback)

    rows = extract_csv(str(Path(csv_path)))
    rows = transform_rows(rows, required_columns=required_columns)
    loaded = load_mssql(rows, conn_str, sql_table)

    prefect_logger.info("completed flow", extra={"rows_loaded": loaded})
    return loaded

