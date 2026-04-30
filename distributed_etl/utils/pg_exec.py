from __future__ import annotations


def execute_sql_ddl(*, dsn: str, sql: str) -> None:
    """Execute SQL using a direct Postgres connection string (psycopg3).

    Intended for local/dev convenience (apply CREATE TABLE from profiling). In production,
    prefer migrations or Supabase SQL editor.
    """

    import psycopg  # type: ignore

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
