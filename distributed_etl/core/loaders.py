from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .base import Dataset, LoadResult, Loader


@dataclass(frozen=True, slots=True)
class SQLServerLoader(Loader):
    """Loads rows into Microsoft SQL Server via ODBC.

    This loader uses a simple INSERT strategy (executemany). For large volumes,
    consider using bulk load (BCP) or staging tables.
    """

    connection_string: str
    table: str

    def load(self, rows: Dataset) -> LoadResult:
        if not rows:
            return LoadResult(rows_loaded=0, meta={"destination": "mssql", "table": self.table})

        try:
            import pyodbc  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("pyodbc is required for SQLServerLoader") from e

        cols = list(rows[0].keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(f"[{c}]" for c in cols)
        sql = f"INSERT INTO {self.table} ({col_list}) VALUES ({placeholders})"

        values = [tuple(r.get(c) for c in cols) for r in rows]

        with pyodbc.connect(self.connection_string) as conn:
            cur = conn.cursor()
            cur.fast_executemany = True
            cur.executemany(sql, values)
            conn.commit()

        return LoadResult(rows_loaded=len(rows), meta={"destination": "mssql", "table": self.table})


@dataclass(frozen=True, slots=True)
class SupabaseLoader(Loader):
    """Loads rows into Supabase using supabase-py (PostgREST).

    Expects `SUPABASE_URL` and `SUPABASE_KEY` to be provided, typically via env
    or secrets manager.
    """

    url: str
    key: str
    table: str
    upsert: bool = False

    def load(self, rows: Dataset) -> LoadResult:
        if not rows:
            return LoadResult(rows_loaded=0, meta={"destination": "supabase", "table": self.table})

        try:
            from supabase import create_client  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("supabase is required for SupabaseLoader") from e

        client = create_client(self.url, self.key)
        q = client.table(self.table)
        if self.upsert:
            res = q.upsert(rows).execute()
        else:
            res = q.insert(rows).execute()

        count = len(rows)
        meta: dict[str, Any] = {"destination": "supabase", "table": self.table}
        if getattr(res, "data", None) is not None:
            meta["returned"] = len(res.data)

        return LoadResult(rows_loaded=count, meta=meta)

