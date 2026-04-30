from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from distributed_etl.flows.api_to_supabase import api_to_supabase_from_env
from distributed_etl.flows.csv_to_supabase import csv_to_supabase_flow
from distributed_etl.utils.csv_profiler import dumps_json, profile_csv, proposal_markdown, sql_create_table_draft
from distributed_etl.utils.env import load_project_dotenv
from distributed_etl.utils.file_picker import pick_csv_path
from distributed_etl.utils.launcher_gui import launch_gui
from distributed_etl.utils.pg_exec import execute_sql_ddl
from distributed_etl.utils.s3 import download_to_path


def cmd_run_csv(args: argparse.Namespace) -> None:
    load_project_dotenv()

    csv_path = args.csv
    if args.pick_csv:
        csv_path = str(pick_csv_path(title="Select CSV to load into Supabase"))

    if not csv_path:
        csv_path = os.getenv("CSV_PATH", "data/input.csv")

    # Optional: pull the CSV from S3 before running (for ECS/Fargate one-off tasks)
    s3_bucket = os.getenv("CSV_S3_BUCKET", "").strip()
    s3_key = os.getenv("CSV_S3_KEY", "").strip()
    if s3_bucket and s3_key:
        download_to_path(bucket=s3_bucket, key=s3_key, dest=csv_path)

    supabase_table = args.supabase_table or os.getenv("SUPABASE_TABLE", "my_table")

    csv_to_supabase_flow(
        csv_path=csv_path,
        supabase_table=supabase_table,
        required_columns=tuple(
            c.strip() for c in os.getenv("REQUIRED_COLUMNS", "").split(",") if c.strip()
        ),
        upsert=os.getenv("UPSERT", "true").strip().lower() in {"1", "true", "yes", "y", "on"},
    )


def cmd_run_api(_: argparse.Namespace) -> None:
    load_project_dotenv()
    api_to_supabase_from_env()


def cmd_profile(args: argparse.Namespace) -> None:
    load_project_dotenv()

    csv_path = Path(args.csv) if args.csv else pick_csv_path(title="Select CSV to profile")

    report = profile_csv(csv_path, encoding=args.encoding, max_rows=args.max_rows, distinct_cap=args.distinct_cap)

    stem = csv_path.name
    if stem.lower().endswith(".csv"):
        stem = stem[:-4]

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / f"{stem}_profile.json"
    md_path = out_dir / f"{stem}_schema_proposal.md"
    sql_path = out_dir / f"{stem}_create_table_draft.sql"

    json_path.write_text(dumps_json(report), encoding="utf-8")
    md_path.write_text(proposal_markdown(report, table_name=args.table_name), encoding="utf-8")
    sql_path.write_text(
        sql_create_table_draft(report, schema=args.schema, table_name=args.table_name),
        encoding="utf-8",
    )

    print(f"Wrote: {json_path}")
    print(f"Wrote: {md_path}")
    print(f"Wrote: {sql_path}")

    if args.apply_ddl:
        dsn = os.getenv("SUPABASE_DB_URL", "").strip()
        if not dsn:
            raise SystemExit("--apply-ddl requires SUPABASE_DB_URL (Postgres connection URI)")

        ddl_sql = sql_path.read_text(encoding="utf-8")
        print("Applying DDL via SUPABASE_DB_URL …")
        execute_sql_ddl(dsn=dsn, sql=ddl_sql)
        print("DDL applied.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="distributed-etl", description="CSV/API→Supabase ETL + CSV profiling utilities.")

    sub = p.add_subparsers(dest="command")

    run_csv = sub.add_parser(
        "run-csv",
        aliases=["run"],
        help="Run CSV→Supabase Prefect flow (env + optional flags).",
    )
    run_csv.add_argument("--csv", default=None, help="CSV path (defaults to CSV_PATH env or data/input.csv).")
    run_csv.add_argument(
        "--pick-csv",
        action="store_true",
        help="Open a native file picker to choose the CSV interactively.",
    )
    run_csv.add_argument(
        "--supabase-table",
        default=None,
        help="Override SUPABASE_TABLE env for this run.",
    )
    run_csv.set_defaults(func=cmd_run_csv)

    run_api = sub.add_parser(
        "run-api",
        help="Run API→Supabase Prefect flow using env vars (API_URL, SUPABASE_*, etc.).",
    )
    run_api.set_defaults(func=cmd_run_api)

    prof = sub.add_parser("profile", help="Profile a CSV and emit JSON + Markdown proposal + DDL draft.")
    prof.add_argument("--csv", default=None, help="CSV path (optional if --pick is used).")
    prof.add_argument(
        "--pick",
        action="store_true",
        help="Open a native file picker to choose the CSV interactively.",
    )
    prof.add_argument("--encoding", default="utf-8", help="CSV text encoding.")
    prof.add_argument("--max-rows", type=int, default=None, help="Optional row cap (omit for entire file).")
    prof.add_argument(
        "--distinct-cap",
        type=int,
        default=5000,
        help="Cap for exact-distinct accumulation per column (then distinct is approximate beyond this).",
    )
    prof.add_argument(
        "--table-name",
        default="draft_table",
        help="Table name label used only in Markdown/SQL draft outputs.",
    )
    prof.add_argument("--schema", default="public", help="Postgres schema for generated DDL drafts.")
    prof.add_argument("--out-dir", default="profiles", help="Where to write JSON/Markdown/SQL.")
    prof.add_argument(
        "--apply-ddl",
        action="store_true",
        help="Execute the generated CREATE TABLE SQL using SUPABASE_DB_URL (Postgres URI).",
    )
    prof.set_defaults(func=cmd_profile)

    gui = sub.add_parser("gui", help="Launch the Tkinter UI launcher (mode picker).")
    gui.set_defaults(func=lambda _: launch_gui())

    return p


def main() -> None:
    argv = sys.argv[1:]

    if not argv:
        if os.getenv("ETL_NO_GUI", "").strip().lower() in {"1", "true", "yes", "y", "on"}:
            cmd_run_csv(argparse.Namespace(csv=None, pick_csv=False, supabase_table=None))
            return

        launch_gui()
        return

    # Back-compat: `python main.py` already handled above; for `python main.py --something`
    if argv[0] not in {"profile", "run-csv", "run", "run-api", "gui"}:
        argv = ["run-csv", *argv]

    parser = build_parser()
    ns = parser.parse_args(argv)

    if ns.command == "profile":
        if not ns.csv and not ns.pick:
            raise SystemExit("profile requires either --csv PATH or --pick")

    if ns.command == "run-csv" and ns.csv and ns.pick_csv:
        raise SystemExit("Use only one of: --csv ... OR --pick-csv")

    ns.func(ns)


if __name__ == "__main__":
    main()
