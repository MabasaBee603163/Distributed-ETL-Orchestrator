from __future__ import annotations

import os
import queue
import threading
import traceback
from dataclasses import dataclass
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from distributed_etl.flows.api_to_supabase import api_to_supabase_from_env
from distributed_etl.flows.csv_to_supabase import csv_to_supabase_flow
from distributed_etl.utils.csv_profiler import dumps_json, profile_csv, proposal_markdown, sql_create_table_draft
from distributed_etl.utils.env import load_project_dotenv
from distributed_etl.utils.envfile import upsert_dotenv
from distributed_etl.utils.pg_exec import execute_sql_ddl


@dataclass(frozen=True, slots=True)
class _GuiState:
    mode: tk.StringVar
    pick_csv: tk.BooleanVar
    csv_path: tk.StringVar
    supabase_url: tk.StringVar
    supabase_key: tk.StringVar
    supabase_table: tk.StringVar
    db_url: tk.StringVar
    apply_ddl: tk.BooleanVar
    api_url: tk.StringVar
    api_method: tk.StringVar
    api_headers_json: tk.StringVar
    api_params_json: tk.StringVar
    api_body_json: tk.StringVar


def _project_root() -> Path:
    # .../distributed_etl/utils/launcher_gui.py -> repo root is parents[2]
    return Path(__file__).resolve().parents[2]


def _dotenv_path() -> Path:
    return _project_root() / ".env"


def _apply_gui_env(st: _GuiState) -> None:
    """Overlay GUI-entered values onto process env for this run."""

    def set_if_nonempty(key: str, value: str) -> None:
        v = value.strip()
        if v:
            os.environ[key] = v

    set_if_nonempty("SUPABASE_URL", st.supabase_url.get())
    set_if_nonempty("SUPABASE_KEY", st.supabase_key.get())
    set_if_nonempty("SUPABASE_TABLE", st.supabase_table.get())

    set_if_nonempty("API_URL", st.api_url.get())
    set_if_nonempty("API_METHOD", st.api_method.get())

    if st.api_headers_json.get().strip():
        os.environ["API_HEADERS_JSON"] = st.api_headers_json.get().strip()

    if st.api_params_json.get().strip():
        os.environ["API_PARAMS_JSON"] = st.api_params_json.get().strip()

    if st.api_body_json.get().strip():
        os.environ["API_JSON_BODY_JSON"] = st.api_body_json.get().strip()

    if st.db_url.get().strip():
        os.environ["SUPABASE_DB_URL"] = st.db_url.get().strip()


def launch_gui() -> None:
    root = tk.Tk()
    root.title("Distributed ETL Orchestrator")

    st = _GuiState(
        mode=tk.StringVar(value="PROFILE_CSV"),
        pick_csv=tk.BooleanVar(value=True),
        csv_path=tk.StringVar(value=""),
        supabase_url=tk.StringVar(value=os.getenv("SUPABASE_URL", "")),
        supabase_key=tk.StringVar(value=os.getenv("SUPABASE_KEY", "")),
        supabase_table=tk.StringVar(value=os.getenv("SUPABASE_TABLE", "contacts")),
        db_url=tk.StringVar(value=os.getenv("SUPABASE_DB_URL", "")),
        apply_ddl=tk.BooleanVar(value=False),
        api_url=tk.StringVar(value=os.getenv("API_URL", "")),
        api_method=tk.StringVar(value=os.getenv("API_METHOD", "GET")),
        api_headers_json=tk.StringVar(value=os.getenv("API_HEADERS_JSON", "")),
        api_params_json=tk.StringVar(value=os.getenv("API_PARAMS_JSON", "")),
        api_body_json=tk.StringVar(value=os.getenv("API_JSON_BODY_JSON", "")),
    )

    frm = ttk.Frame(root, padding=10)
    frm.grid(row=0, column=0, sticky="nsew")
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)

    row = 0

    ttk.Label(frm, text="Mode").grid(row=row, column=0, sticky="w")
    mode_box = ttk.Frame(frm)
    mode_box.grid(row=row, column=1, columnspan=3, sticky="w")
    ttk.Radiobutton(mode_box, text="Profile CSV → artifacts (+ optional CREATE TABLE)", variable=st.mode, value="PROFILE_CSV").pack(
        side="left", padx=(0, 10)
    )
    ttk.Radiobutton(mode_box, text="Run CSV → Supabase", variable=st.mode, value="RUN_CSV").pack(side="left", padx=(0, 10))
    ttk.Radiobutton(mode_box, text="Run API → Supabase", variable=st.mode, value="RUN_API").pack(side="left")
    row += 1

    ttk.Separator(frm, orient="horizontal").grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
    row += 1

    ttk.Label(frm, text="CSV").grid(row=row, column=0, sticky="nw")
    csv_row = ttk.Frame(frm)
    csv_row.grid(row=row, column=1, columnspan=3, sticky="ew")
    ttk.Checkbutton(csv_row, text="Pick file…", variable=st.pick_csv, command=lambda: _toggle_csv_entry()).pack(side="left")
    csv_entry = ttk.Entry(csv_row, textvariable=st.csv_path, width=70)
    csv_entry.pack(side="left", padx=(10, 10), fill="x", expand=True)

    def browse_csv() -> None:
        p = filedialog.askopenfilename(title="Select CSV", filetypes=[("CSV", "*.csv"), ("All files", "*.*")])
        if p:
            st.pick_csv.set(False)
            st.csv_path.set(p)

    ttk.Button(csv_row, text="Browse", command=browse_csv).pack(side="left")

    def _toggle_csv_entry() -> None:
        if st.pick_csv.get():
            st.csv_path.set("")

    row += 1

    ttk.Label(frm, text="Supabase").grid(row=row, column=0, sticky="nw")
    sup = ttk.Frame(frm)
    sup.grid(row=row, column=1, columnspan=3, sticky="ew")

    ttk.Label(sup, text="URL").grid(row=0, column=0, sticky="w")
    ttk.Entry(sup, textvariable=st.supabase_url, width=80).grid(row=0, column=1, sticky="ew", padx=(10, 0))

    ttk.Label(sup, text="Service role key").grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(sup, textvariable=st.supabase_key, width=80, show="*").grid(row=1, column=1, sticky="ew", padx=(10, 0), pady=(6, 0))

    ttk.Label(sup, text="Table").grid(row=2, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(sup, textvariable=st.supabase_table, width=40).grid(row=2, column=1, sticky="w", padx=(10, 0), pady=(6, 0))

    sup.columnconfigure(1, weight=1)
    row += 1

    ttk.Label(frm, text="Profiling DDL apply (optional)").grid(row=row, column=0, sticky="nw")
    ddl = ttk.Frame(frm)
    ddl.grid(row=row, column=1, columnspan=3, sticky="ew")

    ttk.Checkbutton(ddl, text="Execute generated CREATE TABLE SQL via Postgres connection string", variable=st.apply_ddl).grid(
        row=0, column=0, columnspan=2, sticky="w"
    )

    ttk.Label(ddl, text="SUPABASE_DB_URL").grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(ddl, textvariable=st.db_url, width=80).grid(row=1, column=1, sticky="ew", padx=(10, 0), pady=(6, 0))

    ddl.columnconfigure(1, weight=1)
    row += 1

    ttk.Label(frm, text="API").grid(row=row, column=0, sticky="nw")
    api = ttk.Frame(frm)
    api.grid(row=row, column=1, columnspan=3, sticky="ew")

    ttk.Label(api, text="API_URL").grid(row=0, column=0, sticky="w")
    ttk.Entry(api, textvariable=st.api_url, width=80).grid(row=0, column=1, sticky="ew", padx=(10, 0))

    ttk.Label(api, text="METHOD").grid(row=1, column=0, sticky="w", pady=(6, 0))
    ttk.Entry(api, textvariable=st.api_method, width=10).grid(row=1, column=1, sticky="w", padx=(10, 0), pady=(6, 0))

    ttk.Label(api, text="API_HEADERS_JSON").grid(row=2, column=0, sticky="nw", pady=(6, 0))
    hdr = tk.Text(api, height=4, width=80)
    hdr.grid(row=2, column=1, sticky="ew", padx=(10, 0), pady=(6, 0))
    hdr.insert("1.0", st.api_headers_json.get())

    ttk.Label(api, text="API_PARAMS_JSON").grid(row=3, column=0, sticky="nw", pady=(6, 0))
    prm = tk.Text(api, height=4, width=80)
    prm.grid(row=3, column=1, sticky="ew", padx=(10, 0), pady=(6, 0))
    prm.insert("1.0", st.api_params_json.get())

    ttk.Label(api, text="API_JSON_BODY_JSON").grid(row=4, column=0, sticky="nw", pady=(6, 0))
    bod = tk.Text(api, height=4, width=80)
    bod.grid(row=4, column=1, sticky="ew", padx=(10, 0), pady=(6, 0))
    bod.insert("1.0", st.api_body_json.get())

    api.columnconfigure(1, weight=1)
    row += 1

    ttk.Separator(frm, orient="horizontal").grid(row=row, column=0, columnspan=4, sticky="ew", pady=8)
    row += 1

    actions = ttk.Frame(frm)
    actions.grid(row=row, column=0, columnspan=4, sticky="ew")

    log_q: queue.Queue[str] = queue.Queue()

    def append_log(msg: str) -> None:
        log_q.put(msg)

    def poll_log() -> None:
        try:
            while True:
                m = log_q.get_nowait()
                log.insert("end", m + "\n")
                log.see("end")
        except queue.Empty:
            pass
        root.after(100, poll_log)

    def save_env_clicked() -> None:
        path = _dotenv_path()
        try:
            upsert_dotenv(
                path,
                {
                    "SUPABASE_URL": st.supabase_url.get().strip(),
                    "SUPABASE_KEY": st.supabase_key.get().strip(),
                    "SUPABASE_TABLE": st.supabase_table.get().strip(),
                    "SUPABASE_DB_URL": st.db_url.get().strip(),
                    "API_URL": st.api_url.get().strip(),
                    "API_METHOD": st.api_method.get().strip(),
                    "API_HEADERS_JSON": hdr.get("1.0", "end").strip(),
                    "API_PARAMS_JSON": prm.get("1.0", "end").strip(),
                    "API_JSON_BODY_JSON": bod.get("1.0", "end").strip(),
                },
            )
            messagebox.showinfo("Saved", f"Updated {path}")
        except Exception as e:
            messagebox.showerror("Save failed", str(e))

    def run_clicked() -> None:
        start_btn.configure(state="disabled")

        def worker() -> None:
            try:
                load_project_dotenv()
                _apply_gui_env(st)

                # Keep multi-line JSON fields in sync with env vars used by API route
                os.environ["API_HEADERS_JSON"] = hdr.get("1.0", "end").strip()
                os.environ["API_PARAMS_JSON"] = prm.get("1.0", "end").strip()
                os.environ["API_JSON_BODY_JSON"] = bod.get("1.0", "end").strip()

                mode = st.mode.get()

                if mode == "PROFILE_CSV":
                    append_log("[profile] Profiling CSV…")

                    if st.pick_csv.get():
                        pth = filedialog.askopenfilename(title="Select CSV to profile", filetypes=[("CSV", "*.csv"), ("All", "*.*")])
                        if not pth:
                            append_log("[profile] cancelled")
                            return
                        csv_path = Path(pth)
                    else:
                        if not st.csv_path.get().strip():
                            raise RuntimeError("CSV path is empty (uncheck Pick file or enter a path)")
                        csv_path = Path(st.csv_path.get().strip())

                    table_name = st.supabase_table.get().strip() or "draft_table"
                    schema = "public"

                    report = profile_csv(csv_path)
                    sql = sql_create_table_draft(report, schema=schema, table_name=table_name)

                    out_dir = _project_root() / "profiles"
                    out_dir.mkdir(parents=True, exist_ok=True)

                    stem = csv_path.stem

                    json_path = out_dir / f"{stem}_profile.json"
                    md_path = out_dir / f"{stem}_schema_proposal.md"
                    sql_path = out_dir / f"{stem}_create_table_draft.sql"

                    json_path.write_text(dumps_json(report), encoding="utf-8")
                    md_path.write_text(proposal_markdown(report, table_name=table_name), encoding="utf-8")
                    sql_path.write_text(sql, encoding="utf-8")

                    append_log(f"[profile] wrote: {json_path}")
                    append_log(f"[profile] wrote: {md_path}")
                    append_log(f"[profile] wrote: {sql_path}")

                    if st.apply_ddl.get():
                        dsn = st.db_url.get().strip()
                        if not dsn:
                            raise RuntimeError("SUPABASE_DB_URL is required to execute DDL")

                        append_log("[profile] applying CREATE TABLE via Postgres…")
                        execute_sql_ddl(dsn=dsn, sql=sql)
                        append_log("[profile] DDL executed")

                    append_log("[profile] done")

                elif mode == "RUN_CSV":
                    append_log("[csv] starting Prefect flow…")

                    if st.pick_csv.get():
                        pth = filedialog.askopenfilename(title="Select CSV to load", filetypes=[("CSV", "*.csv"), ("All", "*.*")])
                        if not pth:
                            append_log("[csv] cancelled")
                            return
                        csv_path = pth
                    else:
                        csv_path = st.csv_path.get().strip() or os.getenv("CSV_PATH", "data/input.csv")

                    supabase_table = st.supabase_table.get().strip() or os.getenv("SUPABASE_TABLE", "my_table")

                    csv_to_supabase_flow(
                        csv_path=csv_path,
                        supabase_table=supabase_table,
                        required_columns=tuple(
                            c.strip() for c in os.getenv("REQUIRED_COLUMNS", "").split(",") if c.strip()
                        ),
                        upsert=os.getenv("UPSERT", "true").strip().lower() in {"1", "true", "yes", "y", "on"},
                    )

                    append_log("[csv] done")

                elif mode == "RUN_API":
                    append_log("[api] starting Prefect flow…")
                    api_to_supabase_from_env()
                    append_log("[api] done")

                else:
                    raise RuntimeError(f"Unknown mode: {mode}")

            except Exception as e:
                append_log("[error] " + str(e))
                append_log(traceback.format_exc())
                messagebox.showerror("Run failed", str(e))
            finally:
                start_btn.configure(state="normal")

        threading.Thread(target=worker, daemon=True).start()

    ttk.Button(actions, text="Save secrets to .env", command=save_env_clicked).pack(side="left")
    start_btn = ttk.Button(actions, text="Start", command=run_clicked)
    start_btn.pack(side="right")

    row += 1

    ttk.Label(frm, text="Log").grid(row=row, column=0, sticky="nw")
    log = tk.Text(frm, height=18, width=100)
    log.grid(row=row, column=1, columnspan=3, sticky="nsew")
    frm.rowconfigure(row, weight=1)
    frm.columnconfigure(1, weight=1)

    append_log("Tip: For DDL apply, use Supabase Project Settings → Database → Connection string (URI). Store as SUPABASE_DB_URL.")
    poll_log()

    root.mainloop()
