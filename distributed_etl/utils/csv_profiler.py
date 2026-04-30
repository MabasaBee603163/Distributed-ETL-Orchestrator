from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class TypeVote:
    empty: int = 0
    bool_: int = 0
    int_: int = 0
    float_: int = 0
    iso_datetime: int = 0
    iso_date: int = 0
    numeric_plain: int = 0
    numeric_decimal: int = 0
    string: int = 0


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_BOOL_TRUE = {"true", "t", "yes", "y", "1"}
_BOOL_FALSE = {"false", "f", "no", "n", "0"}


def _is_blank(value: object) -> bool:
    return value is None or str(value).strip() == ""


def _sanitize_number_token(s: str) -> str:
    return str(s).strip().replace(",", "").replace("_", "").replace(" ", "")


def _looks_like_decimal(token: str) -> bool:
    try:
        Decimal(token)
        return True
    except (InvalidOperation, ValueError):
        return False


def _looks_like_plain_int_token(token: str) -> bool:
    s = token.strip()

    digits = s.isdigit()
    neg_digits = s.startswith("-") and len(s) > 1 and s[1:].isdigit()
    if not (digits or neg_digits):
        return False

    # Avoid interpreting leading-zero tokens as BIGINT (ambiguous IDs / postal codes etc.)
    body = s.lstrip("-")
    if len(body) > 1 and body.startswith("0"):
        return False
    return True


def classify_cell(value: object) -> TypeVote:
    if _is_blank(value):
        return TypeVote(empty=1)

    raw = str(value)
    s = raw.strip()

    low = s.lower()

    # booleans before numerics/dates where possible
    if low in _BOOL_TRUE or low in _BOOL_FALSE:
        return TypeVote(bool_=1)

    if _ISO_DATE_RE.match(s):
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return TypeVote(iso_date=1)
        except ValueError:
            pass

    # datetime-ish parsing (timezone optional)
    iso = s
    if len(iso) >= 1 and (iso.endswith("Z") or iso.endswith("z")):
        iso = iso[:-1] + "+00:00"

    try:
        datetime.fromisoformat(iso)
        return TypeVote(iso_datetime=1)
    except ValueError:
        pass

    tok = _sanitize_number_token(raw)

    # Zero-padded digit tokens (e.g. "007"): likely IDs/zip-ish; don't coerce to BIGINT
    digits_only_body = tok.lstrip("-")
    if tok and digits_only_body.isdigit() and len(digits_only_body) > 1 and digits_only_body.startswith("0"):
        return TypeVote(string=1)

    if _looks_like_plain_int_token(tok):
        return TypeVote(int_=1)

    try:
        fv = float(tok)
        if math.isfinite(fv):
            # Integers routed above; floats here tend to fractional / scientific-ish
            return TypeVote(float_=1)
    except ValueError:
        pass

    if tok and _looks_like_decimal(tok):
        return TypeVote(numeric_decimal=1)

    return TypeVote(string=1)


def merge_votes(a: TypeVote, b: TypeVote) -> TypeVote:
    return TypeVote(
        empty=a.empty + b.empty,
        bool_=a.bool_ + b.bool_,
        int_=a.int_ + b.int_,
        float_=a.float_ + b.float_,
        iso_datetime=a.iso_datetime + b.iso_datetime,
        iso_date=a.iso_date + b.iso_date,
        numeric_plain=a.numeric_plain + b.numeric_plain,
        numeric_decimal=a.numeric_decimal + b.numeric_decimal,
        string=a.string + b.string,
    )


def infer_pg_type_non_empty(votes: TypeVote, *, nonempty_cells: int) -> tuple[str, str]:
    if nonempty_cells <= 0:
        return "text", "Non-empty observations = 0 (unexpected branch)."

    parts: dict[str, int] = {
        "boolean": votes.bool_,
        "bigint": votes.int_,
        "double precision": votes.float_,
        "timestamptz": votes.iso_datetime,
        "date": votes.iso_date,
        # Treat numeric_decimal as DECIMAL for safety when decimals exist
        "numeric": votes.numeric_decimal,
        "text": votes.string,
    }

    dominant = sorted(parts.items(), key=lambda kv: kv[1], reverse=True)[0]
    name, count = dominant
    runner = sorted(parts.items(), key=lambda kv: kv[1], reverse=True)[1][1]

    rationale_parts: list[str] = []

    rationale_parts.append(f"dominant_classification={name} dominant_count={count} runner_up={runner}")

    if count > 0 and runner > 0 and count < 0.90 * nonempty_cells:
        rationale_parts.append(f"mixed_signals=true dominant_share={count / nonempty_cells:.3f}")

    rationale = "; ".join(rationale_parts)

    match name:
        case "boolean":
            return "boolean", rationale
        case "bigint":
            return "bigint", rationale
        case "double precision":
            return "double precision", rationale
        case "timestamptz":
            return "timestamptz", rationale
        case "date":
            return "date", rationale
        case "numeric":
            return "numeric", rationale
        case _:
            return "text", rationale


@dataclass(slots=True)
class ColumnProfile:
    name: str

    rows_seen: int = 0
    null_rows: int = 0
    non_null_rows: int = 0

    votes_all_cells: TypeVote = field(default_factory=TypeVote)
    votes_nonempty_cells: TypeVote = field(default_factory=TypeVote)

    distinct_exact: set[str] = field(default_factory=set)
    distinct_approx_cap: int = 5000

    numeric_min: Decimal | None = None
    numeric_max: Decimal | None = None

    float_min: float | None = None
    float_max: float | None = None

    int_min: int | None = None
    int_max: int | None = None

    text_len_min: int | None = None
    text_len_max: int | None = None


def _update_ranges(profile: ColumnProfile, raw_nonempty: object) -> None:
    vt = classify_cell(raw_nonempty)

    tok = _sanitize_number_token(raw_nonempty)

    if vt.int_:
        try:
            iv = int(tok)
            profile.int_min = iv if profile.int_min is None else min(profile.int_min, iv)
            profile.int_max = iv if profile.int_max is None else max(profile.int_max, iv)
        except ValueError:
            pass

        if tok and _looks_like_decimal(tok):
            try:
                d = Decimal(tok)
                profile.numeric_min = d if profile.numeric_min is None else min(profile.numeric_min, d)
                profile.numeric_max = d if profile.numeric_max is None else max(profile.numeric_max, d)
            except Exception:
                pass
        return

    if vt.float_:
        try:
            fv = float(tok)
            if math.isfinite(fv):
                profile.float_min = fv if profile.float_min is None else min(profile.float_min, fv)
                profile.float_max = fv if profile.float_max is None else max(profile.float_max, fv)
        except ValueError:
            pass

        if tok and _looks_like_decimal(tok):
            try:
                d = Decimal(tok)
                profile.numeric_min = d if profile.numeric_min is None else min(profile.numeric_min, d)
                profile.numeric_max = d if profile.numeric_max is None else max(profile.numeric_max, d)
            except Exception:
                pass
        return

    if vt.numeric_decimal and tok:
        try:
            d = Decimal(tok)
            profile.numeric_min = d if profile.numeric_min is None else min(profile.numeric_min, d)
            profile.numeric_max = d if profile.numeric_max is None else max(profile.numeric_max, d)
        except Exception:
            pass
        return

    if vt.string or vt.iso_date or vt.iso_datetime or vt.bool_:
        s = str(raw_nonempty).strip()
        ln = len(s)
        profile.text_len_min = ln if profile.text_len_min is None else min(profile.text_len_min, ln)
        profile.text_len_max = ln if profile.text_len_max is None else max(profile.text_len_max, ln)


def profile_csv_rows(
    *,
    rows: Iterable[dict[str, str]],
    fieldnames: list[str],
    distinct_approx_cap: int,
) -> tuple[int, dict[str, ColumnProfile]]:
    profiles: dict[str, ColumnProfile] = {
        fn: ColumnProfile(name=fn, distinct_approx_cap=distinct_approx_cap) for fn in fieldnames
    }

    total_rows = 0

    for row in rows:
        total_rows += 1

        for fn in fieldnames:
            raw = row.get(fn, "")
            sv = "" if raw is None else str(raw)

            prof = profiles[fn]
            prof.rows_seen += 1

            cell_vote = classify_cell(raw)
            prof.votes_all_cells = merge_votes(prof.votes_all_cells, cell_vote)

            if _is_blank(raw):
                prof.null_rows += 1
                continue

            prof.non_null_rows += 1
            prof.votes_nonempty_cells = merge_votes(prof.votes_nonempty_cells, cell_vote)

            if len(prof.distinct_exact) < prof.distinct_approx_cap:
                prof.distinct_exact.add(sv.strip())

            _update_ranges(prof, raw)

    return total_rows, profiles


def profile_csv(
    path: Path,
    *,
    encoding: str = "utf-8",
    max_rows: int | None = None,
    distinct_cap: int = 5000,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    with p.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"No CSV header detected: {p}")

        fieldnames = list(reader.fieldnames)

        sampled: list[dict[str, str]] = []

        if max_rows is None:
            for rr in reader:
                sampled.append({k: ("" if v is None else str(v)) for k, v in rr.items()})
        else:
            for i, rr in enumerate(reader):
                if i >= max_rows:
                    break
                sampled.append({k: ("" if v is None else str(v)) for k, v in rr.items()})

        rows_analyzed, profiles = profile_csv_rows(rows=sampled, fieldnames=fieldnames, distinct_approx_cap=distinct_cap)

    columns_out: dict[str, Any] = {}
    pk_candidates: list[str] = []

    analyzed_rows_local = rows_analyzed

    for fn in fieldnames:
        prof = profiles[fn]

        distinct_n = len(prof.distinct_exact)
        approx_hit = distinct_n >= prof.distinct_approx_cap

        inferred_non_empty: str | None = None
        nonempty_rationale: str | None = None

        if prof.non_null_rows > 0:
            inferred_non_empty, nonempty_rationale = infer_pg_type_non_empty(
                prof.votes_nonempty_cells, prof.non_null_rows
            )

        # Columns that are entirely empty should stay TEXT (explicit review)
        if prof.non_null_rows == 0:
            inferred_pg = "text"
            rationale = (
                "All sampled cells blank; defaulted to TEXT. "
                "If this column shouldn't exist upstream, investigate export settings."
            )
        else:
            inferred_pg = inferred_non_empty  # guaranteed non-null when non-null cells exist

            rationales: list[str] = []

            rationales.append(nonempty_rationale)

            if prof.null_rows > 0:
                rationales.append(
                    "Contains NULL blanks in sample — constraints should normally allow NULL "
                    "(unless intentionally required)."
                )

            # booleans commonly contain NULLs in real data; bias toward nullable
            rationale = "; ".join(rationales)

        non_null_fraction = prof.non_null_rows / max(prof.rows_seen, 1)

        heuristic_pk = (
            prof.null_rows == 0
            and prof.non_null_rows == analyzed_rows_local
            and analyzed_rows_local > 1
            and distinct_n == analyzed_rows_local
            and not approx_hit
        )

        nonempty_inference_payload: dict[str, Any] | None

        if prof.non_null_rows > 0 and inferred_non_empty is not None and nonempty_rationale is not None:
            nonempty_inference_payload = {
                "inferred_pg_type": inferred_non_empty,
                "rationale": nonempty_rationale,
            }
        else:
            nonempty_inference_payload = None

        columns_out[fn] = {
            "name": fn,
            "rows_seen_in_sample": prof.rows_seen,
            "null_rows": prof.null_rows,
            "non_null_rows": prof.non_null_rows,
            "non_null_fraction": non_null_fraction,
            "votes_all_cells": asdict(prof.votes_all_cells),
            "votes_nonempty_cells": asdict(prof.votes_nonempty_cells),
            "distinct_count_truncated_to": distinct_cap,
            "distinct_count_in_sample_exact": distinct_n,
            "distinct_count_approx": approx_hit,
            "inferred_pg_type": inferred_pg if prof.non_null_rows > 0 else "text",
            "nonempty_inference": nonempty_inference_payload,
            "rationale": rationale,
            "ranges": {
                "numeric_min": str(prof.numeric_min) if prof.numeric_min is not None else None,
                "numeric_max": str(prof.numeric_max) if prof.numeric_max is not None else None,
                "int_min": prof.int_min,
                "int_max": prof.int_max,
                "float_min": prof.float_min,
                "float_max": prof.float_max,
                "text_len_min": prof.text_len_min,
                "text_len_max": prof.text_len_max,
            },
            "possible_primary_key_candidate_heuristic": bool(heuristic_pk),
        }

        if heuristic_pk:
            pk_candidates.append(fn)

        _ = approx_hit

    return {
        "source": {"path": str(p), "encoding": encoding, "max_rows_requested": max_rows, "rows_analyzed": analyzed_rows_local},
        "profiler": {"distinct_approx_cap": distinct_cap},
        "columns": columns_out,
        "proposed_constraints": {
            "primary_key_candidates_heuristic": pk_candidates,
            "notes": [
                "Heuristic uniqueness is based on capped distinct counting + completeness in-sample — validate in SQL.",
                "Leading-zero tokens are intentionally not treated as BIGINT.",
                "DECIMAL inferred when decimal/scientific forms appear; widen review for money vs float.",
            ],
        },
    }


def dumps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def proposal_markdown(report: dict[str, Any], *, table_name: str) -> str:
    src = report["source"]
    cols = report["columns"]

    pk_caps = report["proposed_constraints"]["primary_key_candidates_heuristic"]

    lines: list[str] = []
    lines.append(f"# Evidence-based schema proposal: `{table_name}`")
    lines.append("")
    lines.append(f"- Rows analyzed: **{src['rows_analyzed']}**")
    lines.append(f"- Max rows cap: `{src['max_rows_requested']}`")
    lines.append("")
    lines.append("| column | inferred_pg_type | non-null frac | distinct | approx cap hit? | pk heuristic | rationale (truncated) |")
    lines.append("| --- | --- | ---:| --- | ---:| --- | --- |")

    for fn in sorted(cols.keys()):
        cm = cols[fn]

        approx = "**yes**" if bool(cm["distinct_count_approx"]) else "no"

        rat = str(cm["rationale"]).replace("|", "\\|")
        if len(rat) > 220:
            rat = rat[:220] + "…"

        pk = "maybe" if bool(cm["possible_primary_key_candidate_heuristic"]) else "no"

        distinct = cm["distinct_count_in_sample_exact"]
        frac = float(cm["non_null_fraction"])

        lines.append(
            f"| `{fn}` | `{cm['inferred_pg_type']}` | {frac:.4f} | {distinct} | {approx} | {pk} | {rat} |"
        )

    lines.append("")
    lines.append("## Candidate primary keys (must be approved)")
    lines.append("")
    if not pk_caps:
        lines.append("- None confidently detected from profiling alone.")
    else:
        for p in pk_caps:
            lines.append(f"- `{p}`")

    lines.append("")
    lines.append("## Explicit approval checklist")
    lines.append("")
    lines.append("- [ ] Decide **surrogate PK** (`uuid/bigserial`) vs **natural key** UPSERT semantics")
    lines.append("- [ ] Confirm nullable vs NOT NULL per column vs business requirement")
    lines.append("- [ ] Validate uniqueness outside sample (distinct cap / full SQL checks)")
    lines.append("- [ ] Decide NUMERIC precision/scale for money quantities")
    lines.append("")
    lines.append('*If `distinct cap hit?=yes`, do not infer uniqueness confidently without SQL validation.*')

    lines.append("")
    return "\n".join(lines)


def sql_quote_ident(name: str) -> str:
    if str(name).strip() == "":
        raise ValueError("identifier empty")
    return '"' + str(name).replace('"', '""') + '"'


def sql_create_table_draft(report: dict[str, Any], *, schema: str, table_name: str) -> str:
    cols_meta = report["columns"]

    fq_tbl = f"{sql_quote_ident(schema)}.{sql_quote_ident(table_name)}"

    pk_cands = [name for name, cm in cols_meta.items() if cm["possible_primary_key_candidate_heuristic"]]

    pk = pk_cands[:1]

    defs_clean: list[str] = []

    for fn in sorted(cols_meta.keys()):
        cm = cols_meta[fn]
        ctype = str(cm["inferred_pg_type"])

        nullable = cm["null_rows"] > 0

        if ctype == "boolean":
            nullable = True

        null_sql = " NULL" if nullable else " NOT NULL"

        defs_clean.append(f"  {sql_quote_ident(fn)} {ctype}{null_sql}")

    pk_sql_lines: list[str] = []
    if pk:
        cols_sql = ", ".join(sql_quote_ident(x) for x in pk)
        pk_sql_lines.append("")
        pk_sql_lines.append(
            f"  CONSTRAINT {sql_quote_ident(f'{table_name}_pk_draft')} PRIMARY KEY ({cols_sql})"
        )

    out: list[str] = []
    out.append("-- REVIEW CAREFULLY BEFORE APPLYING.")
    out.append("-- This DDL is inferred from profiling, not authoritative.")
    out.append("")
    out.append(f"CREATE TABLE IF NOT EXISTS {fq_tbl} (")
    out.append(",\n".join(defs_clean))

    out.extend(pk_sql_lines)
    out.append(");")

    out.append("")
    out.append("-- Typical follow-ups:")
    out.append("-- - Add UNIQUE (...) on stable business identifiers")
    out.append("-- - Add CHECK (...) constraints grounded in documented rules")

    out.append("")
    return "\n".join(out)
