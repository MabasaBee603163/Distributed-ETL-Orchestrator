from __future__ import annotations

from pathlib import Path


def upsert_dotenv(path: Path, updates: dict[str, str]) -> None:
    """Upsert KEY=VALUE lines into a dotenv file (creates file if missing)."""

    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()

    existing: dict[str, int | None] = {k: None for k in updates.keys()}
    for i, line in enumerate(lines):
        if not line or line.lstrip().startswith("#"):
            continue
        if "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if key in existing:
            existing[key] = i

    out = lines[:]

    for key, value in updates.items():
        rendered = f'{key}={value}'
        idx = existing.get(key)
        if idx is None:
            if out and out[-1].strip() != "":
                out.append("")
            out.append(rendered)
        else:
            out[idx] = rendered

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")
