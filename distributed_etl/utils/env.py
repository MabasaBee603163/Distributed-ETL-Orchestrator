from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_project_dotenv(*, project_root: Path | None = None) -> None:
    """Load local env vars from common dotenv filenames.

    `python-dotenv` defaults to `.env` only; many editors accidentally save
    `.env.env`, which otherwise won't be picked up.
    """

    root = project_root or Path(__file__).resolve().parents[2]
    for name in (".env", ".env.env", ".env.local"):
        load_dotenv(root / name, override=False)
