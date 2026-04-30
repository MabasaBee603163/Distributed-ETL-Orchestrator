from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .base import ExtractResult, Extractor, Row


@dataclass(frozen=True, slots=True)
class CSVExtractor(Extractor):
    path: str | Path
    encoding: str = "utf-8"

    def extract(self) -> ExtractResult:
        p = Path(self.path)
        if not p.exists():
            raise FileNotFoundError(str(p))

        with p.open("r", encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f)
            rows: list[Row] = [dict(r) for r in reader]

        return ExtractResult(rows=rows, meta={"source": "csv", "path": str(p), "rows": len(rows)})


@dataclass(frozen=True, slots=True)
class APIExtractor(Extractor):
    url: str
    method: str = "GET"
    headers: dict[str, str] | None = None
    params: dict[str, Any] | None = None
    json_body: Any | None = None
    timeout_s: float = 30.0

    def extract(self) -> ExtractResult:
        with httpx.Client(timeout=self.timeout_s) as client:
            resp = client.request(
                self.method,
                self.url,
                headers=self.headers,
                params=self.params,
                json=self.json_body,
            )
            resp.raise_for_status()

            content_type = resp.headers.get("content-type", "")
            if "application/json" in content_type:
                payload = resp.json()
            else:
                # Best-effort fallback for APIs that return JSON without correct header.
                payload = json.loads(resp.text)

        if isinstance(payload, list):
            rows = [dict(r) for r in payload]
        elif isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
            rows = [dict(r) for r in payload["data"]]
        else:
            raise ValueError("API response was not a list of objects (or {data: [...]})")

        return ExtractResult(rows=rows, meta={"source": "api", "url": self.url, "rows": len(rows)})

