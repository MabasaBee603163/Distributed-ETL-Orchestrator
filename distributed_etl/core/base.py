from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


Row = dict[str, Any]
Dataset = list[Row]


@dataclass(frozen=True, slots=True)
class ExtractResult:
    rows: Dataset
    meta: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class LoadResult:
    rows_loaded: int
    meta: dict[str, Any] | None = None


class Extractor(ABC):
    """Extracts data from a source into an in-memory dataset."""

    @abstractmethod
    def extract(self) -> ExtractResult:  # pragma: no cover
        raise NotImplementedError


class Transformer(ABC):
    """Transforms/validates a dataset."""

    @abstractmethod
    def transform(self, rows: Dataset) -> Dataset:  # pragma: no cover
        raise NotImplementedError


class Loader(ABC):
    """Loads a dataset into a destination."""

    @abstractmethod
    def load(self, rows: Dataset) -> LoadResult:  # pragma: no cover
        raise NotImplementedError


def coerce_rows(rows: Iterable[Mapping[str, Any]]) -> Dataset:
    """Normalize any mapping-like rows into mutable dict rows."""

    return [dict(r) for r in rows]


def require_columns(rows: Sequence[Mapping[str, Any]], *, columns: Sequence[str]) -> None:
    if not rows:
        return
    missing = [c for c in columns if c not in rows[0]]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

