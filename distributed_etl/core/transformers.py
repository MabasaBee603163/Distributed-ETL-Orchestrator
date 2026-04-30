from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from .base import Dataset, Row, Transformer, coerce_rows, require_columns


def _is_blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


@dataclass(frozen=True, slots=True)
class BasicCleaningTransformer(Transformer):
    """Small, composable transform for common hygiene steps."""

    required_columns: tuple[str, ...] = ()
    drop_blank_rows: bool = True
    strip_strings: bool = True

    def transform(self, rows: Dataset) -> Dataset:
        require_columns(rows, columns=self.required_columns)

        out: Dataset = []
        for r in rows:
            rr: Row = dict(r)
            if self.strip_strings:
                for k, v in list(rr.items()):
                    if isinstance(v, str):
                        rr[k] = v.strip()

            if self.drop_blank_rows and rr and all(_is_blank(v) for v in rr.values()):
                continue

            out.append(rr)
        return out


@dataclass(frozen=True, slots=True)
class ColumnMapper(Transformer):
    """Rename/select columns via a mapping: {source: dest}."""

    mapping: dict[str, str]
    keep_unmapped: bool = False

    def transform(self, rows: Dataset) -> Dataset:
        out: Dataset = []
        for r in rows:
            rr: Row = {}
            if self.keep_unmapped:
                rr.update(r)
            for src, dest in self.mapping.items():
                if src in r:
                    rr[dest] = r[src]
            out.append(rr)
        return out


@dataclass(frozen=True, slots=True)
class FunctionTransformer(Transformer):
    """Plug in custom logic without creating a new class."""

    fn: Callable[[Dataset], Dataset]

    def transform(self, rows: Dataset) -> Dataset:
        return coerce_rows(self.fn(rows))


def chain(rows: Iterable[Mapping[str, Any]], *transformers: Transformer) -> Dataset:
    cur = coerce_rows(rows)
    for t in transformers:
        cur = t.transform(cur)
    return cur

