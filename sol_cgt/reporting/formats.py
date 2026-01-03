"""Output writers for CSV and Parquet reports."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import csv

from ..types import AcquisitionLot, DisposalRecord
from .schema import (
    ACQUISITION_COLUMNS,
    DISPOSAL_COLUMNS,
    SUMMARY_BY_TOKEN_COLUMNS,
    SUMMARY_OVERALL_COLUMNS,
)

Row = dict[str, Any]


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _normalize_rows(records: Sequence) -> list[Row]:
    rows: list[Row] = []
    for record in records:
        if isinstance(record, dict):
            rows.append(record)
        else:
            rows.append(record.model_dump())
    return rows


def write_csv(path: Path, records: Sequence, *, columns: Sequence[str]) -> None:
    rows = _normalize_rows(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _serialize_value(row.get(key)) for key in columns})


def write_parquet(path: Path, records: Sequence, *, columns: Sequence[str]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - behavior tested
        raise RuntimeError(
            "Parquet support requires the 'parquet' extra. Install with: pip install -e '.[parquet]'"
        ) from exc
    rows = _normalize_rows(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        table = pa.Table.from_pylist(rows)
    else:
        table = pa.table({column: [] for column in columns})
    pq.write_table(table, path)


def export_reports(
    outdir: Path,
    acquisitions: Sequence[AcquisitionLot],
    disposals: Sequence[DisposalRecord],
    summary_by_token: Sequence[Row],
    summary_overall: Sequence[Row],
    *,
    fmt: str = "csv",
) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    if fmt in ("csv", "both"):
        write_csv(outdir / "acquisitions.csv", acquisitions, columns=ACQUISITION_COLUMNS)
        write_csv(outdir / "disposals.csv", disposals, columns=DISPOSAL_COLUMNS)
        write_csv(outdir / "summary_by_token.csv", summary_by_token, columns=SUMMARY_BY_TOKEN_COLUMNS)
        write_csv(outdir / "summary_overall.csv", summary_overall, columns=SUMMARY_OVERALL_COLUMNS)
    if fmt in ("parquet", "both"):
        write_parquet(outdir / "acquisitions.parquet", acquisitions, columns=ACQUISITION_COLUMNS)
        write_parquet(outdir / "disposals.parquet", disposals, columns=DISPOSAL_COLUMNS)
        write_parquet(outdir / "summary_by_token.parquet", summary_by_token, columns=SUMMARY_BY_TOKEN_COLUMNS)
        write_parquet(outdir / "summary_overall.parquet", summary_overall, columns=SUMMARY_OVERALL_COLUMNS)
