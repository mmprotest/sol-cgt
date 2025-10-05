"""Output writers for CSV and Parquet reports."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

from ..types import AcquisitionLot, DisposalRecord


def _records_to_frame(records: Sequence) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame([r.model_dump() for r in records])


def write_csv(path: Path, records: Sequence) -> None:
    df = _records_to_frame(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_parquet(path: Path, records: Sequence) -> None:
    df = _records_to_frame(records)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def export_reports(
    outdir: Path,
    acquisitions: Sequence[AcquisitionLot],
    disposals: Sequence[DisposalRecord],
    summary_by_token: pd.DataFrame,
    summary_overall: pd.DataFrame,
    *,
    fmt: str = "csv",
) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    if fmt in ("csv", "both"):
        write_csv(outdir / "acquisitions.csv", acquisitions)
        write_csv(outdir / "disposals.csv", disposals)
        summary_by_token.to_csv(outdir / "summary_by_token.csv", index=False)
        summary_overall.to_csv(outdir / "summary_overall.csv", index=False)
    if fmt in ("parquet", "both"):
        write_parquet(outdir / "acquisitions.parquet", acquisitions)
        write_parquet(outdir / "disposals.parquet", disposals)
        summary_by_token.to_parquet(outdir / "summary_by_token.parquet", index=False)
        summary_overall.to_parquet(outdir / "summary_overall.parquet", index=False)
