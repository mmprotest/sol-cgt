from __future__ import annotations

import builtins
from datetime import datetime, timezone
from decimal import Decimal

from openpyxl import load_workbook
import pytest

from sol_cgt.reporting import formats, summaries, xlsx
from sol_cgt.reporting.schema import (
    ACQUISITION_COLUMNS,
    DISPOSAL_COLUMNS,
    SUMMARY_BY_TOKEN_COLUMNS,
    SUMMARY_OVERALL_COLUMNS,
    WALLET_SUMMARY_COLUMNS,
)
from sol_cgt.types import AcquisitionLot, DisposalRecord, NormalizedEvent, TokenAmount, WarningRecord


class DummyPriceProvider:
    def price_aud(self, mint: str, ts: datetime, *, context: dict | None = None) -> Decimal:
        return Decimal("1")


def _sample_records() -> tuple[
    list[NormalizedEvent],
    list[AcquisitionLot],
    list[DisposalRecord],
    list[WarningRecord],
]:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    lot = AcquisitionLot(
        lot_id="L1",
        wallet="W1",
        ts=ts,
        token_mint="M1",
        token_symbol="TOK",
        qty_acquired=Decimal("1"),
        unit_cost_aud=Decimal("10"),
        fees_aud=Decimal("0.10"),
        remaining_qty=Decimal("1"),
        source_event="sig1",
        source_type="swap",
    )
    disposal = DisposalRecord(
        event_id="E1",
        wallet="W1",
        ts=ts,
        token_mint="M1",
        token_symbol="TOK",
        qty_disposed=Decimal("1"),
        proceeds_aud=Decimal("12"),
        cost_base_aud=Decimal("10"),
        fees_aud=Decimal("0.10"),
        gain_loss_aud=Decimal("1.90"),
        long_term=False,
        held_days=10,
        method="FIFO",
        signature="sig1",
        notes="note",
    )
    event = NormalizedEvent(
        id="sig1#0",
        ts=ts,
        kind="sell",
        base_token=TokenAmount(
            mint="M1",
            symbol="TOK",
            decimals=6,
            amount_raw=1_000_000,
            amount=Decimal("1"),
        ),
        wallet="W1",
        raw={"signature": "sig1", "proceeds_aud": "12.00"},
    )
    warning = WarningRecord(
        ts=ts,
        wallet="W1",
        signature="sig1",
        code="warn",
        message="test warning",
    )
    return [event], [lot], [disposal], [warning]


def test_csv_and_xlsx_outputs(tmp_path) -> None:
    events, acquisitions, disposals, warnings = _sample_records()
    summary_by_token = summaries.summarize_by_token(disposals)
    summary_overall = summaries.summarize_overall(disposals)
    wallet_summary = summaries.summarize_by_wallet(disposals)

    formats.export_reports(
        tmp_path,
        acquisitions,
        disposals,
        summary_by_token,
        summary_overall,
        fmt="csv",
    )

    assert (tmp_path / "acquisitions.csv").read_text(encoding="utf-8").splitlines()[0] == ",".join(
        ACQUISITION_COLUMNS
    )
    assert (tmp_path / "disposals.csv").read_text(encoding="utf-8").splitlines()[0] == ",".join(
        DISPOSAL_COLUMNS
    )
    assert (tmp_path / "summary_by_token.csv").read_text(encoding="utf-8").splitlines()[0] == ",".join(
        SUMMARY_BY_TOKEN_COLUMNS
    )
    assert (tmp_path / "summary_overall.csv").read_text(encoding="utf-8").splitlines()[0] == ",".join(
        SUMMARY_OVERALL_COLUMNS
    )

    xlsx_path = tmp_path / "report.xlsx"
    xlsx.export_xlsx(
        xlsx_path,
        overview={"Financial year": "2024-2025"},
        events=events,
        lots=acquisitions,
        disposals=disposals,
        summary_by_token=summary_by_token,
        wallet_summary=wallet_summary,
        lot_moves=[],
        warnings=warnings,
        price_provider=DummyPriceProvider(),
    )

    workbook = load_workbook(xlsx_path)
    assert "Overview" in workbook.sheetnames
    assert "Summary by token" in workbook.sheetnames
    assert "Wallet summary" in workbook.sheetnames
    assert [cell.value for cell in workbook["Summary by token"][1]] == SUMMARY_BY_TOKEN_COLUMNS
    assert [cell.value for cell in workbook["Wallet summary"][1]] == WALLET_SUMMARY_COLUMNS


def test_parquet_requires_extra(monkeypatch, tmp_path) -> None:
    original_import = builtins.__import__

    def _blocked_import(name: str, *args, **kwargs):
        if name.startswith("pyarrow"):
            raise ImportError("pyarrow missing")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)

    with pytest.raises(RuntimeError, match="Parquet support requires the 'parquet' extra"):
        formats.write_parquet(tmp_path / "summary.parquet", [], columns=SUMMARY_OVERALL_COLUMNS)
