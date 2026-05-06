from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt import cli
from sol_cgt.accounting.engine import AccountingResult
from sol_cgt.config import AppSettings
from sol_cgt.types import MissingLotIssue, NormalizedEvent, WarningRecord


def _event(i: str) -> NormalizedEvent:
    return NormalizedEvent(id=i, ts=datetime(2025, 1, 1, tzinfo=timezone.utc), kind="sell", wallet="w", fee_sol=Decimal("0"), raw={})


def _base_mocks(monkeypatch, settings: AppSettings, events: list[NormalizedEvent]) -> None:
    monkeypatch.setattr(cli, "load_settings", lambda *a, **k: settings)
    monkeypatch.setattr(cli.fetch_mod, "cache_has_data", lambda _: True)
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda _: [{"sig": 1}])
    monkeypatch.setattr(cli, "_load_and_normalize", lambda *a, **k: (events, {"sell": len(events)}))
    monkeypatch.setattr(cli.transfers, "classify_internal_transfers", lambda *a, **k: {"owned_wallets": 1, "internal_transfers": 0, "external_transfer_in": 0, "external_transfer_out": 0})
    monkeypatch.setattr(cli.formats, "export_reports", lambda *a, **k: None)
    monkeypatch.setattr(cli.console_report, "render_summary", lambda *a, **k: None)
    async def _ensure(*a, **k):
        return None
    monkeypatch.setattr(cli.sol_price_table, "ensure_sol_usd_daily_prices", _ensure)
    monkeypatch.setattr(cli.fx_price_table, "ensure_usd_aud_daily_rates", _ensure)
    monkeypatch.setattr(cli.sol_price_table, "cache_stats", lambda *a, **k: (0, None, None))
    monkeypatch.setattr(cli.fx_price_table, "cache_stats", lambda *a, **k: (0, None, None))
    monkeypatch.setattr(cli, "AudPriceProvider", lambda *a, **k: type("P", (), {"fx_rate": lambda *_: Decimal("1"), "price_aud": lambda *_1, **_2: Decimal("1")})())


def test_missing_lot_no_auto_backfill_by_default(monkeypatch, caplog) -> None:
    settings = AppSettings(wallets=["w"], auto_backfill=False)
    events = [_event("e1")]
    _base_mocks(monkeypatch, settings, events)
    monkeypatch.setattr(cli.valuation_module, "valuate_events", lambda *a, **k: [])
    issue = MissingLotIssue(wallet="w", mint="m", ts=events[0].ts, signature="s", event_id="e1", event_type="sell", required_qty=Decimal("2"), available_qty=Decimal("1"), shortfall_qty=Decimal("1"), message="missing")
    def _run(**kwargs):
        kwargs["missing_lot_issues"].append(issue)
        return AccountingResult(acquisitions=[], disposals=[], lot_moves=[], warnings=[]), True
    monkeypatch.setattr(cli, "_run_accounting", _run)

    with caplog.at_level("WARNING"):
        try:
            cli.compute(wallet=["w"], config=None, outdir=None, method=None, fy=None, fy_start=None, fy_end=None, fmt="csv", xlsx_path=None, sol_price_csv=None, fetch=False, dry_run=False)
        except cli.typer.Exit:
            pass

    assert any("Auto-backfill disabled" in r.message for r in caplog.records)


def test_price_warnings_do_not_trigger_backfill_even_if_enabled(monkeypatch, caplog) -> None:
    settings = AppSettings(wallets=["w"], auto_backfill=True)
    events = [_event("e1")]
    _base_mocks(monkeypatch, settings, events)
    monkeypatch.setattr(cli.valuation_module, "valuate_events", lambda *a, **k: [WarningRecord(ts=events[0].ts, wallet="w", signature="s", code="missing_token_price_no_counterparty_leg", message="missing")])
    monkeypatch.setattr(cli, "_run_accounting", lambda **kwargs: (AccountingResult(acquisitions=[], disposals=[], lot_moves=[], warnings=[]), False))

    with caplog.at_level("INFO"):
        cli.compute(wallet=["w"], config=None, outdir=None, method=None, fy=None, fy_start=None, fy_end=None, fmt="csv", xlsx_path=None, sol_price_csv=None, fetch=False, dry_run=False, enable_auto_backfill=True)

    assert not any("Auto-backfill attempt=" in r.message for r in caplog.records)
