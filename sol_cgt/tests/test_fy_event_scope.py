from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt import cli, utils
from sol_cgt.accounting.engine import AccountingResult
from sol_cgt.config import APIKeys, AppSettings
from sol_cgt.types import DisposalRecord, NormalizedEvent


def _event(event_id: str, ts: datetime) -> NormalizedEvent:
    return NormalizedEvent(id=event_id, ts=ts, kind="unknown", wallet="w", fee_sol=Decimal("0"), raw={})


def test_events_required_for_fy_keeps_prefy_and_fy_drops_post_fy() -> None:
    fy_period = utils.australian_financial_year_bounds("2024-2025")
    events = [
        _event("pre", datetime(2024, 6, 1, 0, 0, tzinfo=timezone.utc)),
        _event("in", datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc)),
        _event("post", datetime(2025, 7, 1, 0, 0, tzinfo=timezone.utc)),
    ]

    scoped = cli._events_required_for_fy(events, fy_period)
    assert [event.id for event in scoped] == ["pre", "in"]


def test_events_required_for_fy_uses_au_local_date_cutoff() -> None:
    fy_period = utils.australian_financial_year_bounds("2024-2025")
    # 2025-06-30 in AU local time (AEST) -> should be retained.
    in_au_day = _event("in-au", datetime(2025, 6, 30, 13, 30, tzinfo=timezone.utc))
    # 2025-07-01 in AU local time -> should be dropped.
    post_au_day = _event("post-au", datetime(2025, 6, 30, 15, 0, tzinfo=timezone.utc))

    scoped = cli._events_required_for_fy([in_au_day, post_au_day], fy_period)
    assert [event.id for event in scoped] == ["in-au"]


def test_required_price_dates_excludes_post_fy_events() -> None:
    fy_period = utils.australian_financial_year_bounds("2024-2025")
    events = [
        _event("pre", datetime(2024, 6, 1, 0, 0, tzinfo=timezone.utc)),
        _event("in", datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc)),
    ]
    scoped = cli._events_required_for_fy(events, fy_period)
    start_day, end_day = cli._required_price_dates(scoped, fy_period)
    assert start_day.isoformat() == "2024-07-01"
    assert end_day.isoformat() == "2025-06-30"


def test_no_fy_keeps_all_events() -> None:
    events = [
        _event("a", datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)),
        _event("b", datetime(2025, 10, 11, 0, 0, tzinfo=timezone.utc)),
    ]
    assert cli._events_required_for_fy(events, None) == events


def test_compute_values_only_scoped_events(monkeypatch) -> None:
    settings = AppSettings(wallets=["wallet"], api_keys=APIKeys(helius="key"))
    monkeypatch.setattr(cli, "load_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(cli.fetch_mod, "cache_has_data", lambda _: True)
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda _: [])

    events = [
        _event("pre", datetime(2024, 6, 1, 0, 0, tzinfo=timezone.utc)),
        _event("fy", datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc)),
        _event("post", datetime(2025, 10, 11, 0, 0, tzinfo=timezone.utc)),
    ]
    monkeypatch.setattr(cli, "_load_and_normalize", lambda *args, **kwargs: (events, {"unknown": len(events)}))
    async def fake_ensure_sol(*args, **kwargs):
        return None

    async def fake_ensure_fx(*args, **kwargs):
        return None

    monkeypatch.setattr(cli.sol_price_table, "ensure_sol_usd_daily_prices", fake_ensure_sol)
    monkeypatch.setattr(cli.sol_price_table, "cache_stats", lambda *_: (0, None, None))
    monkeypatch.setattr(cli.fx_price_table, "ensure_usd_aud_daily_rates", fake_ensure_fx)
    monkeypatch.setattr(cli.fx_price_table, "cache_stats", lambda *_: (0, None, None))

    class DummyPriceProvider:
        def __init__(self, *args, **kwargs) -> None:
            self.fx_dates: list[str] = []

        def fx_rate(self, ts):
            self.fx_dates.append(utils.to_au_local(ts).date().isoformat())
            return Decimal("1")

        def price_aud(self, *args, **kwargs):
            return Decimal("1")

    monkeypatch.setattr(cli, "AudPriceProvider", DummyPriceProvider)

    captured_ids: list[str] = []

    def fake_valuate_events(input_events, ctx):
        for event in input_events:
            captured_ids.append(event.id)
            ctx.fx_rate(event.ts)
        return []

    monkeypatch.setattr(cli.valuation_module, "valuate_events", fake_valuate_events)

    in_fy_disposal = DisposalRecord(
        event_id="fy",
        wallet="wallet",
        ts=datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc),
        token_mint="mint",
        qty_disposed=Decimal("1"),
        proceeds_aud=Decimal("1"),
        cost_base_aud=Decimal("1"),
        fees_aud=Decimal("0"),
        gain_loss_aud=Decimal("0"),
        long_term=False,
        held_days=1,
        method="FIFO",
    )
    monkeypatch.setattr(
        cli,
        "_run_accounting",
        lambda **kwargs: (AccountingResult(acquisitions=[], disposals=[in_fy_disposal], lot_moves=[], warnings=[]), False),
    )
    monkeypatch.setattr(cli.formats, "export_reports", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli.console_report, "render_summary", lambda *args, **kwargs: None)

    cli.compute(wallet=["wallet"], fy="2024-2025", fy_start=None, fy_end=None, dry_run=False, fetch=False, fmt="csv", config=None, outdir=None, method=None, xlsx_path=None, sol_price_csv=None)

    assert captured_ids == ["pre", "fy"]
