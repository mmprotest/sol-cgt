from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from sol_cgt import pricing
from sol_cgt.providers import fx_price_table


def test_missing_ranges_collapsed() -> None:
    ranges = fx_price_table._collapse_ranges([date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 4)])
    assert ranges == [(date(2024, 1, 1), date(2024, 1, 2)), (date(2024, 1, 4), date(2024, 1, 4))]


def test_ensure_rates_uses_cache_when_range_already_covered(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "fx.csv"
    monkeypatch.setattr(fx_price_table, "CACHE_PATH", cache_path)
    fx_price_table._write_cache(cache_path, [fx_price_table.FxDailyRate(date(2024, 1, 1), Decimal("1.5")), fx_price_table.FxDailyRate(date(2024, 1, 2), Decimal("1.6"))])

    async def fail_download(*args, **kwargs):
        raise AssertionError("download should not be called")

    monkeypatch.setattr(fx_price_table, "_download_range", fail_download)
    path = __import__("asyncio").run(fx_price_table.ensure_usd_aud_daily_rates(date(2024, 1, 1), date(2024, 1, 2)))
    assert path == cache_path


def test_fx_rate_uses_au_local_date_and_prior_fallback(monkeypatch) -> None:
    captured = {}

    def fake(day):
        captured["day"] = day
        if day.isoformat() == "2024-01-02":
            return Decimal("1.55")
        return None

    monkeypatch.setattr(pricing.fx_price_table, "get_usd_aud_for_date_or_prior", fake)
    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, 13, 30, tzinfo=timezone.utc)
    assert provider.fx_rate(ts) == Decimal("1.55")
    assert captured["day"].isoformat() == "2024-01-02"


def test_fx_rate_no_network_calls(monkeypatch) -> None:
    monkeypatch.setattr(pricing.fx_price_table, "get_usd_aud_for_date_or_prior", lambda _day: Decimal("1.5"))
    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.fx_rate(ts) == Decimal("1.5")
