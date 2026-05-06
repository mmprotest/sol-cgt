from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from sol_cgt import pricing
from sol_cgt.providers import fx_price_table


def test_missing_ranges_collapsed() -> None:
    ranges = fx_price_table._collapse_ranges([date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 4)])
    assert ranges == [(date(2024, 1, 1), date(2024, 1, 2)), (date(2024, 1, 4), date(2024, 1, 4))]


def test_ensure_rates_uses_cache_when_range_already_covered(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "fx.csv"
    monkeypatch.setattr(fx_price_table, "CACHE_PATH", cache_path)
    fx_price_table._write_cache(cache_path, [fx_price_table.FxDailyRate(date(2024, 1, 1), Decimal("1.5"))])

    async def no_rows_download(*args, **kwargs):
        return {}

    monkeypatch.setattr(fx_price_table, "_download_range", no_rows_download)
    path = __import__("asyncio").run(fx_price_table.ensure_usd_aud_daily_rates(date(2024, 1, 1), date(2024, 1, 2)))
    assert path == cache_path


def test_download_range_parses_frankfurter_v2_array(monkeypatch) -> None:
    payload = [
        {"date": "2024-07-01", "base": "USD", "quote": "AUD", "rate": 1.5},
        {"date": "2024-07-02", "base": "USD", "quote": "AUD", "rate": 1.6},
        {"date": "2024-07-02", "base": "USD", "quote": "EUR", "rate": 0.9},
    ]

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return FakeResp()

    monkeypatch.setattr(fx_price_table.httpx, "AsyncClient", lambda **kwargs: FakeClient())
    rows = __import__("asyncio").run(fx_price_table._download_range(date(2024, 7, 1), date(2024, 7, 2)))
    assert rows[date(2024, 7, 1)].usd_to_aud == Decimal("1.5")
    assert rows[date(2024, 7, 2)].usd_to_aud == Decimal("1.6")


def test_download_range_supports_old_dict_shape(monkeypatch) -> None:
    payload = {"rates": {"2024-07-01": {"AUD": 1.55}}}

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return FakeResp()

    monkeypatch.setattr(fx_price_table.httpx, "AsyncClient", lambda **kwargs: FakeClient())
    rows = __import__("asyncio").run(fx_price_table._download_range(date(2024, 7, 1), date(2024, 7, 1)))
    assert rows[date(2024, 7, 1)].usd_to_aud == Decimal("1.55")


def test_download_range_missing_rates_regression(monkeypatch) -> None:
    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def get(self, *args, **kwargs):
            return FakeResp()

    monkeypatch.setattr(fx_price_table.httpx, "AsyncClient", lambda **kwargs: FakeClient())
    with pytest.raises(RuntimeError, match="Missing rates from Frankfurter response"):
        __import__("asyncio").run(fx_price_table._download_range(date(2024, 7, 1), date(2024, 7, 1)))


def test_ensure_rates_weekend_gaps_are_allowed(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "fx.csv"
    monkeypatch.setattr(fx_price_table, "CACHE_PATH", cache_path)

    async def fake_download(*args, **kwargs):
        return {
            date(2024, 7, 1): fx_price_table.FxDailyRate(date(2024, 7, 1), Decimal("1.50")),
            date(2024, 7, 5): fx_price_table.FxDailyRate(date(2024, 7, 5), Decimal("1.51")),
        }

    monkeypatch.setattr(fx_price_table, "_download_range", fake_download)
    __import__("asyncio").run(fx_price_table.ensure_usd_aud_daily_rates(date(2024, 7, 1), date(2024, 7, 7)))
    rows = fx_price_table._read_cache(cache_path)
    assert date(2024, 7, 6) not in rows


def test_prior_fallback_and_window(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "fx.csv"
    monkeypatch.setattr(fx_price_table, "CACHE_PATH", cache_path)
    fx_price_table._write_cache(cache_path, [fx_price_table.FxDailyRate(date(2024, 7, 5), Decimal("1.52"))])
    assert fx_price_table.get_usd_aud_for_date_or_prior(date(2024, 7, 7), max_fallback_days=3) == Decimal("1.52")
    assert fx_price_table.get_usd_aud_for_date_or_prior(date(2024, 7, 20), max_fallback_days=10) is None


def test_fx_rate_uses_au_local_date_and_prior_fallback(monkeypatch) -> None:
    captured = {}

    def fake(day, *, max_fallback_days):
        captured["day"] = day
        captured["window"] = max_fallback_days
        if day.isoformat() == "2024-01-02":
            return Decimal("1.55")
        return None

    monkeypatch.setattr(pricing.fx_price_table, "get_usd_aud_for_date_or_prior", fake)
    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, 13, 30, tzinfo=timezone.utc)
    assert provider.fx_rate(ts) == Decimal("1.55")
    assert captured["day"].isoformat() == "2024-01-02"
    assert captured["window"] == 10


def test_fx_rate_fails_when_no_prior_rate_within_window(monkeypatch) -> None:
    monkeypatch.setattr(pricing.fx_price_table, "get_usd_aud_for_date_or_prior", lambda *_args, **_kwargs: None)
    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(RuntimeError, match="no prior rate within 10 days"):
        provider.fx_rate(ts)


def test_fx_rate_no_network_calls(monkeypatch) -> None:
    monkeypatch.setattr(pricing.fx_price_table, "get_usd_aud_for_date_or_prior", lambda _day, *, max_fallback_days: Decimal("1.5"))
    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.fx_rate(ts) == Decimal("1.5")
