from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt import pricing


def test_sol_price_via_kraken(monkeypatch) -> None:
    async def fake_sol_price(day):
        return Decimal("20")

    async def fake_fx(day):
        return Decimal("1.5")

    monkeypatch.setattr(pricing.kraken, "get_sol_usd_close_for_date", fake_sol_price)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.price_aud("SOL", ts) == Decimal("30.00")


def test_stablecoin_conversion(monkeypatch) -> None:
    async def fake_fx(day):
        return Decimal("1.5")

    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.price_aud(pricing.USDC_MINT, ts) == Decimal("1.50")


def test_price_cache_hit(monkeypatch) -> None:
    calls = {"count": 0}

    async def fake_sol_price(day):
        calls["count"] += 1
        return Decimal("10")

    async def fake_fx(day):
        return Decimal("1.0")

    monkeypatch.setattr(pricing.kraken, "get_sol_usd_close_for_date", fake_sol_price)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    provider.price_aud("SOL", ts)
    provider.price_aud("SOL", ts)
    assert calls["count"] == 1


def test_sol_price_birdeye_then_kraken(monkeypatch) -> None:
    calls = {"birdeye": 0, "kraken": 0}

    async def fake_birdeye(*args, **kwargs):
        calls["birdeye"] += 1
        raise RuntimeError("birdeye down")

    async def fake_kraken(day):
        calls["kraken"] += 1
        return Decimal("12")

    async def fake_fx(day):
        return Decimal("2")

    monkeypatch.setattr(pricing.birdeye, "historical_price_usd", fake_birdeye)
    monkeypatch.setattr(pricing.kraken, "get_sol_usd_close_for_date", fake_kraken)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider(api_key="key")
    ts = datetime(2024, 2, 1, tzinfo=timezone.utc)
    assert provider.price_aud("SOL", ts) == Decimal("24.00")
    assert calls["birdeye"] == 1
    assert calls["kraken"] == 1


def test_sol_price_missing_warns(monkeypatch, caplog) -> None:
    async def fake_kraken(day):
        return None

    async def fake_fx(day):
        return Decimal("1")

    def fail_coingecko(*args, **kwargs):
        raise AssertionError("CoinGecko should not be called without key")

    monkeypatch.setattr(pricing.kraken, "get_sol_usd_close_for_date", fake_kraken)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)
    monkeypatch.setattr(pricing.coingecko, "sol_price_usd", fail_coingecko)

    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 2, 1, tzinfo=timezone.utc)
    with caplog.at_level("WARNING"):
        assert provider.price_aud("SOL", ts) is None
    assert any("Price not available" in record.message for record in caplog.records)


def test_sol_price_coingecko_after_kraken(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_kraken(day):
        calls.append("kraken")
        return None

    async def fake_coingecko(ts, *, api_key=None):
        calls.append("coingecko")
        return Decimal("15")

    async def fake_fx(day):
        return Decimal("2")

    monkeypatch.setattr(pricing.kraken, "get_sol_usd_close_for_date", fake_kraken)
    monkeypatch.setattr(pricing.coingecko, "sol_price_usd", fake_coingecko)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider(coingecko_api_key="key")
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    assert provider.price_aud("SOL", ts) == Decimal("30.00")
    assert calls == ["kraken", "coingecko"]
