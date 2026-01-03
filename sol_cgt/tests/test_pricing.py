from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt import pricing


def test_sol_price_via_coingecko(monkeypatch) -> None:
    async def fake_sol_price(ts):
        return Decimal("20")

    async def fake_fx(day):
        return Decimal("1.5")

    monkeypatch.setattr(pricing.coingecko, "sol_price_usd", fake_sol_price)
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

    async def fake_sol_price(ts):
        calls["count"] += 1
        return Decimal("10")

    async def fake_fx(day):
        return Decimal("1.0")

    monkeypatch.setattr(pricing.coingecko, "sol_price_usd", fake_sol_price)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    provider.price_aud("SOL", ts)
    provider.price_aud("SOL", ts)
    assert calls["count"] == 1
