from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt import pricing


def test_sol_price_via_local_table(monkeypatch) -> None:
    def fake_sol_price(*args, **kwargs):
        return Decimal("20")

    async def fake_fx(day):
        return Decimal("1.5")

    monkeypatch.setattr(pricing.sol_price_table, "get_sol_usd_close_for_date", fake_sol_price)
    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider(api_key="key")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.price_aud("SOL", ts) == Decimal("30.00")


def test_stablecoin_conversion(monkeypatch) -> None:
    async def fake_fx(day):
        return Decimal("1.5")

    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    provider = pricing.AudPriceProvider()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.price_aud(pricing.USDC_MINT, ts) == Decimal("1.50")


def test_non_sol_token_unpriced(monkeypatch, tmp_path) -> None:
    async def fake_fx(day):
        return Decimal("1.0")

    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)

    cache = pricing.PriceCache(tmp_path)
    usd_provider = pricing.TimestampPriceProvider(api_key="key", cache=cache)
    provider = pricing.AudPriceProvider(api_key="key", usd_provider=usd_provider)
    ts = datetime(2024, 1, 1, 0, 0, 10, tzinfo=timezone.utc)
    assert provider.price_aud("TOKEN", ts) is None


def test_sol_price_missing_warns(monkeypatch, caplog) -> None:
    async def fake_fx(day):
        return Decimal("1")

    monkeypatch.setattr(pricing.fx_rates, "usd_to_aud_rate", fake_fx)
    monkeypatch.setattr(pricing.sol_price_table, "get_sol_usd_close_for_date", lambda *_: None)

    provider = pricing.AudPriceProvider(api_key="key")
    ts = datetime(2024, 2, 1, tzinfo=timezone.utc)
    with caplog.at_level("WARNING"):
        assert provider.price_aud("SOL", ts) is None
    assert any("Price not available" in record.message for record in caplog.records)

