from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.ingestion import normalize
from sol_cgt.pricing import TimestampPriceProvider
from sol_cgt.pricing import valuation as valuation_module
from sol_cgt.providers import birdeye
from sol_cgt.types import NormalizedEvent, TokenAmount


class FixedUsdProvider(TimestampPriceProvider):
    def __init__(self, prices: dict[str, Decimal]) -> None:
        super().__init__(api_key=None)
        self.prices = prices

    def price_usd(self, mint: str, ts: datetime) -> Decimal | None:
        return self.prices.get(mint)


def test_swap_valuation_uses_sol_anchor(tmp_path) -> None:
    raw_tx = {
        "signature": "swap-sol",
        "timestamp": int(datetime(2024, 6, 1, tzinfo=timezone.utc).timestamp()),
        "events": {
            "swap": {
                "tokenInputs": [{"mint": "TOKENX", "decimals": 6, "amount": "2"}],
                "nativeOutput": {"amount": 1_000_000_000},
            }
        },
    }

    events = asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            [raw_tx],
            mint_cache_path=tmp_path / "mint_meta.json",
        )
    )
    warnings = valuation_module.valuate_events(
        events,
        valuation_module.ValuationContext(
            usd_provider=FixedUsdProvider({"SOL": Decimal("50")}),
            fx_rate=lambda _: Decimal("1.0"),
        ),
    )
    assert warnings == []
    out_event = next(ev for ev in events if ev.raw.get("swap_direction") == "out")
    in_event = next(ev for ev in events if ev.raw.get("swap_direction") == "in")
    assert Decimal(out_event.raw["proceeds_hint_usd"]) == Decimal("50")
    assert Decimal(in_event.raw["cost_hint_usd"]) == Decimal("50")


def test_non_swap_missing_price_warns() -> None:
    event = NormalizedEvent(
        id="airdrop#1",
        ts=datetime(2024, 7, 1, tzinfo=timezone.utc),
        kind="airdrop",
        base_token=None,
        quote_token=TokenAmount(mint="TOKENZ", symbol="TZ", decimals=6, amount_raw=1_000_000),
        fee_sol=Decimal("0"),
        wallet="WALLET",
        counterparty=None,
        raw={},
        tags=set(),
    )
    ctx = valuation_module.ValuationContext(
        usd_provider=TimestampPriceProvider(api_key=None),
        fx_rate=lambda _: Decimal("1.0"),
    )
    warnings = valuation_module.valuate_events([event], ctx)
    assert warnings
    assert event.raw.get("unpriced") is True


def test_provider_401_does_not_crash(monkeypatch, caplog) -> None:
    async def fail_birdeye(*args, **kwargs):
        raise birdeye.ProviderUnavailable("unauthorized")

    monkeypatch.setattr(birdeye, "historical_price_usd", fail_birdeye)
    provider = TimestampPriceProvider(api_key="key")
    ts = datetime(2024, 8, 1, tzinfo=timezone.utc)
    with caplog.at_level("WARNING"):
        assert provider.price_usd("SOL", ts) is None
    assert any("Birdeye price lookup failed" in record.message for record in caplog.records)
