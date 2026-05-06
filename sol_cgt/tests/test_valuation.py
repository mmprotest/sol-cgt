from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.ingestion import normalize
from sol_cgt.pricing import TimestampPriceProvider
from sol_cgt.pricing import valuation as valuation_module
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


def test_provider_returns_none_for_non_sol_token() -> None:
    provider = TimestampPriceProvider(api_key="key")
    ts = datetime(2024, 8, 1, tzinfo=timezone.utc)
    assert provider.price_usd("TOKENX", ts) is None


def test_buy_swap_infers_from_sol_leg_no_token_price_lookup() -> None:
    ts = datetime(2024, 7, 1, tzinfo=timezone.utc)
    event = NormalizedEvent(
        id="sig#1",
        ts=ts,
        kind="buy",
        quote_token=TokenAmount(mint="TOKENX", symbol="TKX", decimals=6, amount_raw=10_000_000_000),
        fee_sol=Decimal("0"),
        wallet="WALLET",
        raw={
            "signature": "sig",
            "swap_legs": [
                {"mint": "So11111111111111111111111111111111111111112", "amount": "0.5", "direction": "out"},
                {"mint": "TOKENX", "amount": "10000", "direction": "in"},
            ],
        },
    )
    ctx = valuation_module.ValuationContext(
        usd_provider=FixedUsdProvider({"SOL": Decimal("100")}),
        fx_rate=lambda _: Decimal("1.5"),
    )
    warnings = valuation_module.valuate_events([event], ctx)
    assert warnings == []
    assert Decimal(event.raw["cost_hint_usd"]) == Decimal("50.0")
    assert Decimal(event.raw["cost_hint_aud"]) == Decimal("75.00")
    assert event.raw["valuation_method"] == "inferred_from_sol_leg"


def test_ambiguous_multi_token_swap_warns() -> None:
    ts = datetime(2024, 7, 1, tzinfo=timezone.utc)
    event = NormalizedEvent(
        id="sig2#1",
        ts=ts,
        kind="buy",
        quote_token=TokenAmount(mint="TOKENX", symbol="TKX", decimals=6, amount_raw=1_000_000),
        fee_sol=Decimal("0"),
        wallet="WALLET",
        raw={
            "signature": "sig2",
            "swap_legs": [
                {"mint": "WSOL", "amount": "0.5", "direction": "out"},
                {"mint": "TOKENX", "amount": "1", "direction": "in"},
                {"mint": "TOKENY", "amount": "2", "direction": "in"},
            ],
        },
    )
    ctx = valuation_module.ValuationContext(
        usd_provider=FixedUsdProvider({"SOL": Decimal("100")}),
        fx_rate=lambda _: Decimal("1.0"),
    )
    warnings = valuation_module.valuate_events([event], ctx)
    assert warnings
    assert warnings[0].code == "ambiguous_multi_token_swap"
    assert event.raw["valuation_method"] == "ambiguous_multi_token_swap"


def test_inferred_transfer_swap_with_anchor_hint_avoids_missing_price_warning() -> None:
    ts = datetime(2024, 7, 2, tzinfo=timezone.utc)
    event = NormalizedEvent(
        id="sig3#1",
        ts=ts,
        kind="swap",
        quote_token=TokenAmount(mint="TOKENQ", symbol="TKQ", decimals=6, amount_raw=1_000_000),
        fee_sol=Decimal("0"),
        wallet="WALLET",
        raw={
            "signature": "sig3",
            "source": "inferred_transfer_swap",
            "cost_hint_aud": "12.34",
        },
    )
    ctx = valuation_module.ValuationContext(
        usd_provider=TimestampPriceProvider(api_key=None),
        fx_rate=lambda _: Decimal("1.0"),
    )
    warnings = valuation_module.valuate_events([event], ctx)
    assert not any(w.code == "missing_token_price_no_counterparty_leg" for w in warnings)
