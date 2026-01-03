from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.ingestion import normalize
from sol_cgt.pricing import TimestampPriceProvider
from sol_cgt.pricing import valuation as valuation_module


class FakeUsdProvider(TimestampPriceProvider):
    def __init__(self) -> None:
        super().__init__(api_key=None)

    def price_usd(self, mint: str, ts: datetime) -> Decimal | None:
        if mint in {"SOL", "So11111111111111111111111111111111111111112"}:
            return Decimal("100")
        return None


def test_swap_proceeds_hint_uses_stablecoin(tmp_path) -> None:

    raw_tx = {
        "signature": "swap-hint",
        "timestamp": int(datetime(2024, 5, 1, tzinfo=timezone.utc).timestamp()),
        "events": {
            "swap": {
                "tokenInputs": [
                    {"mint": "TOKENA", "decimals": 6, "amount": "1"},
                ],
                "tokenOutputs": [
                    {
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "decimals": 6,
                        "amount": "10",
                    },
                ],
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
            usd_provider=FakeUsdProvider(),
            fx_rate=lambda _: Decimal("1.0"),
        ),
    )
    assert warnings == []
    out_event = next(ev for ev in events if ev.raw.get("swap_direction") == "out")
    in_event = next(ev for ev in events if ev.raw.get("swap_direction") == "in")
    assert Decimal(out_event.raw["proceeds_hint_usd"]) == Decimal("10")
    assert Decimal(in_event.raw["cost_hint_usd"]) == Decimal("10")
