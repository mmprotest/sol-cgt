from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.engine import AccountingEngine
from sol_cgt.ingestion import normalize
from sol_cgt.types import NormalizedEvent, TokenAmount


class FakePriceProvider:
    def price_aud(self, mint: str, ts: datetime, *, context: dict | None = None) -> Decimal:
        prices = {
            "TOKENA": Decimal("1000"),
            "TOKENB": Decimal("1"),
        }
        if mint in prices:
            return prices[mint]
        raise ValueError(f"missing price for {mint}")


def test_swap_proceeds_hint_uses_incoming_value(tmp_path) -> None:

    raw_tx = {
        "signature": "swap-hint",
        "timestamp": int(datetime(2024, 5, 1, tzinfo=timezone.utc).timestamp()),
        "events": {
            "swap": {
                "tokenInputs": [
                    {"mint": "TOKENA", "decimals": 6, "amount": "1", "price_aud": "100"},
                ],
                "tokenOutputs": [
                    {"mint": "TOKENB", "decimals": 6, "amount": "10", "price_aud": "2"},
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
    buy_event = NormalizedEvent(
        id="buy#0",
        ts=datetime(2024, 4, 1, tzinfo=timezone.utc),
        kind="buy",
        base_token=None,
        quote_token=TokenAmount(mint="TOKENA", symbol="TKA", decimals=6, amount_raw=1_000_000),
        fee_sol=Decimal("0"),
        wallet="WALLET",
        counterparty=None,
        raw={"cost_aud": "50"},
        tags=set(),
    )
    events = [buy_event, *events]
    engine = AccountingEngine(price_provider=FakePriceProvider())
    result = engine.process(events, wallets=["WALLET"])

    disposal = next(record for record in result.disposals if record.token_mint == "TOKENA")
    assert disposal.proceeds_aud == Decimal("20.00")
    acquisition = next(lot for lot in result.acquisitions if lot.token_mint == "TOKENB")
    assert acquisition.unit_cost_aud == Decimal("10.00")
