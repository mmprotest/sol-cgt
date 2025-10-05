from __future__ import annotations

from decimal import Decimal
import asyncio

from sol_cgt.ingestion import normalize


def test_normalize_basic(monkeypatch) -> None:
    async def fake_symbol_decimals(mint: str):
        return ("TEST", 6)

    monkeypatch.setattr(normalize.birdeye, "symbol_decimals", fake_symbol_decimals)
    raw_tx = {
        "signature": "sig1",
        "timestamp": 1700000000,
        "events": [
            {
                "type": "swap",
                "base": {"mint": "TOKENA", "amount_raw": 5000000, "decimals": 6},
                "quote": {"mint": "TOKENB", "amount_raw": 2000000, "decimals": 6},
                "fee_lamports": 5000,
            }
        ],
    }
    events = asyncio.run(normalize.normalize_wallet_events("WALLET", [raw_tx]))
    assert len(events) == 1
    event = events[0]
    assert event.kind == "swap"
    assert event.base_token is not None
    assert event.base_token.amount == Decimal("5")
    assert event.fee_sol > Decimal("0")
