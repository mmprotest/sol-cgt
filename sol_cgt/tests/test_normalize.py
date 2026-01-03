from __future__ import annotations

import asyncio
from decimal import Decimal

from sol_cgt.ingestion import normalize
from sol_cgt.pricing import USDC_MINT


def test_normalize_helius_enhanced(monkeypatch) -> None:
    async def fake_metadata(mint: str):
        if mint == "TOKENB":
            return ("TKB", 6)
        return (None, None)

    def fake_fx(day):
        return Decimal("1.5")

    monkeypatch.setattr(normalize.birdeye, "token_metadata", fake_metadata)
    monkeypatch.setattr(normalize.rba_fx, "usd_to_aud_rate", fake_fx)

    raw_tx = {
        "signature": "sig1",
        "timestamp": 1700000000,
        "fee": 5000,
        "events": {
            "swap": {
                "tokenInputs": [
                    {
                        "mint": "TOKENA",
                        "symbol": "TKA",
                        "decimals": 6,
                        "amount": "5",
                    }
                ],
                "tokenOutputs": [
                    {
                        "mint": USDC_MINT,
                        "symbol": "USDC",
                        "decimals": 6,
                        "amount": "10",
                    }
                ],
            }
        },
        "tokenTransfers": [
            {
                "mint": "TOKENB",
                "tokenAmount": "2.5",
                "tokenDecimals": None,
                "tokenSymbol": None,
                "fromUserAccount": "OTHER",
                "toUserAccount": "WALLET",
            }
        ],
        "nativeTransfers": [
            {
                "fromUserAccount": "WALLET",
                "toUserAccount": "OTHER",
                "amount": 1000000000,
            }
        ],
    }

    events = asyncio.run(normalize.normalize_wallet_events("WALLET", [raw_tx]))
    assert len(events) == 3

    swap_event = next(ev for ev in events if ev.kind == "swap")
    assert swap_event.kind == "swap"
    assert swap_event.base_token is not None
    assert swap_event.quote_token is not None
    assert swap_event.base_token.amount == Decimal("5")
    assert swap_event.quote_token.amount == Decimal("10")
    assert Decimal(str(swap_event.raw.get("proceeds_aud"))) == Decimal("15.00")

    transfer_in = next(ev for ev in events if ev.kind == "transfer_in")
    assert transfer_in.quote_token is not None
    assert transfer_in.quote_token.symbol == "TKB"
    assert transfer_in.quote_token.amount == Decimal("2.5")

    transfer_out = next(ev for ev in events if ev.kind == "transfer_out")
    assert transfer_out.base_token is not None
    assert transfer_out.base_token.symbol == "SOL"
    assert transfer_out.base_token.amount == Decimal("1")
