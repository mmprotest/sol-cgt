from __future__ import annotations

import asyncio
from decimal import Decimal

from sol_cgt.ingestion import normalize


def test_routed_swap_canonicalization(monkeypatch) -> None:
    async def fake_metadata(mint: str):
        return (mint[:3], 6)

    async def fake_jupiter_metadata(mint: str):
        return (None, None)

    monkeypatch.setattr(normalize.jupiter, "token_metadata", fake_jupiter_metadata)
    monkeypatch.setattr(normalize.birdeye, "token_metadata", fake_metadata)

    raw_tx = {
        "signature": "sigswap",
        "timestamp": 1700000000,
        "fee": 5000,
        "events": {
            "swap": {
                "tokenInputs": [
                    {"mint": "TOKENA", "decimals": 6, "amount": "5"},
                    {"mint": "TOKENB", "decimals": 6, "amount": "3"},
                ],
                "tokenOutputs": [
                    {"mint": "TOKENC", "decimals": 6, "amount": "7"},
                    {"mint": "TOKEND", "decimals": 6, "amount": "1"},
                ],
            }
        },
        "tokenTransfers": [
            {
                "mint": "TOKENA",
                "tokenAmount": "5",
                "tokenDecimals": 6,
                "tokenSymbol": "TKA",
                "fromUserAccount": "WALLET",
                "toUserAccount": "POOL",
            },
            {
                "mint": "TOKENB",
                "tokenAmount": "3",
                "tokenDecimals": 6,
                "tokenSymbol": "TKB",
                "fromUserAccount": "WALLET",
                "toUserAccount": "POOL",
            },
            {
                "mint": "TOKENC",
                "tokenAmount": "7",
                "tokenDecimals": 6,
                "tokenSymbol": "TKC",
                "fromUserAccount": "POOL",
                "toUserAccount": "WALLET",
            },
            {
                "mint": "TOKEND",
                "tokenAmount": "1",
                "tokenDecimals": 6,
                "tokenSymbol": "TKD",
                "fromUserAccount": "POOL",
                "toUserAccount": "WALLET",
            },
        ],
    }

    events = asyncio.run(normalize.normalize_wallet_events("WALLET", [raw_tx]))
    assert all(ev.kind == "swap" for ev in events)
    assert len(events) == 4
    totals = {}
    for ev in events:
        token = ev.base_token or ev.quote_token
        assert token is not None
        totals[token.mint] = totals.get(token.mint, Decimal("0")) + (
            -token.amount if ev.base_token is not None else token.amount
        )
    assert totals == {
        "TOKENA": Decimal("-5"),
        "TOKENB": Decimal("-3"),
        "TOKENC": Decimal("7"),
        "TOKEND": Decimal("1"),
    }
