from __future__ import annotations

import asyncio
from decimal import Decimal

import httpx

from sol_cgt.ingestion import normalize
from sol_cgt.pricing import USDC_MINT


def test_normalize_helius_enhanced(monkeypatch) -> None:
    async def fake_metadata(mint: str):
        if mint == "TOKENB":
            return ("TKB", 6)
        return (None, None)

    async def fake_jupiter_metadata(mint: str):
        return (None, None)

    monkeypatch.setattr(normalize.jupiter, "token_metadata", fake_jupiter_metadata)
    monkeypatch.setattr(normalize.birdeye, "token_metadata", fake_metadata)

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
    assert len(events) == 4

    swap_events = [ev for ev in events if ev.kind == "swap"]
    assert len(swap_events) == 2
    swap_out = next(ev for ev in swap_events if ev.base_token is not None)
    swap_in = next(ev for ev in swap_events if ev.quote_token is not None)
    assert swap_out.base_token is not None
    assert swap_in.quote_token is not None
    assert swap_out.base_token.amount == Decimal("5")
    assert swap_in.quote_token.amount == Decimal("10")

    transfer_in = next(ev for ev in events if ev.kind == "transfer_in")
    assert transfer_in.quote_token is not None
    assert transfer_in.quote_token.symbol == "TKB"
    assert transfer_in.quote_token.amount == Decimal("2.5")

    transfer_out = next(ev for ev in events if ev.kind == "transfer_out")
    assert transfer_out.base_token is not None
    assert transfer_out.base_token.symbol == "SOL"
    assert transfer_out.base_token.amount == Decimal("1")


def test_normalize_uses_jupiter_metadata_when_birdeye_fails(monkeypatch) -> None:
    async def fake_jupiter_metadata(mint: str):
        if mint == "TOKENC":
            return ("TKC", 8)
        return (None, None)

    async def failing_birdeye_metadata(mint: str):
        raise RuntimeError("Birdeye failure")

    monkeypatch.setattr(normalize.jupiter, "token_metadata", fake_jupiter_metadata)
    monkeypatch.setattr(normalize.birdeye, "token_metadata", failing_birdeye_metadata)

    raw_tx = {
        "signature": "sig2",
        "timestamp": 1700000001,
        "fee": 5000,
        "tokenTransfers": [
            {
                "mint": "TOKENC",
                "tokenAmount": "1.25",
                "tokenDecimals": None,
                "tokenSymbol": None,
                "fromUserAccount": "OTHER",
                "toUserAccount": "WALLET",
            }
        ],
    }

    events = asyncio.run(normalize.normalize_wallet_events("WALLET", [raw_tx]))
    transfer_in = next(ev for ev in events if ev.kind == "transfer_in")
    assert transfer_in.quote_token is not None
    assert transfer_in.quote_token.symbol == "TKC"
    assert transfer_in.quote_token.decimals == 8


def test_normalize_metadata_failures_do_not_crash(monkeypatch) -> None:
    async def failing_jupiter_metadata(mint: str):
        raise httpx.ConnectError("down", request=httpx.Request("GET", "https://rpc"))

    async def failing_birdeye_metadata(mint: str):
        raise httpx.ConnectError("down", request=httpx.Request("GET", "https://birdeye"))

    monkeypatch.setattr(normalize.jupiter, "token_metadata", failing_jupiter_metadata)
    monkeypatch.setattr(normalize.birdeye, "token_metadata", failing_birdeye_metadata)

    raw_tx = {
        "signature": "sig3",
        "timestamp": 1700000002,
        "fee": 5000,
        "tokenTransfers": [
            {
                "mint": "TOKEND",
                "tokenAmount": "3.5",
                "tokenDecimals": None,
                "tokenSymbol": None,
                "fromUserAccount": "OTHER",
                "toUserAccount": "WALLET",
            }
        ],
    }

    events = asyncio.run(normalize.normalize_wallet_events("WALLET", [raw_tx]))
    transfer_in = next(ev for ev in events if ev.kind == "transfer_in")
    assert transfer_in.quote_token is not None
    assert transfer_in.quote_token.decimals == 0
    assert transfer_in.raw["decimals_defaulted_mints"] == ["TOKEND"]
