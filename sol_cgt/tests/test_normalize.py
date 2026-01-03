from __future__ import annotations

import asyncio
from decimal import Decimal

from sol_cgt.ingestion import normalize
from sol_cgt.pricing import USDC_MINT


def test_normalize_helius_enhanced(monkeypatch, tmp_path) -> None:
    async def fake_batch(mints: list[str], rpc_url: str):
        return {mint: 6 if mint == "TOKENB" else None for mint in mints}

    monkeypatch.setattr(normalize.solana_rpc, "get_mint_decimals_batch", fake_batch)

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

    events = asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            [raw_tx],
            mint_cache_path=tmp_path / "mint_meta.json",
        )
    )
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
    assert transfer_in.quote_token.symbol is None
    assert transfer_in.quote_token.amount == Decimal("2.5")

    transfer_out = next(ev for ev in events if ev.kind == "transfer_out")
    assert transfer_out.base_token is not None
    assert transfer_out.base_token.symbol == "SOL"
    assert transfer_out.base_token.amount == Decimal("1")


def test_normalize_prefers_prefetched_decimals(monkeypatch, tmp_path) -> None:
    async def fake_batch(mints: list[str], rpc_url: str):
        return {mint: 8 if mint == "TOKENC" else None for mint in mints}

    monkeypatch.setattr(normalize.solana_rpc, "get_mint_decimals_batch", fake_batch)

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

    events = asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            [raw_tx],
            mint_cache_path=tmp_path / "mint_meta.json",
        )
    )
    transfer_in = next(ev for ev in events if ev.kind == "transfer_in")
    assert transfer_in.quote_token is not None
    assert transfer_in.quote_token.symbol is None
    assert transfer_in.quote_token.decimals == 8


def test_normalize_metadata_failures_do_not_crash(monkeypatch, tmp_path) -> None:
    async def fake_batch(mints: list[str], rpc_url: str):
        return {mint: None for mint in mints}

    monkeypatch.setattr(normalize.solana_rpc, "get_mint_decimals_batch", fake_batch)

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

    events = asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            [raw_tx],
            mint_cache_path=tmp_path / "mint_meta.json",
        )
    )
    transfer_in = next(ev for ev in events if ev.kind == "transfer_in")
    assert transfer_in.quote_token is not None
    assert transfer_in.quote_token.decimals == 0
    assert transfer_in.raw["decimals_defaulted_mints"] == ["TOKEND"]
