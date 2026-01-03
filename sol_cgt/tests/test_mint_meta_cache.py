from __future__ import annotations

import asyncio

from sol_cgt.ingestion import normalize
from sol_cgt.meta.mints import MintMetaCache


def test_mint_meta_cache_load_save_with_none(tmp_path) -> None:
    cache = MintMetaCache()
    cache.set_decimals("MINTA", 6)
    cache.set_decimals("MINTB", None)
    path = tmp_path / "mint_meta.json"
    cache.save(path)

    loaded = MintMetaCache.load(path)
    assert loaded.get_decimals("MINTA") == 6
    assert loaded.get_decimals("MINTB") is None


def test_normalize_prefetch_batches_once(monkeypatch, tmp_path) -> None:
    calls = {"count": 0}

    async def fake_batch(mints: list[str], rpc_url: str):
        calls["count"] += 1
        return {mint: 6 for mint in mints}

    monkeypatch.setattr(normalize.solana_rpc, "get_mint_decimals_batch", fake_batch)

    raw_txs = [
        {
            "signature": "sig1",
            "timestamp": 1700000000,
            "tokenTransfers": [
                {
                    "mint": "MINT1",
                    "tokenAmount": "1",
                    "tokenDecimals": None,
                    "tokenSymbol": None,
                    "fromUserAccount": "A",
                    "toUserAccount": "WALLET",
                }
            ],
        },
        {
            "signature": "sig2",
            "timestamp": 1700000001,
            "tokenTransfers": [
                {
                    "mint": "MINT2",
                    "tokenAmount": "2",
                    "tokenDecimals": None,
                    "tokenSymbol": None,
                    "fromUserAccount": "A",
                    "toUserAccount": "WALLET",
                }
            ],
        },
    ]

    asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            raw_txs,
            mint_cache_path=tmp_path / "mint_meta.json",
        )
    )

    assert calls["count"] == 1


def test_normalize_skips_rpc_when_cache_warm(monkeypatch, tmp_path) -> None:
    cache = MintMetaCache()
    cache.set_decimals("MINTC", 9)
    cache_path = tmp_path / "mint_meta.json"
    cache.save(cache_path)

    async def fake_batch(mints: list[str], rpc_url: str):
        raise AssertionError("RPC should not be called when cache is warm")

    monkeypatch.setattr(normalize.solana_rpc, "get_mint_decimals_batch", fake_batch)

    raw_txs = [
        {
            "signature": "sig3",
            "timestamp": 1700000002,
            "tokenTransfers": [
                {
                    "mint": "MINTC",
                    "tokenAmount": "3",
                    "tokenDecimals": None,
                    "tokenSymbol": None,
                    "fromUserAccount": "A",
                    "toUserAccount": "WALLET",
                }
            ],
        }
    ]

    events = asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            raw_txs,
            mint_cache_path=cache_path,
        )
    )

    assert events[0].quote_token is not None
    assert events[0].quote_token.decimals == 9
