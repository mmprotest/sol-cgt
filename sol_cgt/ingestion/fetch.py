"""Fetch raw transactions and persist to cache."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Iterable, List, Optional

from .. import utils
from ..providers import helius

RAW_CACHE_DIR = utils.ensure_cache_dir("raw")


def _wallet_cache_path(wallet: str) -> Path:
    return RAW_CACHE_DIR / f"{wallet}.jsonl"


async def fetch_wallet(wallet: str, *, before: Optional[str] = None, limit: int = 1000) -> list[dict]:
    txs = await helius.fetch_txs(wallet, before=before, limit=limit)
    if not txs:
        return []
    path = _wallet_cache_path(wallet)
    utils.write_jsonl(path, txs)
    return txs


async def fetch_many(wallets: Iterable[str], *, before: Optional[str] = None, limit: int = 1000) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {}
    async def _fetch(wallet: str) -> None:
        results[wallet] = await fetch_wallet(wallet, before=before, limit=limit)

    await asyncio.gather(*[_fetch(wallet) for wallet in wallets])
    return results


def load_cached(wallet: str) -> list[dict]:
    path = _wallet_cache_path(wallet)
    return list(utils.read_jsonl(path))
