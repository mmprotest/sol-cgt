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


async def fetch_wallet(
    wallet: str,
    *,
    before: Optional[str] = None,
    limit: int = 100,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    fy_start_ts: Optional[int] = None,
    max_pages: int = 100,
    append: bool = False,
) -> list[dict]:
    txs = await helius.fetch_txs(
        wallet,
        before=before,
        limit=limit,
        api_key=api_key,
        base_url=base_url,
        fy_start_ts=fy_start_ts,
        max_pages=max_pages,
    )
    if not txs:
        return []
    path = _wallet_cache_path(wallet)
    utils.write_jsonl(path, txs, mode="a" if append else "w")
    return txs


async def fetch_many(
    wallets: Iterable[str],
    *,
    before: Optional[str] = None,
    limit: int = 100,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    fy_start_ts: Optional[int] = None,
    max_pages: int = 100,
    append: bool = False,
) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {}
    async def _fetch(wallet: str) -> None:
        results[wallet] = await fetch_wallet(
            wallet,
            before=before,
            limit=limit,
            api_key=api_key,
            base_url=base_url,
            fy_start_ts=fy_start_ts,
            max_pages=max_pages,
            append=append,
        )

    await asyncio.gather(*[_fetch(wallet) for wallet in wallets])
    return results


def load_cached(wallet: str) -> list[dict]:
    path = _wallet_cache_path(wallet)
    items = []
    seen: set[str] = set()
    for entry in utils.read_jsonl(path):
        signature = entry.get("signature") or entry.get("id")
        if not signature:
            items.append(entry)
            continue
        if signature in seen:
            continue
        seen.add(signature)
        items.append(entry)
    return items
