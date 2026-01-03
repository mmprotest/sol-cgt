"""Fetch raw transactions and persist to cache."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Iterable, List, Optional

from .. import utils
from ..providers import helius

RAW_CACHE_DIR = utils.ensure_cache_dir("raw")

logger = logging.getLogger(__name__)


def _wallet_cache_path(wallet: str) -> Path:
    return RAW_CACHE_DIR / f"{wallet}.jsonl"


def wallet_cache_path(wallet: str) -> Path:
    return _wallet_cache_path(wallet)


async def fetch_wallet(
    wallet: str,
    *,
    before_signature: Optional[str] = None,
    after_signature: Optional[str] = None,
    limit: int = 100,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    gte_time: Optional[int] = None,
    lte_time: Optional[int] = None,
    max_pages: int = 2000,
    append: bool = False,
) -> list[dict]:
    all_txs: list[dict] = []
    cursor = before_signature
    path = _wallet_cache_path(wallet)
    for page_idx in range(max_pages):
        page = await helius.fetch_txs(
            wallet,
            before_signature=cursor,
            after_signature=after_signature,
            sort_order="desc",
            gte_time=gte_time,
            lte_time=lte_time,
            limit=limit,
            api_key=api_key,
            base_url=base_url,
        )
        if not page:
            break
        all_txs.extend(page)
        mode = "a" if append or page_idx > 0 else "w"
        utils.write_jsonl(path, page, mode=mode)
        last_entry = page[-1]
        cursor = last_entry.get("signature") or last_entry.get("id")
        if gte_time is not None:
            timestamp = last_entry.get("timestamp")
            if isinstance(timestamp, int) and timestamp < gte_time:
                break
        if not cursor:
            break
    else:
        logger.warning("Fetch pagination stopped after max_pages=%s wallet=%s", max_pages, wallet)
    return all_txs


async def fetch_many(
    wallets: Iterable[str],
    *,
    before_signature: Optional[str] = None,
    after_signature: Optional[str] = None,
    limit: int = 100,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    gte_time: Optional[int] = None,
    lte_time: Optional[int] = None,
    max_pages: int = 2000,
    append: bool = False,
) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {}
    async def _fetch(wallet: str) -> None:
        results[wallet] = await fetch_wallet(
            wallet,
            before_signature=before_signature,
            after_signature=after_signature,
            limit=limit,
            api_key=api_key,
            base_url=base_url,
            gte_time=gte_time,
            lte_time=lte_time,
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


def cache_has_data(wallet: str) -> bool:
    path = _wallet_cache_path(wallet)
    for _ in utils.read_jsonl(path):
        return True
    return False
