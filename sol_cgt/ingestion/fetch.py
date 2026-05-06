"""Fetch raw transactions and persist to cache."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import httpx
from .. import utils
from ..providers import helius

RAW_CACHE_DIR = utils.ensure_cache_dir("raw")

logger = logging.getLogger(__name__)


@dataclass
class FetchStats:
    wallet: str
    provider: str
    token_accounts_mode: str
    pages: int
    rows_returned: int
    unique_signatures: int
    duplicate_signatures: int
    earliest_timestamp: Optional[int]
    latest_timestamp: Optional[int]


@dataclass
class CacheCoverage:
    wallet: str
    cache_path: str
    has_cache: bool
    raw_tx_count: int
    cache_min_timestamp: Optional[int]
    cache_max_timestamp: Optional[int]
    requested_start: Optional[int]
    requested_end: Optional[int]
    covers_start: bool
    covers_end: bool
    coverage_complete: bool
    missing_ranges: list[dict[str, int | str]]
    malformed_rows: int = 0
    provider_checked_ranges: list[dict[str, int | str]] | None = None


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
    provider: str = "enhanced",
    token_accounts: str = "balanceChanged",
    rate_limit_rps: float = 2.0,
) -> list[dict]:
    all_txs: list[dict] = []
    cursor = None
    path = _wallet_cache_path(wallet)
    logger.info(
        "Fetching wallet=%s limit=%s max_pages=%s before=%s after=%s append=%s",
        wallet,
        limit,
        max_pages,
        before_signature,
        after_signature,
        append,
    )
    rows_returned = 0
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None
    if provider not in {"enhanced", "getTransactionsForAddress", "auto"}:
        raise RuntimeError(f"Unsupported Helius history provider: {provider}")
    selected_provider = "enhanced" if provider == "auto" else provider
    for page_idx in range(max_pages):
        if gte_time is None or lte_time is None:
            raise RuntimeError("fetch_wallet requires gte_time and lte_time")
        if selected_provider == "getTransactionsForAddress":
            try:
                response = await helius.fetch_wallet_transactions_for_period_v2(
                    wallet, gte_time, lte_time, token_accounts=token_accounts, status="succeeded", transaction_details="full",
                    sort_order="asc", limit=limit, api_key=api_key, rpc_url=base_url, pagination_token=cursor,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 403 and provider == "auto":
                    logger.warning("Helius getTransactionsForAddress unavailable (403) for wallet=%s, falling back to enhanced provider in auto mode", wallet)
                    selected_provider = "enhanced"
                    page = await helius.fetch_txs(
                        wallet, limit=limit, api_key=api_key, base_url=helius.DEFAULT_BASE_URL, gte_time=gte_time, lte_time=lte_time,
                        before_signature=cursor, sort_order="desc", token_accounts=token_accounts,
                    )
                    cursor = str(page[-1].get("signature")) if page and page[-1].get("signature") else None
                    response = None
                else:
                    raise RuntimeError("Helius getTransactionsForAddress is unavailable for this API key (HTTP 403).") from exc
            if response is not None:
                page = response.get("data", [])
                cursor = response.get("paginationToken")
        else:
            page = await helius.fetch_txs(
                wallet, limit=limit, api_key=api_key, base_url=helius.DEFAULT_BASE_URL, gte_time=gte_time, lte_time=lte_time,
                before_signature=cursor, sort_order="desc", token_accounts=token_accounts,
            )
            cursor = str(page[-1].get("signature")) if page and page[-1].get("signature") else None
        if not page:
            logger.info("No more transactions for wallet=%s after page=%s", wallet, page_idx + 1)
            break
        all_txs.extend(page)
        rows_returned += len(page)
        for item in page:
            ts = item.get("timestamp")
            if isinstance(ts, int):
                min_ts = ts if min_ts is None else min(min_ts, ts)
                max_ts = ts if max_ts is None else max(max_ts, ts)
        page_ts = [row.get("timestamp") for row in page if isinstance(row.get("timestamp"), int)]
        logger.info(
            "Fetched wallet=%s provider=%s page=%s rows=%s total=%s cursor=%s earliest_timestamp=%s latest_timestamp=%s",
            wallet,
            selected_provider,
            page_idx + 1,
            len(page),
            len(all_txs),
            cursor,
            min(page_ts) if page_ts else None,
            max(page_ts) if page_ts else None,
        )
        await asyncio.sleep(1.0 / max(rate_limit_rps, 0.1))
        if not cursor:
            break
    else:
        logger.warning("Fetch pagination stopped after max_pages=%s wallet=%s", max_pages, wallet)
    deduped: list[dict] = []
    seen: set[str] = set()
    for tx in all_txs:
        sig = tx.get("signature") or tx.get("id")
        if not sig:
            deduped.append(tx)
            continue
        if str(sig) in seen:
            continue
        seen.add(str(sig))
        deduped.append(tx)
    mode = "a" if append else "w"
    utils.write_jsonl(path, deduped, mode=mode)
    unique_signatures = len(seen)
    duplicate_signatures = max(0, rows_returned - unique_signatures)
    all_block_times = [row.get("blockTime") for row in deduped if isinstance(row.get("blockTime"), int)]
    logger.info(
        "Completed fetch wallet=%s provider=%s token_accounts_mode=%s pages=%s rows_returned=%s unique_signatures=%s duplicate_signatures=%s earliest_timestamp=%s latest_timestamp=%s cache_path=%s",
        wallet,
        selected_provider,
        token_accounts,
        page_idx + 1 if all_txs else 0,
        rows_returned,
        unique_signatures,
        duplicate_signatures,
        min_ts,
        max_ts,
        path,
    )
    return deduped


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
    provider: str = "enhanced",
    token_accounts: str = "balanceChanged",
    rate_limit_rps: float = 2.0,
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
            provider=provider,
            token_accounts=token_accounts,
            rate_limit_rps=rate_limit_rps,
        )
    for wallet in wallets:
        await _fetch(wallet)
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


def cache_time_bounds(wallet: str) -> tuple[Optional[int], Optional[int]]:
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None
    for entry in utils.read_jsonl(_wallet_cache_path(wallet)):
        ts = entry.get("timestamp")
        if not isinstance(ts, int):
            continue
        min_ts = ts if min_ts is None else min(min_ts, ts)
        max_ts = ts if max_ts is None else max(max_ts, ts)
    return min_ts, max_ts


def inspect_raw_cache_coverage(wallet: str, requested_start: Optional[int], requested_end: Optional[int]) -> CacheCoverage:
    path = _wallet_cache_path(wallet)
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None
    raw_tx_count = 0
    malformed_rows = 0
    seen_signatures: set[str] = set()
    has_cache = path.exists()
    for entry in utils.read_jsonl(path):
        if not isinstance(entry, dict):
            malformed_rows += 1
            continue
        signature = entry.get("signature") or entry.get("id")
        if isinstance(signature, str):
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
        raw_tx_count += 1
        ts = entry.get("timestamp")
        if not isinstance(ts, int):
            ts = entry.get("blockTime")
        if not isinstance(ts, int):
            malformed_rows += 1
            continue
        min_ts = ts if min_ts is None else min(min_ts, ts)
        max_ts = ts if max_ts is None else max(max_ts, ts)
    covers_start = requested_start is None or (min_ts is not None and min_ts <= requested_start)
    covers_end = requested_end is None or (max_ts is not None and max_ts >= requested_end)
    coverage_complete = bool(raw_tx_count) and covers_start and covers_end and malformed_rows == 0
    missing_ranges: list[dict[str, int | str]] = []
    if raw_tx_count == 0:
        if requested_start is not None and requested_end is not None:
            missing_ranges.append({"start": requested_start, "end": requested_end, "reason": "empty_cache"})
    else:
        if requested_start is not None and (min_ts is None or min_ts > requested_start):
            missing_ranges.append({"start": requested_start, "end": min_ts or requested_end or requested_start, "reason": "missing_start"})
        if requested_end is not None and (max_ts is None or max_ts < requested_end):
            missing_ranges.append({"start": max_ts or requested_start or requested_end, "end": requested_end, "reason": "missing_end"})
    if malformed_rows > 0 and requested_start is not None and requested_end is not None:
        coverage_complete = False
        if not missing_ranges:
            missing_ranges.append({"start": requested_start, "end": requested_end, "reason": "malformed_or_missing_timestamps"})
    return CacheCoverage(
        wallet=wallet,
        cache_path=str(path),
        has_cache=has_cache,
        raw_tx_count=raw_tx_count,
        cache_min_timestamp=min_ts,
        cache_max_timestamp=max_ts,
        requested_start=requested_start,
        requested_end=requested_end,
        covers_start=covers_start,
        covers_end=covers_end,
        coverage_complete=coverage_complete,
        missing_ranges=missing_ranges,
        malformed_rows=malformed_rows,
        provider_checked_ranges=[],
    )
