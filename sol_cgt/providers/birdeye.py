"""Minimal Birdeye price lookup client with caching."""
from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Tuple

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from .. import utils

API_URL = "https://public-api.birdeye.so"


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "birdeye") / f"{key}.json"


async def _cached_request(url: str, params: dict[str, Any], headers: dict[str, str]) -> Optional[dict[str, Any]]:
    cache_key = utils.sha1_digest(f"{url}|{params}")
    path = _cache_path(cache_key)
    if path.exists():
        return utils.json_loads(path.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            data = await _perform_request(client, url, params, headers)
        except RetryError as exc:  # pragma: no cover
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    path.write_text(utils.json_dumps(data), encoding="utf-8")
    return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def _perform_request(
    client: httpx.AsyncClient, url: str, params: dict[str, Any], headers: dict[str, str]
) -> dict[str, Any]:
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


async def price_at(mint: str, ts: datetime, *, api_key: Optional[str] = None) -> Optional[Decimal]:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        return None
    url = f"{API_URL}/defi/price"
    params = {"address": mint, "time": int(ts.timestamp())}
    headers = {"X-API-KEY": api_key}
    data = await _cached_request(url, params, headers)
    if not data:
        return None
    price = data.get("data", {}).get("value")
    if price is None:
        return None
    return Decimal(str(price))


async def symbol_decimals(mint: str, *, api_key: Optional[str] = None) -> Optional[Tuple[Optional[str], int]]:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        return None
    url = f"{API_URL}/defi/token_data"
    params = {"address": mint}
    headers = {"X-API-KEY": api_key}
    data = await _cached_request(url, params, headers)
    if not data:
        return None
    token = data.get("data") or {}
    symbol = token.get("symbol")
    decimals = token.get("decimals")
    if decimals is None:
        return None
    return symbol, int(decimals)
