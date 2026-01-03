"""Birdeye price and metadata lookup with caching."""
from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Tuple

import httpx
from tenacity import RetryError, retry, retry_if_exception, stop_after_attempt, wait_exponential

from .. import utils

API_URL = "https://public-api.birdeye.so"


class PriceLookupError(RuntimeError):
    def __init__(self, mint: str, ts: datetime, message: str = "Price lookup failed") -> None:
        super().__init__(f"{message} for mint={mint} at {ts.isoformat()}")
        self.mint = mint
        self.ts = ts


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "birdeye") / f"{key}.json"


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_should_retry),
)
async def _perform_request(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any]:
    response = await client.get(url, params=params, headers=headers)
    if response.status_code == 429 or response.status_code >= 500:
        raise httpx.HTTPStatusError("Retryable Birdeye error", request=response.request, response=response)
    response.raise_for_status()
    return response.json()


async def _cached_request(url: str, params: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
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


def _headers(api_key: str) -> dict[str, str]:
    return {
        "X-API-KEY": api_key,
        "x-chain": "solana",
    }


def _extract_price(payload: dict[str, Any]) -> Optional[Decimal]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        for key in ("value", "price", "v"):
            if key in data and data[key] is not None:
                return Decimal(str(data[key]))
        if "items" in data and isinstance(data["items"], list) and data["items"]:
            item = data["items"][0]
            if isinstance(item, dict):
                for key in ("value", "price"):
                    if key in item and item[key] is not None:
                        return Decimal(str(item[key]))
    if isinstance(payload, dict):
        for key in ("value", "price"):
            if key in payload and payload[key] is not None:
                return Decimal(str(payload[key]))
    return None


async def historical_price_usd(mint: str, ts: datetime, *, api_key: Optional[str] = None) -> Decimal:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        raise RuntimeError("BIRDEYE_API_KEY is required for price lookups")
    url = f"{API_URL}/defi/historical_price_unix"
    headers = _headers(api_key)
    unix_ts = int(ts.timestamp())
    param_variants = [
        {"address": mint, "unixtime": unix_ts},
        {"address": mint, "timestamp": unix_ts},
        {"address": mint, "time": unix_ts},
    ]
    last_payload: Optional[dict[str, Any]] = None
    for params in param_variants:
        payload = await _cached_request(url, params, headers)
        last_payload = payload
        price = _extract_price(payload)
        if price is not None:
            return price
    raise PriceLookupError(mint, ts, message=f"No price in response: {last_payload}")


async def token_metadata(mint: str, *, api_key: Optional[str] = None) -> Tuple[Optional[str], Optional[int]]:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        return None, None
    url = f"{API_URL}/defi/token_data"
    params = {"address": mint}
    payload = await _cached_request(url, params, _headers(api_key))
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None, None
    symbol = data.get("symbol")
    decimals = data.get("decimals")
    if decimals is None:
        return symbol, None
    return symbol, int(decimals)
