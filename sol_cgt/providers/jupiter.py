"""Jupiter price API client with caching."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from .. import utils

BASE_URL = "https://price.jup.ag/v6/price"


class PriceLookupError(RuntimeError):
    def __init__(self, mint: str, ts: datetime, message: str = "Price lookup failed") -> None:
        super().__init__(f"{message} for mint={mint} at {ts.isoformat()}")
        self.mint = mint
        self.ts = ts


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "jupiter") / f"{key}.json"


async def _cached_get(params: dict[str, Any]) -> dict[str, Any]:
    cache_key = utils.sha1_digest(f"{BASE_URL}|{params}")
    cache_file = _cache_path(cache_key)
    if cache_file.exists():
        return utils.json_loads(cache_file.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            payload = await _perform_request(client, params)
        except RetryError as exc:  # pragma: no cover
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    cache_file.write_text(utils.json_dumps(payload), encoding="utf-8")
    return payload


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def _perform_request(client: httpx.AsyncClient, params: dict[str, Any]) -> dict[str, Any]:
    response = await client.get(BASE_URL, params=params, headers={"accept": "application/json"})
    response.raise_for_status()
    return response.json()


async def price_usd(mint: str, ts: datetime) -> Optional[Decimal]:
    params = {"ids": mint}
    data = await _cached_get(params)
    if not isinstance(data, dict):
        raise PriceLookupError(mint, ts, message="Invalid Jupiter response")
    payload = data.get("data") or {}
    if isinstance(payload, dict) and mint in payload:
        item = payload[mint]
        if isinstance(item, dict):
            price = item.get("price")
            if price is not None:
                return Decimal(str(price))
    raise PriceLookupError(mint, ts, message="No price found in Jupiter response")
