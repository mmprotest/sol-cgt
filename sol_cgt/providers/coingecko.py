"""Coingecko price and FX helper."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import os
from pathlib import Path
from typing import Any, Optional

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from .. import utils

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoError(RuntimeError):
    pass


class ProviderUnavailable(RuntimeError):
    pass


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "coingecko") / f"{key}.json"


def _api_key_header(api_key: str) -> dict[str, str]:
    return {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key,
        "x-cg-pro-api-key": api_key,
    }


def _resolve_api_key(api_key: Optional[str]) -> str:
    resolved = api_key or os.getenv("COINGECKO_API_KEY")
    if not resolved:
        raise ProviderUnavailable("CoinGecko API key is required")
    return resolved


async def _cached_get(path: str, params: dict[str, Any], *, api_key: Optional[str]) -> dict[str, Any]:
    api_key_value = _resolve_api_key(api_key)
    cache_key = utils.sha1_digest(f"{path}|{params}")
    cache_file = _cache_path(cache_key)
    if cache_file.exists():
        return utils.json_loads(cache_file.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=25.0) as client:
        try:
            data = await _perform_request(client, path, params, api_key=api_key_value)
        except RetryError as exc:  # pragma: no cover
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    cache_file.write_text(utils.json_dumps(data), encoding="utf-8")
    return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def _perform_request(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any],
    *,
    api_key: str,
) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    response = await client.get(url, params=params, headers=_api_key_header(api_key))
    response.raise_for_status()
    return response.json()


async def price_at(symbol: str, ts: datetime, *, api_key: Optional[str] = None) -> Optional[Decimal]:
    path = f"/coins/{symbol.lower()}/history"
    params = {"date": ts.strftime("%d-%m-%Y"), "localization": "false"}
    data = await _cached_get(path, params, api_key=api_key)
    market_data = data.get("market_data")
    if not market_data:
        return None
    price = market_data.get("current_price", {}).get("usd")
    if price is None:
        return None
    return Decimal(str(price))


async def sol_price_usd(ts: datetime, *, api_key: Optional[str] = None) -> Optional[Decimal]:
    return await price_at("solana", ts, api_key=api_key)


async def fx_aud_rate(ts: date, *, api_key: Optional[str] = None) -> Decimal:
    path = "/exchange_rates"
    params: dict[str, Any] = {}
    data = await _cached_get(path, params, api_key=api_key)
    rates = data.get("rates") or {}
    aud = rates.get("aud")
    usd = rates.get("usd")
    if not aud or not usd:
        raise CoinGeckoError("AUD or USD rate missing")
    aud_value = Decimal(str(aud.get("value")))
    usd_value = Decimal(str(usd.get("value")))
    if usd_value == 0:
        raise CoinGeckoError("USD rate zero")
    return aud_value / usd_value
