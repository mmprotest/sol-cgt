"""Coingecko price and FX helper."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from .. import utils

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoError(RuntimeError):
    pass


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "coingecko") / f"{key}.json"


async def _cached_get(path: str, params: dict[str, Any]) -> dict[str, Any]:
    cache_key = utils.sha1_digest(f"{path}|{params}")
    cache_file = _cache_path(cache_key)
    if cache_file.exists():
        return utils.json_loads(cache_file.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=25.0) as client:
        try:
            data = await _perform_request(client, path, params)
        except RetryError as exc:  # pragma: no cover
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    cache_file.write_text(utils.json_dumps(data), encoding="utf-8")
    return data


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def _perform_request(client: httpx.AsyncClient, path: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    response = await client.get(url, params=params, headers={"accept": "application/json"})
    response.raise_for_status()
    return response.json()


async def price_at(symbol: str, ts: datetime) -> Optional[Decimal]:
    path = f"/coins/{symbol.lower()}/history"
    params = {"date": ts.strftime("%d-%m-%Y"), "localization": "false"}
    data = await _cached_get(path, params)
    market_data = data.get("market_data")
    if not market_data:
        return None
    price = market_data.get("current_price", {}).get("usd")
    if price is None:
        return None
    return Decimal(str(price))


async def sol_price_usd(ts: datetime) -> Optional[Decimal]:
    return await price_at("solana", ts)


async def fx_aud_rate(ts: date) -> Decimal:
    path = "/exchange_rates"
    params: dict[str, Any] = {}
    data = await _cached_get(path, params)
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
