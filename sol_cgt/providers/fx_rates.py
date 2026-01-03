"""USD/AUD FX rates via frankfurter.app with caching."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from .. import utils

BASE_URL = "https://api.frankfurter.app"


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "fx") / f"{key}.json"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def _perform_request(client: httpx.AsyncClient, url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = await client.get(url, params=params, headers={"accept": "application/json"})
    response.raise_for_status()
    return response.json()


async def usd_to_aud_rate(day: date) -> Decimal:
    cache_key = utils.sha1_digest(f"{day.isoformat()}")
    cache_file = _cache_path(cache_key)
    if cache_file.exists():
        payload = utils.json_loads(cache_file.read_text(encoding="utf-8"))
        rate = payload.get("rate") if isinstance(payload, dict) else None
        if rate is not None:
            return Decimal(str(rate))
    url = f"{BASE_URL}/{day.isoformat()}"
    params = {"from": "USD", "to": "AUD"}
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            data = await _perform_request(client, url, params)
        except RetryError as exc:  # pragma: no cover
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    rates = data.get("rates") if isinstance(data, dict) else None
    if not isinstance(rates, dict) or "AUD" not in rates:
        raise RuntimeError("AUD rate missing from FX response")
    rate = Decimal(str(rates["AUD"]))
    cache_file.write_text(utils.json_dumps({"rate": str(rate)}), encoding="utf-8")
    return rate
