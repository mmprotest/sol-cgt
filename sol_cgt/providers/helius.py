"""Client for the Helius enhanced transaction API."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import httpx
from tenacity import RetryError, retry, retry_if_exception, stop_after_attempt, wait_exponential

from .. import utils

DEFAULT_BASE_URL = "https://api.helius.xyz"


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "helius") / f"{key}.json"


async def _read_cache(key: str) -> Optional[Any]:
    path = _cache_path(key)
    if not path.exists():
        return None
    data = path.read_text(encoding="utf-8")
    return utils.json_loads(data)


async def _write_cache(key: str, payload: Any) -> None:
    path = _cache_path(key)
    path.write_text(utils.json_dumps(payload), encoding="utf-8")


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
async def _perform_request(client: httpx.AsyncClient, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    response = await client.get(url, params=params)
    if response.status_code == 429 or response.status_code >= 500:
        raise httpx.HTTPStatusError("Retryable Helius error", request=response.request, response=response)
    response.raise_for_status()
    body = response.json()
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and "transactions" in body:
        txs = body["transactions"]
        if isinstance(txs, list):
            return txs
    raise ValueError("Unexpected payload from Helius")


async def fetch_txs(
    wallet: str,
    *,
    before: Optional[str] = None,
    limit: int = 1000,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Fetch a page of transactions for ``wallet`` using the Enhanced API."""

    api_key = api_key or os.getenv("HELIUS_API_KEY")
    if not api_key:
        raise RuntimeError("HELIUS_API_KEY is required to fetch transactions")
    base_url = base_url or os.getenv("HELIUS_BASE_URL", DEFAULT_BASE_URL)
    params = {"api-key": api_key, "limit": limit}
    if before:
        params["before"] = before
    url = f"{base_url}/v0/addresses/{wallet}/transactions"
    cache_key = utils.sha1_digest(f"{url}|{params}")
    cached = await _read_cache(cache_key)
    if cached is not None:
        return cached
    async with httpx.AsyncClient(timeout=30.0, http2=True) as client:
        try:
            payload = await _perform_request(client, url, params)
        except RetryError as exc:  # pragma: no cover - network failure path
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    await _write_cache(cache_key, payload)
    return payload
