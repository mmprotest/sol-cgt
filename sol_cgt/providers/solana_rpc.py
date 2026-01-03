"""Solana JSON-RPC helpers."""
from __future__ import annotations

import asyncio
import base64
import logging
from typing import Optional

import httpx
from tenacity import RetryError, retry, retry_if_exception, stop_after_attempt, wait_exponential

from .. import utils


LOGGER = logging.getLogger(__name__)
MINT_DECIMALS_OFFSET = 44
BATCH_SIZE = 100
MAX_CONCURRENCY = 8


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    if isinstance(exc, httpx.TransportError):
        return True
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_should_retry),
)
async def _perform_request(client: httpx.AsyncClient, rpc_url: str, mints: list[str]) -> dict[str, Optional[int]]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [mints, {"encoding": "base64"}],
    }
    response = await client.post(rpc_url, json=payload)
    if response.status_code == 429 or response.status_code >= 500:
        raise httpx.HTTPStatusError("Retryable RPC error", request=response.request, response=response)
    response.raise_for_status()
    body = response.json()
    result = body.get("result", {}) if isinstance(body, dict) else {}
    values = result.get("value", []) if isinstance(result, dict) else []
    if not isinstance(values, list):
        return {mint: None for mint in mints}
    decimals_by_mint: dict[str, Optional[int]] = {}
    for idx, mint in enumerate(mints):
        entry = values[idx] if idx < len(values) else None
        if not isinstance(entry, dict):
            decimals_by_mint[mint] = None
            continue
        data = entry.get("data")
        if not isinstance(data, list) or not data:
            decimals_by_mint[mint] = None
            continue
        encoded = data[0]
        if not isinstance(encoded, str):
            decimals_by_mint[mint] = None
            continue
        try:
            raw = base64.b64decode(encoded)
        except Exception:
            decimals_by_mint[mint] = None
            continue
        if len(raw) <= MINT_DECIMALS_OFFSET:
            decimals_by_mint[mint] = None
            continue
        decimals_by_mint[mint] = raw[MINT_DECIMALS_OFFSET]
    return decimals_by_mint


async def _fetch_batch(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    rpc_url: str,
    mints: list[str],
) -> dict[str, Optional[int]]:
    async with semaphore:
        try:
            return await _perform_request(client, rpc_url, mints)
        except RetryError as exc:  # pragma: no cover - network failure path
            LOGGER.warning("RPC batch failed after retries: %s", exc)
            return {mint: None for mint in mints}


async def get_mint_decimals_batch(mints: list[str], rpc_url: str) -> dict[str, Optional[int]]:
    if not mints:
        return {}
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    results: dict[str, Optional[int]] = {}
    async with httpx.AsyncClient(timeout=30.0, http2=True) as client:
        tasks = []
        for chunk in utils.chunked(mints, BATCH_SIZE):
            tasks.append(_fetch_batch(client, semaphore, rpc_url, list(chunk)))
        for batch_result in await asyncio.gather(*tasks):
            results.update(batch_result)
    return results
