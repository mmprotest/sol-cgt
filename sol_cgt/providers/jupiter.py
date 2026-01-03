"""Jupiter price API client with caching."""
from __future__ import annotations

import base64
import logging
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from .. import utils

LOGGER = logging.getLogger(__name__)

JUPITER_PRICE_V2_URL = "https://api.jup.ag/price/v2"
JUPITER_PRICE_V3_URL = "https://api.jup.ag/price/v3"
JUPITER_TOKENS_V1_URL = "https://lite-api.jup.ag/tokens/v1/token"
JUPITER_TOKENS_V2_URL = "https://api.jup.ag/tokens/v2/search"
DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"


class PriceLookupError(RuntimeError):
    def __init__(self, mint: str, ts: datetime, message: str = "Price lookup failed") -> None:
        super().__init__(f"{message} for mint={mint} at {ts.isoformat()}")
        self.mint = mint
        self.ts = ts


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "jupiter") / f"{key}.json"


def _price_base_url(api_key: Optional[str]) -> str:
    override = os.getenv("JUPITER_PRICE_URL")
    if override:
        return override
    if api_key:
        return JUPITER_PRICE_V3_URL
    return JUPITER_PRICE_V2_URL


async def _cached_get(params: dict[str, Any], *, base_url: str, api_key: Optional[str]) -> dict[str, Any]:
    cache_key = utils.sha1_digest(f"{base_url}|{params}")
    cache_file = _cache_path(cache_key)
    if cache_file.exists():
        return utils.json_loads(cache_file.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            payload = await _perform_request(client, params, base_url=base_url, api_key=api_key)
        except RetryError as exc:  # pragma: no cover
            raise exc.last_attempt.exception() if exc.last_attempt else exc
    cache_file.write_text(utils.json_dumps(payload), encoding="utf-8")
    return payload


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
async def _perform_request(
    client: httpx.AsyncClient,
    params: dict[str, Any],
    *,
    base_url: str,
    api_key: Optional[str],
) -> dict[str, Any]:
    headers = {"accept": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key
    try:
        response = await client.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
        raise PriceLookupError("unknown", datetime.utcnow(), message=str(exc)) from exc


async def price_usd(mint: str, ts: datetime, *, api_key: Optional[str] = None) -> Optional[Decimal]:
    params = {"ids": mint}
    api_key = api_key or os.getenv("JUP_API_KEY")
    base_url = _price_base_url(api_key)
    data = await _cached_get(params, base_url=base_url, api_key=api_key)
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


def _rpc_url() -> str:
    override = os.getenv("HELIUS_RPC_URL")
    if override:
        return utils.validate_helius_rpc_url(override) or override
    return DEFAULT_RPC_URL


def _parse_mint_decimals(raw_data: str) -> Optional[int]:
    try:
        decoded = base64.b64decode(raw_data)
    except Exception:
        return None
    if len(decoded) <= 44:
        return None
    return int(decoded[44])


async def _rpc_mint_decimals(mint: str) -> Optional[int]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [mint, {"encoding": "base64"}],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(_rpc_url(), json=payload)
        response.raise_for_status()
        data = response.json()
    value = data.get("result", {}).get("value") if isinstance(data, dict) else None
    if not isinstance(value, dict):
        return None
    encoded = value.get("data")
    if isinstance(encoded, list) and encoded:
        encoded = encoded[0]
    if not isinstance(encoded, str):
        return None
    return _parse_mint_decimals(encoded)


async def _jupiter_token_v2(mint: str, api_key: str) -> tuple[Optional[str], Optional[int]]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            JUPITER_TOKENS_V2_URL,
            params={"query": mint},
            headers={"accept": "application/json", "x-api-key": api_key},
        )
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, list):
        return None, None
    for item in payload:
        if not isinstance(item, dict):
            continue
        if item.get("address") != mint:
            continue
        symbol = item.get("symbol")
        decimals = item.get("decimals")
        return symbol, int(decimals) if decimals is not None else None
    return None, None


async def _jupiter_token_v1(mint: str) -> tuple[Optional[str], Optional[int]]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(
            f"{JUPITER_TOKENS_V1_URL}/{mint}",
            headers={"accept": "application/json"},
        )
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict):
        return None, None
    symbol = payload.get("symbol")
    decimals = payload.get("decimals")
    return symbol, int(decimals) if decimals is not None else None


async def token_metadata(mint: str) -> tuple[Optional[str], Optional[int]]:
    try:
        decimals = await _rpc_mint_decimals(mint)
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
        LOGGER.warning("Jupiter RPC metadata lookup failed for mint=%s: %s", mint, exc)
        decimals = None
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Jupiter RPC metadata lookup failed for mint=%s: %s", mint, exc)
        decimals = None
    if decimals is not None:
        return None, decimals

    api_key = os.getenv("JUP_API_KEY")
    if api_key:
        try:
            symbol, decimals = await _jupiter_token_v2(mint, api_key)
            if symbol is not None or decimals is not None:
                return symbol, decimals
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            LOGGER.warning("Jupiter v2 token lookup failed for mint=%s: %s", mint, exc)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("Jupiter v2 token lookup failed for mint=%s: %s", mint, exc)

    try:
        return await _jupiter_token_v1(mint)
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
        LOGGER.warning("Jupiter v1 token lookup failed for mint=%s: %s", mint, exc)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Jupiter v1 token lookup failed for mint=%s: %s", mint, exc)
    return None, None
