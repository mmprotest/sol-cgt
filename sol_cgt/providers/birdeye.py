"""Birdeye price and metadata lookup with caching."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple

import httpx

from .. import utils

API_URL = "https://public-api.birdeye.so"
METADATA_URL = f"{API_URL}/defi/v3/token/meta-data/single"
LOGGER = logging.getLogger(__name__)
_INFLIGHT: dict[str, asyncio.Task] = {}
_INFLIGHT_LOCK = asyncio.Lock()


class PriceLookupError(RuntimeError):
    def __init__(self, mint: str, ts: datetime, message: str = "Price lookup failed") -> None:
        super().__init__(f"{message} for mint={mint} at {ts.isoformat()}")
        self.mint = mint
        self.ts = ts


class ProviderUnavailable(RuntimeError):
    pass


class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float) -> None:
        self.capacity = max(1, capacity)
        self.refill_rate = max(0.1, refill_rate)
        self.tokens = float(self.capacity)
        self.last_refill = time.monotonic()

    def refill(self, now: float) -> None:
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def time_to_next(self) -> float:
        if self.tokens >= 1:
            return 0.0
        return (1 - self.tokens) / self.refill_rate


class RateLimiter:
    def __init__(self, *, rps: int, rpm: int) -> None:
        self.bucket = TokenBucket(capacity=rps, refill_rate=float(rps))
        self.rpm = max(1, rpm)
        self._lock = asyncio.Lock()
        self._recent: deque[float] = deque()

    async def acquire(self) -> None:
        while True:
            wait = 0.0
            async with self._lock:
                now = time.monotonic()
                self.bucket.refill(now)
                while self._recent and now - self._recent[0] > 60:
                    self._recent.popleft()
                rpm_wait = 0.0
                if len(self._recent) >= self.rpm:
                    rpm_wait = 60 - (now - self._recent[0])
                if self.bucket.tokens >= 1 and rpm_wait <= 0:
                    self.bucket.tokens -= 1
                    self._recent.append(now)
                    return
                wait = max(self.bucket.time_to_next(), rpm_wait)
            await asyncio.sleep(wait)


class PriceHistoryCache:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, dict[str, str]] | None = None
        self._lock = asyncio.Lock()

    def _load(self) -> dict[str, dict[str, str]]:
        if self._data is not None:
            return self._data
        if not self.path.exists():
            self._data = {}
            return self._data
        raw = utils.json_loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            self._data = {}
            return self._data
        self._data = {str(key): value for key, value in raw.items() if isinstance(value, dict)}
        return self._data

    async def get(self, key: str) -> Optional[Decimal]:
        async with self._lock:
            data = self._load()
            entry = data.get(key)
            if not entry:
                return None
            price = entry.get("price_usd")
            if price is None:
                return None
            return Decimal(str(price))

    async def set(self, key: str, price: Decimal, ts: int, source: str) -> None:
        async with self._lock:
            data = self._load()
            data[key] = {"price_usd": str(price), "ts": str(ts), "source": source}
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(utils.json_dumps(data), encoding="utf-8")


def _rate_limiter() -> RateLimiter:
    rps = int(os.getenv("BIRDEYE_RPS", "5"))
    rpm = int(os.getenv("BIRDEYE_RPM", "60"))
    return RateLimiter(rps=rps, rpm=rpm)


_RATE_LIMITER = _rate_limiter()
_PRICE_CACHE = PriceHistoryCache(utils.ensure_cache_dir("prices") / "birdeye_hist_unix.json")


def _cache_path(key: str) -> Path:
    return utils.ensure_cache_dir("providers", "birdeye") / f"{key}.json"


async def _perform_request(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    *,
    allow_not_found: bool = False,
    max_retries: int = 5,
) -> Optional[dict[str, Any]]:
    backoff = 1.0
    for attempt in range(max_retries):
        await _RATE_LIMITER.acquire()
        response = await client.get(url, params=params, headers=headers)
        if response.status_code in (401, 403):
            raise ProviderUnavailable("Birdeye API key missing or unauthorized")
        if response.status_code == 404 and allow_not_found:
            return None
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = backoff
            else:
                delay = backoff
                backoff = min(backoff * 2, 30)
            if attempt == max_retries - 1:
                response.raise_for_status()
            await asyncio.sleep(delay)
            continue
        if response.status_code >= 500:
            delay = backoff
            backoff = min(backoff * 2, 30)
            if attempt == max_retries - 1:
                response.raise_for_status()
            await asyncio.sleep(delay)
            continue
        response.raise_for_status()
        return response.json()
    raise httpx.HTTPStatusError("Retryable Birdeye error", request=response.request, response=response)


async def _cached_request(
    url: str,
    params: dict[str, Any],
    headers: dict[str, str],
    *,
    allow_not_found: bool = False,
) -> Optional[dict[str, Any]]:
    cache_key = utils.sha1_digest(f"{url}|{params}")
    path = _cache_path(cache_key)
    if path.exists():
        return utils.json_loads(path.read_text(encoding="utf-8"))
    async with httpx.AsyncClient(timeout=20.0) as client:
        data = await _perform_request(client, url, params, headers, allow_not_found=allow_not_found)
    if data is None:
        return None
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


def _extract_price_series(payload: dict[str, Any]) -> dict[int, Decimal]:
    results: dict[int, Decimal] = {}
    data = payload.get("data") if isinstance(payload, dict) else None
    items = None
    if isinstance(data, dict):
        items = data.get("items") or data.get("prices")
    elif isinstance(data, list):
        items = data
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            ts_value = item.get("unixTime") or item.get("time") or item.get("timestamp") or item.get("t")
            price_value = item.get("value") or item.get("price") or item.get("v")
            if ts_value is None or price_value is None:
                continue
            results[int(ts_value)] = Decimal(str(price_value))
    return results


def _minute_bucket(ts: datetime) -> int:
    return int(ts.timestamp()) // 60 * 60


async def _single_flight(key: str, coro):
    async with _INFLIGHT_LOCK:
        task = _INFLIGHT.get(key)
        if task is None:
            task = asyncio.create_task(coro)
            _INFLIGHT[key] = task
    try:
        return await task
    finally:
        async with _INFLIGHT_LOCK:
            if _INFLIGHT.get(key) is task:
                _INFLIGHT.pop(key, None)


async def historical_price_usd(mint: str, ts: datetime, *, api_key: Optional[str] = None) -> Decimal:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        raise ProviderUnavailable("BIRDEYE_API_KEY is required for price lookups")
    url = f"{API_URL}/defi/historical_price_unix"
    headers = _headers(api_key)
    unix_ts = int(ts.timestamp())
    bucket = _minute_bucket(ts)
    cache_key = f"{mint}:{bucket}"
    cached = await _PRICE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    param_variants = [
        {"address": mint, "unixtime": unix_ts},
        {"address": mint, "timestamp": unix_ts},
        {"address": mint, "time": unix_ts},
    ]

    async def _request() -> Decimal:
        last_payload: Optional[dict[str, Any]] = None
        for params in param_variants:
            payload = await _cached_request(url, params, headers)
            last_payload = payload
            price = _extract_price(payload)
            if price is not None:
                await _PRICE_CACHE.set(cache_key, price, bucket, "birdeye")
                return price
        raise PriceLookupError(mint, ts, message=f"No price in response: {last_payload}")

    return await _single_flight(cache_key, _request())


async def historical_price_usd_batch(
    mint: str,
    unix_times: Sequence[int],
    *,
    api_key: Optional[str] = None,
) -> dict[int, Decimal]:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        raise ProviderUnavailable("BIRDEYE_API_KEY is required for price lookups")
    if not unix_times:
        return {}
    url = f"{API_URL}/defi/historical_price_unix"
    headers = _headers(api_key)
    sorted_times = sorted(set(unix_times))
    cached: dict[int, Decimal] = {}
    missing: list[int] = []
    for ts_value in sorted_times:
        cache_key = f"{mint}:{ts_value}"
        cached_price = await _PRICE_CACHE.get(cache_key)
        if cached_price is not None:
            cached[ts_value] = cached_price
        else:
            missing.append(ts_value)
    if not missing:
        return cached
    joined = ",".join(str(ts) for ts in missing)
    param_variants = [
        {"address": mint, "timestamps": joined},
        {"address": mint, "timestamp": joined},
        {"address": mint, "time": joined},
        {"address": mint, "unixtime": joined},
    ]
    last_payload: Optional[dict[str, Any]] = None
    for params in param_variants:
        payload = await _cached_request(url, params, headers)
        last_payload = payload
        if not payload:
            continue
        series = _extract_price_series(payload)
        if series:
            for bucket, price in series.items():
                await _PRICE_CACHE.set(f"{mint}:{bucket}", price, bucket, "birdeye")
            return {**cached, **series}
        price = _extract_price(payload)
        if price is not None and len(unix_times) == 1:
            single_bucket = missing[0]
            await _PRICE_CACHE.set(f"{mint}:{single_bucket}", price, single_bucket, "birdeye")
            return {**cached, single_bucket: price}
    raise PriceLookupError(mint, datetime.utcnow(), message=f"No price series in response: {last_payload}")


async def token_metadata(mint: str, *, api_key: Optional[str] = None) -> Tuple[Optional[str], Optional[int]]:
    api_key = api_key or os.getenv("BIRDEYE_API_KEY")
    if not api_key:
        return None, None
    url = METADATA_URL
    params = {"address": mint}
    try:
        payload = await _cached_request(url, params, _headers(api_key), allow_not_found=True)
    except httpx.HTTPStatusError as exc:
        LOGGER.warning("Birdeye metadata lookup failed for mint=%s status=%s", mint, exc.response.status_code)
        return None, None
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Birdeye metadata lookup failed for mint=%s: %s", mint, exc)
        return None, None
    if payload is None:
        LOGGER.warning("Birdeye metadata not found for mint=%s", mint)
        return None, None
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None, None
    symbol = data.get("symbol")
    decimals = data.get("decimals")
    if decimals is None:
        return symbol, None
    return symbol, int(decimals)
