from __future__ import annotations

import asyncio
import datetime
from decimal import Decimal

import httpx

from sol_cgt import utils
from sol_cgt.providers import birdeye


class DummyAsyncClient:
    def __init__(self, *, timeout: float, response: httpx.Response) -> None:
        self._response = response
        self.calls = 0

    async def __aenter__(self) -> "DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, params: dict, headers: dict) -> httpx.Response:
        self.calls += 1
        return self._response


class SequenceAsyncClient:
    def __init__(self, *, timeout: float, responses: list[httpx.Response]) -> None:
        self._responses = responses
        self.calls = 0

    async def __aenter__(self) -> "SequenceAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, params: dict, headers: dict) -> httpx.Response:
        self.calls += 1
        if not self._responses:
            raise RuntimeError("No more responses")
        return self._responses.pop(0)


def test_birdeye_metadata_parsing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(utils, "CACHE_ROOT", tmp_path)
    request = httpx.Request("GET", "https://public-api.birdeye.so/defi/v3/token/meta-data/single")
    response = httpx.Response(200, json={"data": {"symbol": "ABC", "decimals": 6}}, request=request)

    def factory(*, timeout: float) -> DummyAsyncClient:
        return DummyAsyncClient(timeout=timeout, response=response)

    monkeypatch.setattr(birdeye.httpx, "AsyncClient", factory)
    symbol, decimals = asyncio.run(birdeye.token_metadata("MINT", api_key="key"))

    assert symbol == "ABC"
    assert decimals == 6


def test_birdeye_metadata_404_returns_none(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(utils, "CACHE_ROOT", tmp_path)
    request = httpx.Request("GET", "https://public-api.birdeye.so/defi/v3/token/meta-data/single")
    response = httpx.Response(404, json={"error": "not found"}, request=request)

    def factory(*, timeout: float) -> DummyAsyncClient:
        return DummyAsyncClient(timeout=timeout, response=response)

    monkeypatch.setattr(birdeye.httpx, "AsyncClient", factory)
    symbol, decimals = asyncio.run(birdeye.token_metadata("MINT", api_key="key"))

    assert symbol is None
    assert decimals is None


def test_birdeye_429_retries_and_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(utils, "CACHE_ROOT", tmp_path)
    birdeye._PRICE_CACHE = birdeye.PriceHistoryCache(tmp_path / "birdeye_hist_unix.jsonl")

    request = httpx.Request("GET", "https://public-api.birdeye.so/defi/historical_price_unix")
    response_429 = httpx.Response(429, headers={"Retry-After": "0"}, request=request)
    response_ok = httpx.Response(200, json={"data": {"value": 1.23}}, request=request)
    responses = [response_429, response_ok]
    client_holder = {}

    def factory(*, timeout: float) -> SequenceAsyncClient:
        client = SequenceAsyncClient(timeout=timeout, responses=list(responses))
        client_holder["client"] = client
        return client

    async def no_sleep(*args, **kwargs):
        return None

    async def no_wait():
        return None

    monkeypatch.setattr(birdeye.httpx, "AsyncClient", factory)
    monkeypatch.setattr(birdeye.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(birdeye._RATE_LIMITER, "acquire", no_wait)

    ts = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    price = asyncio.run(birdeye.historical_price_usd("MINT", ts, api_key="key"))
    assert price == Decimal("1.23")
    assert client_holder["client"].calls == 2

    price_cached = asyncio.run(birdeye.historical_price_usd("MINT", ts, api_key="key"))
    assert price_cached == Decimal("1.23")
