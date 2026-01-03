from __future__ import annotations

import asyncio

import httpx

from sol_cgt import utils
from sol_cgt.providers import birdeye


class DummyAsyncClient:
    def __init__(self, *, timeout: float, response: httpx.Response) -> None:
        self._response = response

    async def __aenter__(self) -> "DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, params: dict, headers: dict) -> httpx.Response:
        return self._response


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
