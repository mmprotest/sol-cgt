from __future__ import annotations

import asyncio
from datetime import date
from decimal import Decimal

import httpx

from sol_cgt import utils
from sol_cgt.providers import kraken


class DummyAsyncClient:
    def __init__(self, *, timeout: float, responses: list[httpx.Response]) -> None:
        self._responses = responses

    async def __aenter__(self) -> "DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, params: dict, headers: dict) -> httpx.Response:
        if not self._responses:
            raise AssertionError("Unexpected Kraken request")
        return self._responses.pop(0)


def test_kraken_ohlc_parsing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(utils, "CACHE_ROOT", tmp_path)
    kraken._PAIR_CACHE = None
    request = httpx.Request("GET", "https://api.kraken.com/0/public/OHLC")
    response = httpx.Response(
        200,
        json={
            "error": [],
            "result": {
                "SOLUSD": [
                    [1704067200, "100", "110", "90", "105", "105", "1", "10"],
                    [1704153600, "105", "115", "95", "108", "108", "1", "10"],
                    [1704240000, "108", "120", "100", "111", "111", "1", "10"],
                ],
                "last": 1704240000,
            },
        },
        request=request,
    )

    def factory(*, timeout: float) -> DummyAsyncClient:
        return DummyAsyncClient(timeout=timeout, responses=[response])

    monkeypatch.setattr(kraken.httpx, "AsyncClient", factory)
    asyncio.run(kraken.warm_sol_usd_closes(date(2024, 1, 1), date(2024, 1, 3)))

    close = asyncio.run(kraken.get_sol_usd_close_for_date(date(2024, 1, 2)))
    assert close == Decimal("108")


def test_kraken_cache_hit_skips_http(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(utils, "CACHE_ROOT", tmp_path)
    kraken._PAIR_CACHE = None
    request = httpx.Request("GET", "https://api.kraken.com/0/public/OHLC")
    response = httpx.Response(
        200,
        json={
            "error": [],
            "result": {
                "SOLUSD": [
                    [1704067200, "100", "110", "90", "105", "105", "1", "10"],
                ],
                "last": 1704067200,
            },
        },
        request=request,
    )
    calls = {"count": 0}

    def factory(*, timeout: float) -> DummyAsyncClient:
        calls["count"] += 1
        return DummyAsyncClient(timeout=timeout, responses=[response])

    monkeypatch.setattr(kraken.httpx, "AsyncClient", factory)
    asyncio.run(kraken.warm_sol_usd_closes(date(2024, 1, 1), date(2024, 1, 1)))
    asyncio.run(kraken.warm_sol_usd_closes(date(2024, 1, 1), date(2024, 1, 1)))

    assert calls["count"] == 1
