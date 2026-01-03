from __future__ import annotations

import asyncio
import base64

import httpx

from sol_cgt.providers import jupiter


class DummyAsyncClient:
    def __init__(
        self,
        *,
        timeout: float,
        post_response: httpx.Response | None = None,
        get_response: httpx.Response | None = None,
        error_on_post: bool = False,
        error_on_get: bool = False,
        record: dict[str, str] | None = None,
    ) -> None:
        self._post_response = post_response
        self._get_response = get_response
        self._error_on_post = error_on_post
        self._error_on_get = error_on_get
        self._record = {} if record is None else record

    async def __aenter__(self) -> "DummyAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def post(self, url: str, json: dict) -> httpx.Response:
        self._record["post_url"] = url
        if self._error_on_post:
            raise httpx.ConnectError("boom", request=httpx.Request("POST", url))
        if self._post_response is None:
            raise AssertionError("Unexpected POST request")
        return self._post_response

    async def get(self, url: str, params: dict | None = None, headers: dict | None = None) -> httpx.Response:
        self._record["get_url"] = url
        if self._error_on_get:
            raise httpx.ConnectError("boom", request=httpx.Request("GET", url))
        if self._get_response is None:
            raise AssertionError("Unexpected GET request")
        return self._get_response


def test_jupiter_metadata_uses_rpc_decimals(monkeypatch) -> None:
    raw = b"\x00" * 44 + bytes([9]) + b"\x00"
    encoded = base64.b64encode(raw).decode()
    request = httpx.Request("POST", jupiter.DEFAULT_RPC_URL)
    response = httpx.Response(200, json={"result": {"value": {"data": [encoded, "base64"]}}}, request=request)

    def factory(*, timeout: float) -> DummyAsyncClient:
        return DummyAsyncClient(timeout=timeout, post_response=response)

    monkeypatch.setattr(jupiter.httpx, "AsyncClient", factory)
    symbol, decimals = asyncio.run(jupiter.token_metadata("MINT"))

    assert symbol is None
    assert decimals == 9


def test_jupiter_metadata_does_not_call_token_list(monkeypatch) -> None:
    record: dict[str, str] = {}
    request = httpx.Request("GET", f"{jupiter.JUPITER_TOKENS_V1_URL}/MINT")
    response = httpx.Response(200, json={"symbol": "ABC", "decimals": 6}, request=request)

    clients = [
        DummyAsyncClient(timeout=10.0, error_on_post=True, record=record),
        DummyAsyncClient(timeout=10.0, get_response=response, record=record),
    ]

    def factory(*, timeout: float) -> DummyAsyncClient:
        return clients.pop(0)

    monkeypatch.setattr(jupiter.httpx, "AsyncClient", factory)
    symbol, decimals = asyncio.run(jupiter.token_metadata("MINT"))

    assert symbol == "ABC"
    assert decimals == 6
    assert record["get_url"] == f"{jupiter.JUPITER_TOKENS_V1_URL}/MINT"


def test_jupiter_metadata_rpc_connect_error_returns_none(monkeypatch) -> None:
    clients = [
        DummyAsyncClient(timeout=10.0, error_on_post=True),
        DummyAsyncClient(timeout=10.0, error_on_get=True),
    ]

    def factory(*, timeout: float) -> DummyAsyncClient:
        return clients.pop(0)

    monkeypatch.setattr(jupiter.httpx, "AsyncClient", factory)
    symbol, decimals = asyncio.run(jupiter.token_metadata("MINT"))

    assert symbol is None
    assert decimals is None
