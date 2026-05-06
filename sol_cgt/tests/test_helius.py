import pytest

from sol_cgt.providers import helius


@pytest.mark.asyncio
async def test_fetch_txs_clamps_limit(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_read_cache(_: str) -> None:
        return None

    async def fake_write_cache(_: str, __: object) -> None:
        return None

    async def fake_perform_request(_: object, __: str, params: dict[str, object]) -> list[dict[str, object]]:
        captured.append(params)
        return []

    monkeypatch.setattr(helius, "_read_cache", fake_read_cache)
    monkeypatch.setattr(helius, "_write_cache", fake_write_cache)
    monkeypatch.setattr(helius, "_perform_request", fake_perform_request)

    await helius.fetch_txs(
        "wallet",
        limit=1000,
        api_key="key",
        base_url="https://example.com",
        before_signature="before",
        after_signature="after",
        sort_order="asc",
        gte_time=1_000,
        lte_time=2_000,
    )

    assert captured
    assert captured[0]["limit"] == helius.HELIUS_TX_LIMIT_MAX
    assert captured[0]["sort-order"] == "asc"
    assert captured[0]["before-signature"] == "before"
    assert captured[0]["after-signature"] == "after"
    assert captured[0]["gte-time"] == 1_000
    assert captured[0]["lte-time"] == 2_000
    assert captured[0]["token-accounts"] == "balanceChanged"


@pytest.mark.asyncio
async def test_fetch_txs_uses_signature_params(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_read_cache(_: str) -> None:
        return None

    async def fake_write_cache(_: str, __: object) -> None:
        return None

    async def fake_perform_request(_: object, __: str, params: dict[str, object]) -> list[dict[str, object]]:
        captured.append(params)
        return []

    monkeypatch.setattr(helius, "_read_cache", fake_read_cache)
    monkeypatch.setattr(helius, "_write_cache", fake_write_cache)
    monkeypatch.setattr(helius, "_perform_request", fake_perform_request)

    await helius.fetch_txs(
        "wallet",
        limit=100,
        api_key="key",
        base_url="https://example.com",
        before_signature="sig-1",
        after_signature="sig-0",
    )

    assert captured
    assert captured[0]["before-signature"] == "sig-1"
    assert captured[0]["after-signature"] == "sig-0"


@pytest.mark.asyncio
async def test_fetch_wallet_transactions_for_period_v2_request_body(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_perform_rpc_request(_: object, __: str, body: dict[str, object]) -> dict[str, object]:
        captured.append(body)
        return {"result": {"data": [], "paginationToken": None}}

    monkeypatch.setattr(helius, "_perform_rpc_request", fake_perform_rpc_request)
    await helius.fetch_wallet_transactions_for_period_v2(
        "wallet",
        1_000,
        2_000,
        api_key="key",
        rpc_url="https://mainnet.helius-rpc.com/",
    )
    params = captured[0]["params"][1]
    assert captured[0]["method"] == "getTransactionsForAddress"
    assert params["transactionDetails"] == "full"
    assert params["sortOrder"] == "asc"
    assert params["filters"]["blockTime"]["gte"] == 1_000
    assert params["filters"]["blockTime"]["lte"] == 2_000
    assert params["filters"]["status"] == "succeeded"
    assert params["filters"]["tokenAccounts"] == "balanceChanged"
