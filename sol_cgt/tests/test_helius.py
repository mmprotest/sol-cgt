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

    await helius.fetch_txs("wallet", limit=1000, api_key="key", base_url="https://example.com")

    assert captured
    assert captured[0]["limit"] == helius.HELIUS_TX_LIMIT_MAX
    assert captured[0]["sort-order"] == "desc"


@pytest.mark.asyncio
async def test_fetch_txs_paginates_until_fy_start(monkeypatch) -> None:
    captured: list[dict[str, object]] = []
    pages = [
        [{"signature": "sig-1", "timestamp": 2_000}],
        [{"signature": "sig-0", "timestamp": 1_000}],
    ]

    async def fake_read_cache(_: str) -> None:
        return None

    async def fake_write_cache(_: str, __: object) -> None:
        return None

    async def fake_perform_request(_: object, __: str, params: dict[str, object]) -> list[dict[str, object]]:
        captured.append(params)
        if pages:
            return pages.pop(0)
        return []

    monkeypatch.setattr(helius, "_read_cache", fake_read_cache)
    monkeypatch.setattr(helius, "_write_cache", fake_write_cache)
    monkeypatch.setattr(helius, "_perform_request", fake_perform_request)

    result = await helius.fetch_txs(
        "wallet",
        limit=100,
        api_key="key",
        base_url="https://example.com",
        fy_start_ts=1_500,
        max_pages=10,
    )

    assert result == [{"signature": "sig-1", "timestamp": 2_000}, {"signature": "sig-0", "timestamp": 1_000}]
    assert len(captured) == 2
    assert "before-signature" not in captured[0]
    assert captured[1]["before-signature"] == "sig-1"
