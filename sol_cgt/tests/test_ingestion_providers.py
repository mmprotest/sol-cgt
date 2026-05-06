import httpx
import pytest

from sol_cgt.ingestion import fetch


def test_default_rate_limit_free_compatible() -> None:
    assert fetch.fetch_wallet.__defaults__ is None  # keyword-only defaults only


@pytest.mark.asyncio
async def test_enhanced_pagination_continues_duplicate_only_pages(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    pages = [
        [{"signature": "s1", "timestamp": 10}],
        [{"signature": "s1", "timestamp": 10}],
        [{"signature": "s2", "timestamp": 11}],
        [],
    ]

    async def fake_fetch_txs(*args, **kwargs):
        before = kwargs.get("before_signature")
        if before is None:
            return pages[0]
        if before == "s1" and len(fake_fetch_txs.calls) == 1:
            return pages[1]
        if before == "s1" and len(fake_fetch_txs.calls) == 2:
            return pages[2]
        return pages[3]

    fake_fetch_txs.calls = []

    async def wrapped(*args, **kwargs):
        fake_fetch_txs.calls.append(1)
        return await fake_fetch_txs(*args, **kwargs)

    monkeypatch.setattr(fetch.helius, "fetch_txs", wrapped)
    rows = await fetch.fetch_wallet("w", gte_time=1, lte_time=20, provider="enhanced")
    assert [r["signature"] for r in rows] == ["s1", "s2"]
    assert len(fake_fetch_txs.calls) >= 3


@pytest.mark.asyncio
async def test_get_transactions_403_fallback_auto(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)

    async def fail_rpc(*args, **kwargs):
        req = httpx.Request("POST", "https://example.com")
        resp = httpx.Response(403, request=req)
        raise httpx.HTTPStatusError("forbidden", request=req, response=resp)

    async def ok_enhanced(*args, **kwargs):
        return [{"signature": "s1", "timestamp": 10}]

    monkeypatch.setattr(fetch.helius, "fetch_wallet_transactions_for_period_v2", fail_rpc)
    monkeypatch.setattr(fetch.helius, "fetch_txs", ok_enhanced)
    rows = await fetch.fetch_wallet("w", gte_time=1, lte_time=20, provider="auto")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_get_transactions_403_forced_fails(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)

    async def fail_rpc(*args, **kwargs):
        req = httpx.Request("POST", "https://example.com")
        resp = httpx.Response(403, request=req)
        raise httpx.HTTPStatusError("forbidden", request=req, response=resp)

    monkeypatch.setattr(fetch.helius, "fetch_wallet_transactions_for_period_v2", fail_rpc)
    with pytest.raises(RuntimeError):
        await fetch.fetch_wallet("w", gte_time=1, lte_time=20, provider="getTransactionsForAddress")
