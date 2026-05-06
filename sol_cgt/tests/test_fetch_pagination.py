import asyncio

from sol_cgt import utils
from sol_cgt.ingestion import fetch


def test_fetch_wallet_paginates_with_pagination_token(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    pages = [
        [{"signature": "sig-2", "timestamp": 2}],
        [{"signature": "sig-1", "timestamp": 1}],
    ]
    calls: list[dict[str, object]] = []

    async def fake_fetch_v2(_: str, __: int, ___: int, **kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        if pages:
            return {"data": pages.pop(0), "paginationToken": "next" if pages else None}
        return {"data": [], "paginationToken": None}

    monkeypatch.setattr(fetch.helius, "fetch_wallet_transactions_for_period_v2", fake_fetch_v2)

    result = asyncio.run(fetch.fetch_wallet("wallet", api_key="key", base_url="https://example.com", gte_time=1, lte_time=2))

    assert [entry["signature"] for entry in result] == ["sig-2", "sig-1"]
    assert calls[0]["pagination_token"] is None
    assert calls[1]["pagination_token"] == "next"
    assert "before_signature" not in calls[0]

    cached = list(utils.read_jsonl(tmp_path / "wallet.jsonl"))
    assert [entry["signature"] for entry in cached] == ["sig-2", "sig-1"]


def test_fetch_wallet_keeps_paging_when_page_has_duplicates(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    pages = [
        [{"signature": "sig-3", "timestamp": 3}, {"signature": "sig-2", "timestamp": 2}],
        [{"signature": "sig-2", "timestamp": 2}, {"signature": "sig-1", "timestamp": 1}],
    ]

    async def fake_fetch_v2(_: str, __: int, ___: int, **kwargs: object) -> dict[str, object]:
        if pages:
            return {"data": pages.pop(0), "paginationToken": "next" if pages else None}
        return {"data": [], "paginationToken": None}

    monkeypatch.setattr(fetch.helius, "fetch_wallet_transactions_for_period_v2", fake_fetch_v2)
    result = asyncio.run(fetch.fetch_wallet("wallet", api_key="key", base_url="https://example.com", gte_time=1, lte_time=4))
    assert [r["signature"] for r in result] == ["sig-3", "sig-2", "sig-1"]
