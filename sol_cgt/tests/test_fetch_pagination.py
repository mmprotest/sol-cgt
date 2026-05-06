import asyncio

from sol_cgt import utils
from sol_cgt.ingestion import fetch


def test_fetch_wallet_paginates_with_before_signature(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    pages = [
        [{"signature": "sig-2", "timestamp": 2}],
        [{"signature": "sig-1", "timestamp": 1}],
    ]
    calls: list[dict[str, object]] = []

    async def fake_fetch_txs(_: str, **kwargs: object) -> list[dict[str, object]]:
        calls.append(kwargs)
        if pages:
            return pages.pop(0)
        return []

    monkeypatch.setattr(fetch.helius, "fetch_txs", fake_fetch_txs)

    result = asyncio.run(fetch.fetch_wallet("wallet", api_key="key", base_url="https://example.com"))

    assert [entry["signature"] for entry in result] == ["sig-2", "sig-1"]
    assert calls[0]["before_signature"] is None
    assert calls[1]["before_signature"] == "sig-2"

    cached = list(utils.read_jsonl(tmp_path / "wallet.jsonl"))
    assert [entry["signature"] for entry in cached] == ["sig-2", "sig-1"]


def test_fetch_wallet_keeps_paging_when_page_has_duplicates(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    pages = [
        [{"signature": "sig-3", "timestamp": 3}, {"signature": "sig-2", "timestamp": 2}],
        [{"signature": "sig-2", "timestamp": 2}, {"signature": "sig-1", "timestamp": 1}],
    ]

    async def fake_fetch_txs(_: str, **kwargs: object) -> list[dict[str, object]]:
        if pages:
            return pages.pop(0)
        return []

    monkeypatch.setattr(fetch.helius, "fetch_txs", fake_fetch_txs)
    result = asyncio.run(fetch.fetch_wallet("wallet", api_key="key", base_url="https://example.com"))
    assert [r["signature"] for r in result] == ["sig-3", "sig-2", "sig-2", "sig-1"]
