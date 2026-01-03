from __future__ import annotations

from sol_cgt.ingestion import fetch
from sol_cgt import utils


def test_load_cached_dedup(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    cache_path = tmp_path / "wallet.jsonl"
    records = [
        {"signature": "sig1", "value": 1},
        {"signature": "sig1", "value": 2},
        {"signature": "sig2", "value": 3},
    ]
    utils.write_jsonl(cache_path, records, mode="w")
    loaded = fetch.load_cached("wallet")
    assert len(loaded) == 2
    assert loaded[0]["signature"] == "sig1"
    assert loaded[1]["signature"] == "sig2"
