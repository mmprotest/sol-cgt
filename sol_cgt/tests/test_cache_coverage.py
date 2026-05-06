from __future__ import annotations

from sol_cgt import utils
from sol_cgt.ingestion import fetch


def _set_cache_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(fetch, "RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(fetch, "PROVIDER_CHECKED_RANGES_PATH", tmp_path / "provider_checked_ranges.json")
    monkeypatch.setattr(fetch, "_PROVIDER_CHECKED_RANGES_IN_MEMORY", {})


def test_inspect_empty_cache_incomplete(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    c = fetch.inspect_raw_cache_coverage("w", 10, 20)
    assert not c.coverage_complete
    assert c.missing_ranges[0]["reason"] == "empty_cache"


def test_inspect_full_coverage_complete(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    utils.write_jsonl(tmp_path / "w.jsonl", [{"signature": "a", "timestamp": 10}, {"signature": "b", "timestamp": 20}], mode="w")
    c = fetch.inspect_raw_cache_coverage("w", 12, 19)
    assert c.coverage_complete


def test_inspect_missing_start_end(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    utils.write_jsonl(tmp_path / "w.jsonl", [{"signature": "a", "timestamp": 15}, {"signature": "b", "timestamp": 16}], mode="w")
    c = fetch.inspect_raw_cache_coverage("w", 10, 20)
    reasons = {r["reason"] for r in c.missing_ranges}
    assert "missing_start" in reasons
    assert "missing_end" in reasons


def test_inspect_malformed_incomplete(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    utils.write_jsonl(tmp_path / "w.jsonl", [{"signature": "a"}, {"signature": "b", "blockTime": 18}], mode="w")
    c = fetch.inspect_raw_cache_coverage("w", 10, 20)
    assert not c.coverage_complete
    assert c.malformed_rows > 0


def test_inspect_dedup_signature_not_double_count(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    utils.write_jsonl(tmp_path / "w.jsonl", [{"signature": "a", "timestamp": 10}, {"signature": "a", "timestamp": 10}], mode="w")
    c = fetch.inspect_raw_cache_coverage("w", 10, 10)
    assert c.raw_tx_count == 1


def test_provider_checked_range_marks_complete_even_if_cache_min_gt_start(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    utils.write_jsonl(tmp_path / "w.jsonl", [{"signature": "a", "timestamp": 20}], mode="w")
    fetch.record_provider_checked_range("w", "enhanced", "balanceChanged", 10, 30, 1, 1, 1, 20, 20, True)
    c = fetch.inspect_raw_cache_coverage("w", 10, 30)
    assert c.coverage_complete
    assert c.coverage_complete_reason == "provider_checked_range"


def test_provider_checked_zero_rows_exhausted_is_complete(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    fetch.record_provider_checked_range("w", "enhanced", "balanceChanged", 10, 30, 1, 0, 0, None, None, True)
    c = fetch.inspect_raw_cache_coverage("w", 10, 30)
    assert c.coverage_complete


def test_cache_min_gt_start_without_provider_checked_still_incomplete(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    utils.write_jsonl(tmp_path / "w.jsonl", [{"signature": "a", "timestamp": 20}], mode="w")
    c = fetch.inspect_raw_cache_coverage("w", 10, 30)
    assert not c.coverage_complete


def test_provider_checked_not_exhausted_is_not_complete(tmp_path, monkeypatch):
    _set_cache_paths(monkeypatch, tmp_path)
    fetch.record_provider_checked_range("w", "enhanced", "balanceChanged", 10, 30, 1, 0, 0, None, None, False)
    c = fetch.inspect_raw_cache_coverage("w", 10, 30)
    assert not c.coverage_complete
