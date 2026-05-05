from __future__ import annotations

from pathlib import Path

from sol_cgt import cli
from sol_cgt.config import APIKeys, AppSettings


def test_compute_writes_raw_transactions_before_price_warmup(monkeypatch, tmp_path: Path) -> None:
    settings = AppSettings(wallets=["wallet"], api_keys=APIKeys(helius="key"))
    monkeypatch.setattr(cli, "load_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(cli.fetch_mod, "cache_has_data", lambda _: True)
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda _: [{"signature": "abc", "blockTime": 1}])

    order: list[str] = []

    def fake_export(xlsx_path, wallet, fy_label, source, raw_items):
        order.append("export")
        p = xlsx_path.parent / "raw_transactions.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{}", encoding="utf-8")
        return p

    async def fake_normalize(*args, **kwargs):
        return []

    async def fake_ensure(*args, **kwargs):
        order.append("price")
        return Path(".sol_cgt_cache/prices/sol_usd_daily.csv")

    monkeypatch.setattr(cli, "_export_raw_transactions", fake_export)
    monkeypatch.setattr(cli, "_normalize_wallet", fake_normalize)
    monkeypatch.setattr(cli.sol_price_table, "ensure_sol_usd_daily_prices", fake_ensure)
    monkeypatch.setattr(cli.sol_price_table, "cache_stats", lambda *_: (0, None, None))

    cli.compute(wallet=["wallet"], config=None, outdir=None, method=None, fy="2024-2025", fy_start=None, fy_end=None, fmt="csv", xlsx_path=tmp_path / "reports" / "out.xlsx", sol_price_csv=None, dry_run=True, fetch=False)

    assert order[0] == "export"
    assert (tmp_path / "reports" / "raw_transactions.json").exists()
