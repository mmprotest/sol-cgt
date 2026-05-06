from __future__ import annotations

from pathlib import Path

from sol_cgt import cli
from sol_cgt.config import APIKeys, AppSettings


def test_compute_writes_combined_raw_transactions_before_price_warmup(monkeypatch, tmp_path: Path) -> None:
    settings = AppSettings(wallets=["wallet1", "wallet2"], api_keys=APIKeys(helius="key"))
    monkeypatch.setattr(cli, "load_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda w: [{"signature": f"{w}-sig", "blockTime": 1}])
    monkeypatch.setattr(
        cli.fetch_mod,
        "inspect_raw_cache_coverage",
        lambda wallet, start, end: cli.fetch_mod.CacheCoverage(
            wallet=wallet,
            cache_path=f"{wallet}.jsonl",
            has_cache=True,
            raw_tx_count=1,
            cache_min_timestamp=start,
            cache_max_timestamp=end,
            requested_start=start,
            requested_end=end,
            covers_start=True,
            covers_end=True,
            coverage_complete=True,
            missing_ranges=[],
        ),
    )

    order: list[str] = []

    def fake_export(xlsx_path, wallets, fy_label, source, raw_by_wallet):
        order.append("export")
        assert wallets == ["wallet1", "wallet2"]
        assert set(raw_by_wallet) == {"wallet1", "wallet2"}
        p = xlsx_path.parent / "raw_transactions.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{}", encoding="utf-8")
        return p

    async def fake_normalize(*args, **kwargs):
        return []

    async def fake_ensure_sol(*args, **kwargs):
        order.append("sol")
        return Path(".sol_cgt_cache/prices/sol_usd_daily.csv")

    async def fake_ensure_fx(*args, **kwargs):
        order.append("fx")
        return Path(".sol_cgt_cache/fx/usd_aud_daily.csv")

    monkeypatch.setattr(cli, "_export_raw_transactions", fake_export)
    monkeypatch.setattr(cli, "_normalize_wallet", fake_normalize)
    monkeypatch.setattr(cli.sol_price_table, "ensure_sol_usd_daily_prices", fake_ensure_sol)
    monkeypatch.setattr(cli.sol_price_table, "cache_stats", lambda *_: (0, None, None))
    monkeypatch.setattr(cli.fx_price_table, "ensure_usd_aud_daily_rates", fake_ensure_fx)
    monkeypatch.setattr(cli.fx_price_table, "cache_stats", lambda *_: (0, None, None))

    cli.compute(wallet=["wallet1", "wallet2"], config=None, outdir=None, method=None, fy="2024-2025", fy_start=None, fy_end=None, fmt="csv", xlsx_path=tmp_path / "reports" / "out.xlsx", sol_price_csv=None, dry_run=True, fetch=False)

    assert order[:1] == ["export"]
    assert (tmp_path / "reports" / "raw_transactions.json").exists()
