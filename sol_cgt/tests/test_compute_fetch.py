from sol_cgt import cli, utils
from sol_cgt.config import APIKeys, AppSettings


def test_compute_fetches_missing_cache_with_fy_filters(monkeypatch) -> None:
    settings = AppSettings(wallets=["wallet"], api_keys=APIKeys(helius="key"))
    monkeypatch.setattr(cli, "load_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(
        cli.fetch_mod,
        "inspect_raw_cache_coverage",
        lambda wallet, start, end: cli.fetch_mod.CacheCoverage(
            wallet=wallet,
            cache_path=f"{wallet}.jsonl",
            has_cache=False,
            raw_tx_count=0,
            cache_min_timestamp=None,
            cache_max_timestamp=None,
            requested_start=start,
            requested_end=end,
            covers_start=False,
            covers_end=False,
            coverage_complete=False,
            missing_ranges=[{"start": start, "end": end, "reason": "empty_cache"}],
        ),
    )

    captured: dict[str, object] = {}

    async def fake_fetch_wallet(wallet: str, **kwargs: object) -> list[dict]:
        captured["wallet"] = wallet
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(cli.fetch_mod, "fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda _: [])

    async def fake_normalize(_: str, __: list[dict], **kwargs: object) -> list[cli.NormalizedEvent]:
        return []

    monkeypatch.setattr(cli, "_normalize_wallet", fake_normalize)
    monkeypatch.setattr(cli.transfers, "detect_self_transfers", lambda *args, **kwargs: [])

    class DummyPriceProvider:
        def __init__(self, *args, **kwargs) -> None:
            pass

    monkeypatch.setattr(cli, "AudPriceProvider", DummyPriceProvider)

    async def fake_ensure(*args, **kwargs):
        from pathlib import Path
        return Path(".sol_cgt_cache/prices/sol_usd_daily.csv")

    monkeypatch.setattr(cli.sol_price_table, "ensure_sol_usd_daily_prices", fake_ensure)
    monkeypatch.setattr(cli.sol_price_table, "cache_stats", lambda *_: (0, None, None))
    monkeypatch.setattr(cli.fx_price_table, "ensure_usd_aud_daily_rates", fake_ensure)
    monkeypatch.setattr(cli.fx_price_table, "cache_stats", lambda *_: (0, None, None))

    cli.compute(
        wallet=["wallet"],
        config=None,
        outdir=None,
        method=None,
        fy="2024-2025",
        fy_start=None,
        fy_end=None,
        fmt="csv",
        xlsx_path=None,
        sol_price_csv=None,
        dry_run=True,
        fetch=True,
    )

    fy_period = utils.australian_financial_year_bounds("2024-2025")
    assert captured["wallet"] == "wallet"
    assert captured["kwargs"]["gte_time"] == int(fy_period.start.timestamp())
    assert captured["kwargs"]["lte_time"] == int(fy_period.end.timestamp())


def test_no_fetch_fails_on_incomplete_cache_coverage(monkeypatch) -> None:
    settings = AppSettings(wallets=["wallet"], api_keys=APIKeys(helius="key"))
    monkeypatch.setattr(cli, "load_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(
        cli.fetch_mod,
        "inspect_raw_cache_coverage",
        lambda wallet, start, end: cli.fetch_mod.CacheCoverage(
            wallet=wallet,
            cache_path=f"{wallet}.jsonl",
            has_cache=True,
            raw_tx_count=1,
            cache_min_timestamp=1719792000,
            cache_max_timestamp=1722470399,
            requested_start=start,
            requested_end=end,
            covers_start=False,
            covers_end=False,
            coverage_complete=False,
            missing_ranges=[{"start": start, "end": 1719792000, "reason": "missing_start"}],
        ),
    )
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda _: [])
    try:
        cli.compute(wallet=["wallet"], config=None, outdir=None, method=None, fy="2023-2024", fy_start=None, fy_end=None, fmt="csv", xlsx_path=None, sol_price_csv=None, dry_run=True, fetch=False)
        assert False, "expected failure"
    except Exception as exc:
        assert "incomplete" in str(exc).lower()
