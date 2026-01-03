from sol_cgt import cli, utils
from sol_cgt.config import APIKeys, AppSettings


def test_compute_fetches_missing_cache_with_fy_filters(monkeypatch) -> None:
    settings = AppSettings(wallets=["wallet"], api_keys=APIKeys(helius="key"))
    monkeypatch.setattr(cli, "load_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(cli.fetch_mod, "cache_has_data", lambda _: False)

    captured: dict[str, object] = {}

    async def fake_fetch_wallet(wallet: str, **kwargs: object) -> list[dict]:
        captured["wallet"] = wallet
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(cli.fetch_mod, "fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(cli.fetch_mod, "load_cached", lambda _: [])

    async def fake_normalize(_: str, __: list[dict]) -> list[cli.NormalizedEvent]:
        return []

    monkeypatch.setattr(cli, "_normalize_wallet", fake_normalize)
    monkeypatch.setattr(cli.transfers, "detect_self_transfers", lambda *args, **kwargs: [])

    class DummyPriceProvider:
        def __init__(self, *args, **kwargs) -> None:
            pass

    monkeypatch.setattr(cli, "AudPriceProvider", DummyPriceProvider)

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
        dry_run=True,
        fetch=True,
    )

    fy_period = utils.australian_financial_year_bounds("2024-2025")
    assert captured["wallet"] == "wallet"
    assert captured["kwargs"]["gte_time"] == int(fy_period.start.timestamp())
    assert captured["kwargs"]["lte_time"] == int(fy_period.end.timestamp())
