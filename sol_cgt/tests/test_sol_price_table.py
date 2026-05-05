from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from sol_cgt.pricing import TimestampPriceProvider, WSOL_MINT, normalize_mint
from sol_cgt.providers import sol_price_table


def test_sol_mint_normalization_variants() -> None:
    assert normalize_mint("SOL") == WSOL_MINT
    assert normalize_mint("WSOL") == WSOL_MINT
    assert normalize_mint(WSOL_MINT) == WSOL_MINT


def test_ensure_prices_uses_cache_when_range_already_covered(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "prices.csv"
    monkeypatch.setattr(sol_price_table, "CACHE_PATH", cache_path)
    sol_price_table._write_cache(
        cache_path,
        [
            sol_price_table.SolDailyPrice(date(2024, 1, 1), Decimal("10"), Decimal("11"), Decimal("9"), Decimal("10.5"), Decimal("100")),
            sol_price_table.SolDailyPrice(date(2024, 1, 2), Decimal("10"), Decimal("11"), Decimal("9"), Decimal("10.5"), Decimal("100")),
        ],
    )

    async def fail_download(*args, **kwargs):
        raise AssertionError("download should not be called")

    monkeypatch.setattr(sol_price_table, "_download_range", fail_download)
    path = __import__("asyncio").run(sol_price_table.ensure_sol_usd_daily_prices(date(2024, 1, 1), date(2024, 1, 2)))
    assert path == cache_path


def test_missing_dates_detected(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "prices.csv"
    monkeypatch.setattr(sol_price_table, "CACHE_PATH", cache_path)
    sol_price_table._write_cache(
        cache_path,
        [sol_price_table.SolDailyPrice(date(2024, 1, 1), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"))],
    )

    async def empty_download(*args, **kwargs):
        return {}

    monkeypatch.setattr(sol_price_table, "_download_range", empty_download)
    try:
        __import__("asyncio").run(sol_price_table.ensure_sol_usd_daily_prices(date(2024, 1, 1), date(2024, 1, 3)))
        assert False, "expected runtime error"
    except RuntimeError as exc:
        assert "missing dates" in str(exc)


def test_sol_price_uses_au_local_date(monkeypatch) -> None:
    provider = TimestampPriceProvider(api_key="key")
    captured = {}

    def fake(day):
        captured["day"] = day
        return Decimal("123")

    monkeypatch.setattr(sol_price_table, "get_sol_usd_close_for_date", fake)
    ts = datetime(2024, 1, 1, 13, 30, tzinfo=timezone.utc)
    assert provider.price_usd("SOL", ts) == Decimal("123")
    assert captured["day"].isoformat() == "2024-01-02"


def test_non_sol_does_not_fallback_to_sol(monkeypatch) -> None:
    provider = TimestampPriceProvider(api_key=None)
    monkeypatch.setattr(sol_price_table, "get_sol_usd_close_for_date", lambda _day: Decimal("99"))
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert provider.price_usd("TOKENX", ts) is None
