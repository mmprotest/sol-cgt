"""SOL/USD daily OHLC table provider with local CSV cache."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import csv
import logging
import os
from pathlib import Path
from typing import Iterable

import httpx


LOGGER = logging.getLogger(__name__)
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "SOLUSD"
COINGECKO_RANGE_URL = "https://api.coingecko.com/api/v3/coins/solana/market_chart/range"
CACHE_PATH = Path(".sol_cgt_cache") / "prices" / "sol_usd_daily.csv"
SOURCE = "kraken"


@dataclass(frozen=True)
class SolDailyPrice:
    day: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    source: str = SOURCE


def _cache_path() -> Path:
    return CACHE_PATH


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(str(raw))


def _read_cache(path: Path) -> dict[date, SolDailyPrice]:
    if not path.exists():
        return {}
    rows: dict[date, SolDailyPrice] = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            day = date.fromisoformat(str(row["date"]))
            rows[day] = SolDailyPrice(day=day, open=_parse_decimal(str(row["open"])), high=_parse_decimal(str(row["high"])), low=_parse_decimal(str(row["low"])), close=_parse_decimal(str(row["close"])), volume=_parse_decimal(str(row["volume"])), source=str(row.get("source") or SOURCE))
    return rows


def _write_cache(path: Path, rows: Iterable[SolDailyPrice]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "open", "high", "low", "close", "volume", "source"])
        writer.writeheader()
        for row in sorted(rows, key=lambda r: r.day):
            writer.writerow({"date": row.day.isoformat(), "open": str(row.open), "high": str(row.high), "low": str(row.low), "close": str(row.close), "volume": str(row.volume), "source": row.source})
    tmp.replace(path)


def _missing_dates(cache: dict[date, SolDailyPrice], start_date: date, end_date: date) -> list[date]:
    missing: list[date] = []
    day = start_date
    while day <= end_date:
        if day not in cache:
            missing.append(day)
        day += timedelta(days=1)
    return missing


def _collapse_ranges(days: list[date]) -> list[tuple[date, date]]:
    if not days:
        return []
    sorted_days = sorted(days)
    ranges: list[tuple[date, date]] = []
    start = end = sorted_days[0]
    for day in sorted_days[1:]:
        if day == end + timedelta(days=1):
            end = day
            continue
        ranges.append((start, end))
        start = end = day
    ranges.append((start, end))
    return ranges


async def _download_range_kraken(start_date: date, end_date: date) -> dict[date, SolDailyPrice]:
    since = int(datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc).timestamp())
    params = {"pair": KRAKEN_PAIR, "interval": 1440, "since": since}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(KRAKEN_OHLC_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken returned errors: {data['error']}")
    rows = data.get("result", {}).get(KRAKEN_PAIR) or []
    parsed: dict[date, SolDailyPrice] = {}
    for row in rows:
        ts = int(row[0])
        day = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        if day < start_date or day > end_date:
            continue
        parsed[day] = SolDailyPrice(day=day, open=_parse_decimal(row[1]), high=_parse_decimal(row[2]), low=_parse_decimal(row[3]), close=_parse_decimal(row[4]), volume=_parse_decimal(row[6]), source="kraken")
    return parsed


async def _download_range_coingecko(start_date: date, end_date: date) -> dict[date, SolDailyPrice]:
    from_ts = int(datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc).timestamp())
    to_ts = int(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(COINGECKO_RANGE_URL, params={"vs_currency": "usd", "from": from_ts, "to": to_ts})
        resp.raise_for_status()
        payload = resp.json()
    prices = payload.get("prices") or []
    parsed: dict[date, SolDailyPrice] = {}
    for ts_ms, price in prices:
        day = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).date()
        if day < start_date or day > end_date:
            continue
        dec = _parse_decimal(str(price))
        parsed[day] = SolDailyPrice(day=day, open=dec, high=dec, low=dec, close=dec, volume=Decimal("0"), source="coingecko")
    return parsed


async def _download_range(start_date: date, end_date: date) -> tuple[str, dict[date, SolDailyPrice]]:
    try:
        return "kraken", await _download_range_kraken(start_date, end_date)
    except Exception:
        return "coingecko", await _download_range_coingecko(start_date, end_date)


def _manual_csv_path() -> Path | None:
    manual = os.getenv("SOL_CGT_SOL_PRICE_CSV")
    return Path(manual) if manual else None


async def ensure_sol_usd_daily_prices(start_date: date, end_date: date) -> Path:
    path = _cache_path()
    cache = _read_cache(path)
    manual = _manual_csv_path()
    if manual and manual.exists():
        cache.update(_read_cache(manual))
        _write_cache(path, cache.values())
    missing = _missing_dates(cache, start_date, end_date)
    if not missing:
        return path
    ranges = _collapse_ranges(missing)
    LOGGER.info("SOL/USD daily price download source=kraken start=%s end=%s missing_days=%s ranges=%s requests=%s", start_date.isoformat(), end_date.isoformat(), len(missing), len(ranges), len(ranges))
    attempted_sources: list[str] = []
    for range_start, range_end in ranges:
        try:
            source, downloaded = await _download_range(range_start, range_end)
            attempted_sources.append(source)
            cache.update(downloaded)
        except Exception as exc:
            attempted_sources.append("kraken/coingecko")
            missing_after = _missing_dates(cache, start_date, end_date)
            if not missing_after:
                return path
            raise RuntimeError(
                "SOL/USD price download failed "
                f"attempted={','.join(attempted_sources)} requested={start_date.isoformat()}..{end_date.isoformat()} "
                f"missing={missing_after[0].isoformat()}..{missing_after[-1].isoformat()} cache={path} "
                "You may manually place sol_usd_daily.csv at the cache path or set SOL_CGT_SOL_PRICE_CSV."
            ) from exc
    missing_after = _missing_dates(cache, start_date, end_date)
    if missing_after:
        raise RuntimeError(
            f"SOL/USD price table incomplete attempted={','.join(attempted_sources)} requested={start_date.isoformat()}..{end_date.isoformat()} missing={missing_after[0].isoformat()}..{missing_after[-1].isoformat()} cache={path}"
        )
    _write_cache(path, cache.values())
    return path


def get_sol_usd_close_for_date(day: date) -> Decimal | None:
    cache = _read_cache(_cache_path())
    row = cache.get(day)
    return None if row is None else row.close


def cache_stats(path: Path) -> tuple[int, date | None, date | None]:
    cache = _read_cache(path)
    if not cache:
        return 0, None, None
    days = sorted(cache)
    return len(days), days[0], days[-1]
