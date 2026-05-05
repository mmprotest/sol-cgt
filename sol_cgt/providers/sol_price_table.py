"""SOL/USD daily OHLC table provider with local CSV cache."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import csv
import logging
from pathlib import Path
from typing import Iterable

import httpx


LOGGER = logging.getLogger(__name__)
YAHOO_DOWNLOAD_URL = "https://query1.finance.yahoo.com/v7/finance/download/SOL-USD"
CACHE_PATH = Path(".sol_cgt_cache") / "prices" / "sol_usd_daily.csv"
SOURCE = "yahoo"


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
            rows[day] = SolDailyPrice(
                day=day,
                open=_parse_decimal(str(row["open"])),
                high=_parse_decimal(str(row["high"])),
                low=_parse_decimal(str(row["low"])),
                close=_parse_decimal(str(row["close"])),
                volume=_parse_decimal(str(row["volume"])),
                source=str(row.get("source") or SOURCE),
            )
    return rows


def _write_cache(path: Path, rows: Iterable[SolDailyPrice]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "open", "high", "low", "close", "volume", "source"])
        writer.writeheader()
        for row in sorted(rows, key=lambda r: r.day):
            writer.writerow(
                {
                    "date": row.day.isoformat(),
                    "open": str(row.open),
                    "high": str(row.high),
                    "low": str(row.low),
                    "close": str(row.close),
                    "volume": str(row.volume),
                    "source": row.source,
                }
            )
    tmp.replace(path)


def _missing_dates(cache: dict[date, SolDailyPrice], start_date: date, end_date: date) -> list[date]:
    missing: list[date] = []
    day = start_date
    while day <= end_date:
        if day not in cache:
            missing.append(day)
        day += timedelta(days=1)
    return missing


async def _download_range(start_date: date, end_date: date) -> dict[date, SolDailyPrice]:
    period1 = int(datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc).timestamp())
    period2 = int(datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    params = {"period1": period1, "period2": period2, "interval": "1d", "events": "history", "includeAdjustedClose": "true"}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(YAHOO_DOWNLOAD_URL, params=params)
        resp.raise_for_status()
    parsed: dict[date, SolDailyPrice] = {}
    reader = csv.DictReader(resp.text.splitlines())
    for row in reader:
        if not row.get("Date") or row.get("Close") in {None, "null", ""}:
            continue
        if row["Open"] == "null" or row["High"] == "null" or row["Low"] == "null" or row["Volume"] == "null":
            continue
        day = date.fromisoformat(row["Date"])
        parsed[day] = SolDailyPrice(
            day=day,
            open=_parse_decimal(row["Open"]),
            high=_parse_decimal(row["High"]),
            low=_parse_decimal(row["Low"]),
            close=_parse_decimal(row["Close"]),
            volume=_parse_decimal(row["Volume"]),
            source=SOURCE,
        )
    return parsed


async def ensure_sol_usd_daily_prices(start_date: date, end_date: date) -> Path:
    path = _cache_path()
    cache = _read_cache(path)
    missing = _missing_dates(cache, start_date, end_date)
    if not missing:
        return path
    try:
        downloaded = await _download_range(min(missing), max(missing))
    except Exception as exc:
        missing_after = _missing_dates(cache, start_date, end_date)
        if not missing_after:
            return path
        raise RuntimeError(
            f"SOL/USD price table unavailable; missing dates {missing_after[0].isoformat()}..{missing_after[-1].isoformat()}"
        ) from exc
    for day, row in downloaded.items():
        cache[day] = row
    missing_after = _missing_dates(cache, start_date, end_date)
    if missing_after:
        raise RuntimeError(
            f"SOL/USD price table incomplete; missing dates {missing_after[0].isoformat()}..{missing_after[-1].isoformat()}"
        )
    _write_cache(path, cache.values())
    return path


def get_sol_usd_close_for_date(day: date) -> Decimal | None:
    cache = _read_cache(_cache_path())
    row = cache.get(day)
    if row is None:
        return None
    return row.close


def cache_stats(path: Path) -> tuple[int, date | None, date | None]:
    cache = _read_cache(path)
    if not cache:
        return 0, None, None
    days = sorted(cache)
    return len(days), days[0], days[-1]
