"""USD/AUD daily FX table provider with local CSV cache."""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterable

import httpx

CACHE_PATH = Path(".sol_cgt_cache") / "fx" / "usd_aud_daily.csv"
BASE_URL = "https://api.frankfurter.dev"
SOURCE = "frankfurter"
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FxDailyRate:
    day: date
    usd_to_aud: Decimal
    source: str = SOURCE


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(str(raw))


def _read_cache(path: Path) -> dict[date, FxDailyRate]:
    if not path.exists():
        return {}
    rows: dict[date, FxDailyRate] = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            day = date.fromisoformat(str(row["date"]))
            rows[day] = FxDailyRate(day=day, usd_to_aud=_parse_decimal(str(row["usd_to_aud"])), source=str(row.get("source") or SOURCE))
    return rows


def _write_cache(path: Path, rows: Iterable[FxDailyRate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "usd_to_aud", "source"])
        writer.writeheader()
        for row in sorted(rows, key=lambda r: r.day):
            writer.writerow({"date": row.day.isoformat(), "usd_to_aud": str(row.usd_to_aud), "source": row.source})
    tmp.replace(path)


def _missing_dates(cache: dict[date, FxDailyRate], start_date: date, end_date: date) -> list[date]:
    out: list[date] = []
    day = start_date
    while day <= end_date:
        if day not in cache:
            out.append(day)
        day += timedelta(days=1)
    return out


def _collapse_ranges(days: list[date]) -> list[tuple[date, date]]:
    if not days:
        return []
    days = sorted(days)
    ranges: list[tuple[date, date]] = []
    start = end = days[0]
    for day in days[1:]:
        if day == end + timedelta(days=1):
            end = day
        else:
            ranges.append((start, end))
            start = end = day
    ranges.append((start, end))
    return ranges


async def _download_range(start_date: date, end_date: date) -> dict[date, FxDailyRate]:
    params = {"from": start_date.isoformat(), "to": end_date.isoformat(), "base": "USD", "quotes": "AUD"}
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(f"{BASE_URL}/v2/rates", params=params)
        resp.raise_for_status()
        payload = resp.json()
    parsed: dict[date, FxDailyRate] = {}
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            day_raw = item.get("date")
            base = item.get("base")
            quote = item.get("quote")
            rate = item.get("rate")
            if not day_raw or base != "USD" or quote != "AUD" or rate is None:
                continue
            day = date.fromisoformat(str(day_raw))
            if start_date <= day <= end_date:
                parsed[day] = FxDailyRate(day=day, usd_to_aud=_parse_decimal(str(rate)), source=SOURCE)
    elif isinstance(payload, dict):
        rates = payload.get("rates")
        if isinstance(rates, dict):
            for day_raw, quote_map in rates.items():
                if not isinstance(quote_map, dict) or "AUD" not in quote_map:
                    continue
                day = date.fromisoformat(str(day_raw))
                if start_date <= day <= end_date:
                    parsed[day] = FxDailyRate(day=day, usd_to_aud=_parse_decimal(str(quote_map["AUD"])), source=SOURCE)
    if not parsed:
        raise RuntimeError("Missing rates from Frankfurter response")
    LOGGER.info("USD/AUD FX download source=frankfurter start=%s end=%s rows=%s", start_date.isoformat(), end_date.isoformat(), len(parsed))
    return parsed


async def ensure_usd_aud_daily_rates(start_date: date, end_date: date) -> Path:
    path = CACHE_PATH
    cache = _read_cache(path)
    downloaded = await _download_range(start_date, end_date)
    if downloaded:
        cache.update(downloaded)
        _write_cache(path, cache.values())
    if not cache:
        raise RuntimeError(f"USD/AUD FX table empty for requested range {start_date}..{end_date}")
    in_range = [d for d in cache if start_date <= d <= end_date]
    if not in_range:
        raise RuntimeError(f"USD/AUD FX table has no rates in requested range {start_date}..{end_date}")
    return path


def get_usd_aud_for_date(day: date) -> Decimal | None:
    row = _read_cache(CACHE_PATH).get(day)
    return row.usd_to_aud if row else None


def get_usd_aud_for_date_or_prior(day: date, *, max_fallback_days: int = 10) -> Decimal | None:
    cache = _read_cache(CACHE_PATH)
    for offset in range(0, max_fallback_days + 1):
        probe = day - timedelta(days=offset)
        row = cache.get(probe)
        if row is not None:
            return row.usd_to_aud
    return None


def cache_stats(path: Path) -> tuple[int, date | None, date | None]:
    cache = _read_cache(path)
    if not cache:
        return 0, None, None
    days = sorted(cache)
    return len(days), days[0], days[-1]
