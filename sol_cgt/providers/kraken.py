"""Kraken SOL/USD daily OHLC pricing with caching."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import logging
from pathlib import Path
from typing import Any, Optional

import httpx

from .. import utils

BASE_URL = "https://api.kraken.com/0/public"
LOGGER = logging.getLogger(__name__)
_PAIR_CACHE: Optional[str] = None


def _cache_path() -> Path:
    return utils.ensure_cache_dir("prices") / "kraken_solusd_daily.json"


def _load_cache() -> dict[str, str]:
    path = _cache_path()
    if not path.exists():
        return {}
    data = utils.json_loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return {str(key): str(value) for key, value in data.items()}
    return {}


def _save_cache(data: dict[str, str]) -> None:
    path = _cache_path()
    path.write_text(utils.json_dumps(data), encoding="utf-8")


async def _perform_request(client: httpx.AsyncClient, path: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    response = await client.get(url, params=params, headers={"accept": "application/json"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Kraken response payload")
    return payload


def _extract_ohlc_result(payload: dict[str, Any]) -> tuple[Optional[str], list[list[Any]]]:
    errors = payload.get("error") or []
    if errors:
        return None, []
    result = payload.get("result")
    if not isinstance(result, dict):
        return None, []
    for key, value in result.items():
        if key == "last":
            continue
        if isinstance(value, list) and value:
            return str(key), value
    return None, []


def _parse_ohlc_entries(entries: list[list[Any]]) -> dict[str, Decimal]:
    mapping: dict[str, Decimal] = {}
    for row in entries:
        if not isinstance(row, list) or len(row) < 5:
            continue
        ts_value = row[0]
        close_value = row[4]
        if ts_value is None or close_value is None:
            continue
        try:
            ts = datetime.fromtimestamp(int(ts_value), tz=timezone.utc)
        except Exception:
            continue
        day = utils.to_au_local(ts).date().isoformat()
        mapping[day] = Decimal(str(close_value))
    return mapping


def _fy_bounds_for_date(day: date) -> tuple[date, date]:
    if day.month >= 7:
        start_year = day.year
        end_year = day.year + 1
    else:
        start_year = day.year - 1
        end_year = day.year
    return date(start_year, 7, 1), date(end_year, 6, 30)


async def _resolve_pair_key(client: httpx.AsyncClient) -> Optional[str]:
    for query in ("SOLUSD", "SOL/USD"):
        payload = await _perform_request(client, "/AssetPairs", {"pair": query})
        result = payload.get("result")
        if isinstance(result, dict) and result:
            return next(iter(result.keys()))
    return None


async def _fetch_ohlc_for_range(fy_start_date_local: date, fy_end_date_local: date) -> dict[str, Decimal]:
    global _PAIR_CACHE
    start_dt = datetime(
        fy_start_date_local.year,
        fy_start_date_local.month,
        fy_start_date_local.day,
        tzinfo=utils.AU_TZ,
    ) - timedelta(days=7)
    since = int(start_dt.timestamp())
    async with httpx.AsyncClient(timeout=20.0) as client:
        pair = _PAIR_CACHE or "SOLUSD"
        payload = await _perform_request(client, "/OHLC", {"pair": pair, "interval": 1440, "since": since})
        pair_key, entries = _extract_ohlc_result(payload)
        if pair_key is None or not entries:
            resolved = await _resolve_pair_key(client)
            if not resolved:
                return {}
            payload = await _perform_request(client, "/OHLC", {"pair": resolved, "interval": 1440, "since": since})
            pair_key, entries = _extract_ohlc_result(payload)
            if pair_key is None or not entries:
                return {}
        _PAIR_CACHE = pair_key
    return _parse_ohlc_entries(entries)


def _missing_dates(cache: dict[str, str], start: date, end: date) -> bool:
    day = start
    while day <= end:
        if day.isoformat() not in cache:
            return True
        day += timedelta(days=1)
    return False


async def warm_sol_usd_closes(fy_start_date_local: date, fy_end_date_local: date) -> None:
    cache = _load_cache()
    if not _missing_dates(cache, fy_start_date_local, fy_end_date_local):
        return
    data = await _fetch_ohlc_for_range(fy_start_date_local, fy_end_date_local)
    if not data:
        LOGGER.warning("Kraken OHLC returned no SOL/USD data for %s-%s", fy_start_date_local, fy_end_date_local)
        return
    for key, value in data.items():
        cache[key] = str(value)
    _save_cache(cache)


async def get_sol_usd_close_for_date(date_local: date) -> Optional[Decimal]:
    cache = _load_cache()
    key = date_local.isoformat()
    if key not in cache:
        fy_start, fy_end = _fy_bounds_for_date(date_local)
        await warm_sol_usd_closes(fy_start, fy_end)
        cache = _load_cache()
    value = cache.get(key)
    if value is None:
        return None
    return Decimal(str(value))
