"""RBA AUD/USD FX rates with caching."""
from __future__ import annotations

import csv
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict

import httpx

from .. import utils

RBA_CSV_URL = "https://www.rba.gov.au/statistics/tables/csv/f11-data.csv"
CACHE_FILE = utils.ensure_cache_dir("fx") / "rba_f11_audusd.json"


def _load_cached_table() -> Dict[date, Decimal]:
    if not CACHE_FILE.exists():
        return {}
    payload = utils.json_loads(CACHE_FILE.read_text(encoding="utf-8"))
    table: Dict[date, Decimal] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            try:
                day = datetime.strptime(key, "%Y-%m-%d").date()
                table[day] = Decimal(str(value))
            except Exception:
                continue
    return table


def _write_cached_table(table: Dict[date, Decimal]) -> None:
    payload = {day.isoformat(): str(rate) for day, rate in table.items()}
    CACHE_FILE.write_text(utils.json_dumps(payload), encoding="utf-8")


def _parse_rba_csv(text: str) -> Dict[date, Decimal]:
    reader = csv.reader(text.splitlines())
    description_row = None
    header_row = None
    rows = []
    for row in reader:
        if not row:
            continue
        rows.append(row)
        if row[0].strip().lower() in {"series description", "description"}:
            description_row = row
        if row[0].strip().lower() == "date":
            header_row = row
            break
    if not header_row:
        raise RuntimeError("Unable to locate header row in RBA CSV")
    if not description_row:
        for row in rows:
            if row[0].strip().lower() in {"series description", "description"}:
                description_row = row
                break
    if not description_row:
        raise RuntimeError("Unable to locate description row in RBA CSV")

    target_index = None
    for idx, value in enumerate(description_row):
        if "aud/usd" in value.lower():
            target_index = idx
            break
    if target_index is None:
        raise RuntimeError("AUD/USD Exchange Rate series not found")

    table: Dict[date, Decimal] = {}
    for row in reader:
        if not row or len(row) <= target_index:
            continue
        if row[0].strip().lower() == "date":
            continue
        try:
            day = datetime.strptime(row[0].strip(), "%Y-%m-%d").date()
        except ValueError:
            continue
        value = row[target_index].strip()
        if not value:
            continue
        try:
            rate = Decimal(value)
        except Exception:
            continue
        table[day] = rate
    if not table:
        raise RuntimeError("No AUD/USD data parsed from RBA CSV")
    return table


def _ensure_table() -> Dict[date, Decimal]:
    table = _load_cached_table()
    if table:
        return table
    response = httpx.get(RBA_CSV_URL, timeout=30.0)
    response.raise_for_status()
    table = _parse_rba_csv(response.text)
    _write_cached_table(table)
    return table


def usd_to_aud_rate(day: date) -> Decimal:
    """Return USD->AUD rate for the given day, using prior business day if needed."""

    table = _ensure_table()
    if day in table:
        audusd = table[day]
    else:
        prior_days = [d for d in table.keys() if d <= day]
        if not prior_days:
            raise RuntimeError("No RBA FX data available for requested date")
        closest = max(prior_days)
        audusd = table[closest]
    if audusd == 0:
        raise RuntimeError("AUD/USD rate is zero")
    return (Decimal("1") / audusd).quantize(Decimal("0.00000001"))
