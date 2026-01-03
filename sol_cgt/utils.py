"""Utility helpers shared across modules."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence
from zoneinfo import ZoneInfo

try:  # pragma: no cover - optional dependency for speed
    import orjson
except Exception:  # pragma: no cover - fallback during tests
    orjson = None  # type: ignore


CACHE_ROOT = Path("./cache")
AU_TZ = ZoneInfo("Australia/Melbourne")


def ensure_cache_dir(*parts: str) -> Path:
    """Return a cache directory ensuring it exists."""

    path = CACHE_ROOT.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def json_dumps(data: Any) -> str:
    if orjson is not None:  # pragma: no cover - executed when orjson available
        return orjson.dumps(data).decode("utf-8")
    return json.dumps(data, default=str)


def json_loads(data: str) -> Any:
    if orjson is not None:  # pragma: no cover
        return orjson.loads(data)
    return json.loads(data)


def sha1_digest(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def write_jsonl(path: Path, records: Iterable[Any], *, mode: str = "a") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode, encoding="utf-8") as fh:
        for item in records:
            fh.write(json_dumps(item))
            fh.write("\n")


def read_jsonl(path: Path) -> Iterator[Any]:
    if not path.exists():
        return iter(())
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json_loads(line)


@dataclass(frozen=True)
class Period:
    start: datetime
    end: datetime


def australian_financial_year_bounds(label: str) -> Period:
    """Return the UTC bounds for a financial year label ``YYYY-YYYY`` in AU time."""

    parts = label.split("-")
    if len(parts) != 2:
        raise ValueError("Financial year label must be in 'YYYY-YYYY' format")
    start_year = int(parts[0])
    end_year = int(parts[1])
    if end_year != start_year + 1:
        raise ValueError("Financial year end must be start + 1 year")
    start_local = datetime(start_year, 7, 1, tzinfo=AU_TZ)
    end_local = datetime(end_year, 6, 30, 23, 59, 59, tzinfo=AU_TZ)
    return Period(start=start_local.astimezone(timezone.utc), end=end_local.astimezone(timezone.utc))


def parse_local_date(value: str) -> datetime:
    """Parse a YYYY-MM-DD date into AU local time."""
    day = datetime.strptime(value, "%Y-%m-%d")
    return day.replace(tzinfo=AU_TZ)


def to_au_local(ts: datetime) -> datetime:
    """Convert a timestamp to Australia/Melbourne."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(AU_TZ)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def quantize_aud(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def chunked(seq: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]


def holding_period_days(acquired: datetime, disposed: datetime) -> int:
    return (disposed - acquired).days


def end_of_day_utc(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
