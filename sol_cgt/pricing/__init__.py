"""AUD price provider with timestamp-aware pricing."""
from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

from .. import utils
from ..providers import fx_price_table, sol_price_table

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
STABLECOIN_MINTS = {USDC_MINT, USDT_MINT}
LOGGER = logging.getLogger(__name__)


class PriceCache:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._cache: dict[str, dict[int, Decimal]] = {}

    def _path(self, mint: str) -> Path:
        safe = mint.replace("/", "_")
        return self.root / f"{safe}.json"

    def _load(self, mint: str) -> dict[int, Decimal]:
        if mint in self._cache:
            return self._cache[mint]
        path = self._path(mint)
        if not path.exists():
            self._cache[mint] = {}
            return self._cache[mint]
        raw = utils.json_loads(path.read_text(encoding="utf-8"))
        data = {int(ts): Decimal(str(price)) for ts, price in raw.items()}
        self._cache[mint] = data
        return data

    def get(self, mint: str, bucket: int) -> Optional[Decimal]:
        data = self._load(mint)
        return data.get(bucket)

    def set(self, mint: str, bucket: int, price: Decimal) -> None:
        data = self._load(mint)
        data[bucket] = price
        path = self._path(mint)
        payload = {str(ts): str(value) for ts, value in data.items()}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(utils.json_dumps(payload), encoding="utf-8")


class TimestampPriceProvider:
    def __init__(self, *, api_key: Optional[str] = None, cache: Optional[PriceCache] = None) -> None:
        _ = api_key
        self.cache = cache or PriceCache(utils.ensure_cache_dir("prices", "tokens"))

    def price_usd(self, mint: str, ts: datetime) -> Optional[Decimal]:
        mint = normalize_mint(mint)
        if mint in STABLECOIN_MINTS:
            return Decimal("1")
        if mint == WSOL_MINT:
            date_local = utils.to_au_local(ts).date()
            return sol_price_table.get_sol_usd_close_for_date(date_local)
        bucket = _unix_minute_bucket(ts)
        cached = self.cache.get(mint, bucket)
        if cached is not None:
            return cached
        return None

    def prefetch(self, requests: Iterable[tuple[str, int]]) -> None:
        grouped: dict[str, list[int]] = {}
        for mint, bucket in requests:
            mint = normalize_mint(mint)
            if mint in STABLECOIN_MINTS:
                continue
            if mint == WSOL_MINT:
                continue
            if self.cache.get(mint, bucket) is not None:
                continue
            grouped.setdefault(mint, []).append(bucket)
        return


def _unix_minute_bucket(ts: datetime) -> int:
    return int(ts.timestamp()) // 60 * 60


def normalize_mint(mint: str) -> str:
    upper = mint.upper()
    if upper in {"SOL", "WSOL"} or mint == WSOL_MINT:
        return WSOL_MINT
    return mint


class AudPriceProvider:
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        fx_source: str = "frankfurter",
        usd_provider: Optional[TimestampPriceProvider] = None,
    ) -> None:
        _ = api_key
        self.fx_source = fx_source
        self.usd_provider = usd_provider or TimestampPriceProvider()

    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Optional[Decimal]:
        context = context or {}
        hints = context.get("price_aud")
        if isinstance(hints, dict) and mint in hints:
            return Decimal(str(hints[mint]))
        hint_key = f"{mint}_price_aud"
        if hint_key in context:
            return Decimal(str(context[hint_key]))
        if "price_usd" in context and context.get("mint") == mint:
            usd = Decimal(str(context["price_usd"]))
            return utils.quantize_aud(usd * self.fx_rate(ts))
        mint = normalize_mint(mint)
        bucket = ts.replace(second=0, microsecond=0)
        return self._cached_price(mint, bucket)

    @lru_cache(maxsize=4096)
    def _cached_price(self, mint: str, bucket: datetime) -> Optional[Decimal]:
        return self._price_aud_uncached(mint, bucket)

    def _price_aud_uncached(self, mint: str, ts: datetime) -> Optional[Decimal]:
        usd_price = self.price_usd(mint, ts)
        if usd_price is None:
            LOGGER.warning("Price not available for mint=%s at %s", mint, ts.isoformat())
            return None
        fx = self.fx_rate(ts)
        return utils.quantize_aud(usd_price * fx)

    def price_usd(self, mint: str, ts: datetime) -> Optional[Decimal]:
        return self.usd_provider.price_usd(mint, ts)

    def prefetch(self, requests: Iterable[tuple[str, int]]) -> None:
        self.usd_provider.prefetch(requests)

    def fx_rate(self, ts: datetime) -> Decimal:
        fx_day = utils.to_au_local(ts).date()
        rate = fx_price_table.get_usd_aud_for_date_or_prior(fx_day)
        if rate is None:
            raise RuntimeError(f"USD/AUD FX rate unavailable in local cache for {fx_day.isoformat()}")
        return rate
