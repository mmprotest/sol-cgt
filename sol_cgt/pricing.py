"""AUD price provider with free fallbacks."""
from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from typing import Optional

from . import utils
from .providers import birdeye, coingecko, fx_rates, jupiter, rba_fx

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
STABLECOIN_MINTS = {USDC_MINT, USDT_MINT}


class AudPriceProvider:
    def __init__(self, *, api_key: Optional[str] = None, fx_source: str = "frankfurter") -> None:
        self.api_key = api_key
        self.fx_source = fx_source

    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Decimal:
        context = context or {}
        hints = context.get("price_aud")
        if isinstance(hints, dict) and mint in hints:
            return Decimal(str(hints[mint]))
        hint_key = f"{mint}_price_aud"
        if hint_key in context:
            return Decimal(str(context[hint_key]))
        if "price_usd" in context and context.get("mint") == mint:
            usd = Decimal(str(context["price_usd"]))
            return utils.quantize_aud(usd * self._fx_rate(ts))
        mint = self._normalize_mint(mint)
        bucket = ts.replace(second=0, microsecond=0)
        return self._cached_price(mint, bucket)

    def _normalize_mint(self, mint: str) -> str:
        if mint.upper() == "SOL":
            return WSOL_MINT
        return mint

    @lru_cache(maxsize=4096)
    def _cached_price(self, mint: str, bucket: datetime) -> Decimal:
        return self._price_aud_uncached(mint, bucket)

    def _price_aud_uncached(self, mint: str, ts: datetime) -> Decimal:
        usd_price = self._price_usd(mint, ts)
        fx = self._fx_rate(ts)
        return utils.quantize_aud(usd_price * fx)

    def _price_usd(self, mint: str, ts: datetime) -> Decimal:
        if mint in STABLECOIN_MINTS:
            return Decimal("1")
        if mint == WSOL_MINT:
            price = self._run_async(coingecko.sol_price_usd(ts))
            if price is not None:
                return price
        try:
            price = self._run_async(jupiter.price_usd(mint, ts))
            if price is not None:
                return price
        except jupiter.PriceLookupError:
            pass
        if self.api_key:
            try:
                return self._run_async(birdeye.historical_price_usd(mint, ts, api_key=self.api_key))
            except Exception:
                pass
        raise birdeye.PriceLookupError(mint, ts, message="Price not available from available sources")

    def _fx_rate(self, ts: datetime) -> Decimal:
        fx_day = utils.to_au_local(ts).date()
        try:
            if self.fx_source == "rba":
                return rba_fx.usd_to_aud_rate(fx_day)
            return self._run_async(fx_rates.usd_to_aud_rate(fx_day))
        except Exception:
            return rba_fx.usd_to_aud_rate(fx_day)

    def _run_async(self, coro):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        result_container: dict[str, Decimal] = {}
        error_container: dict[str, BaseException] = {}

        def runner() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result_container["result"] = loop.run_until_complete(coro)
            except BaseException as exc:  # pragma: no cover - defensive
                error_container["error"] = exc
            finally:
                loop.close()

        thread = threading.Thread(target=runner)
        thread.start()
        thread.join()
        if "error" in error_container:
            raise error_container["error"]
        return result_container["result"]
