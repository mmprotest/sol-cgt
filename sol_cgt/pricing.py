"""AUD price provider using Birdeye and RBA FX."""
from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from typing import Optional

from . import utils
from .providers import birdeye
from .providers import rba_fx

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
STABLECOIN_MINTS = {USDC_MINT, USDT_MINT}


class AudPriceProvider:
    def __init__(self, *, api_key: Optional[str] = None) -> None:
        self.api_key = api_key

    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Decimal:
        context = context or {}
        hints = context.get("price_aud")
        if isinstance(hints, dict) and mint in hints:
            return Decimal(str(hints[mint]))
        hint_key = f"{mint}_price_aud"
        if hint_key in context:
            return Decimal(str(context[hint_key]))
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
        try:
            usd_price = self._run_async(birdeye.historical_price_usd(mint, ts, api_key=self.api_key))
        except RuntimeError as exc:
            if mint in STABLECOIN_MINTS:
                usd_price = Decimal("1")
            else:
                raise exc
        except birdeye.PriceLookupError:
            if mint in STABLECOIN_MINTS:
                usd_price = Decimal("1")
            else:
                raise
        fx = rba_fx.usd_to_aud_rate(ts.date())
        return utils.quantize_aud(usd_price * fx)

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
