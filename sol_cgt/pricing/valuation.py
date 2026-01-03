"""Event valuation helpers for timestamp-aware pricing."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Callable, Iterable, Optional

from .. import utils
from ..types import NormalizedEvent, WarningRecord
from . import STABLECOIN_MINTS, TimestampPriceProvider, WSOL_MINT, normalize_mint

SOL_MINT = "SOL"


@dataclass(frozen=True)
class ValuationResult:
    value_usd: Optional[Decimal]
    value_aud: Optional[Decimal]
    price_source: str
    notes: str


@dataclass
class ValuationContext:
    usd_provider: TimestampPriceProvider
    fx_rate: Callable[[datetime], Decimal]
    warnings: list[WarningRecord] = field(default_factory=list)
    _warned: set[tuple[str, str]] = field(default_factory=set)

    def warn(self, event: NormalizedEvent, code: str, message: str) -> None:
        signature = event.raw.get("signature")
        key = (signature or event.id, code)
        if key in self._warned:
            return
        self._warned.add(key)
        self.warnings.append(
            WarningRecord(
                ts=event.ts,
                wallet=event.wallet,
                signature=signature,
                code=code,
                message=message,
            )
        )


def valuate_events(events: Iterable[NormalizedEvent], ctx: ValuationContext) -> list[WarningRecord]:
    events_list = list(events)
    _prefetch_prices(events_list, ctx)
    swap_map = _swap_valuation_map(events_list, ctx)
    for event in events_list:
        if event.id in swap_map:
            result = swap_map[event.id]
        else:
            result = valuate_event(event, ctx)
        _attach_result(event, result, ctx)
    return ctx.warnings


def valuate_event(event: NormalizedEvent, ctx: ValuationContext) -> ValuationResult:
    if event.kind == "swap":
        return ValuationResult(None, None, "tx-implied", "swap valuation handled by group")
    token = event.base_token or event.quote_token
    if token is None:
        return ValuationResult(None, None, "none", "no token to value")
    if event.base_token is not None and (
        "proceeds_hint_aud" in event.raw or "proceeds_hint_usd" in event.raw
    ):
        return ValuationResult(None, None, "hint", "existing proceeds hint")
    if event.quote_token is not None and (
        "cost_hint_aud" in event.raw or "cost_hint_usd" in event.raw
    ):
        return ValuationResult(None, None, "hint", "existing cost hint")
    usd_price = ctx.usd_provider.price_usd(token.mint, event.ts)
    if usd_price is None:
        ctx.warn(
            event,
            "missing_price",
            f"Price not available for mint={token.mint} at {event.ts.isoformat()}",
        )
        _mark_unpriced(event)
        return ValuationResult(None, None, "unpriced", "missing timestamp price")
    value_usd = usd_price * token.amount
    fx = ctx.fx_rate(event.ts)
    value_aud = utils.quantize_aud(value_usd * fx)
    if event.base_token is not None:
        event.raw.setdefault("proceeds_hint_usd", str(value_usd))
        event.raw.setdefault("proceeds_hint_aud", str(value_aud))
    else:
        event.raw.setdefault("cost_hint_usd", str(value_usd))
        event.raw.setdefault("cost_hint_aud", str(value_aud))
    return ValuationResult(value_usd, value_aud, "timestamp", "spot pricing")


def _attach_result(event: NormalizedEvent, result: ValuationResult, ctx: ValuationContext) -> None:
    event.raw["valuation_source"] = result.price_source
    if result.notes:
        event.raw["valuation_notes"] = result.notes
    if result.value_aud is None and result.value_usd is None:
        return
    fx = ctx.fx_rate(event.ts)
    value_usd = result.value_usd
    value_aud = result.value_aud
    if value_usd is not None and value_aud is None:
        value_aud = utils.quantize_aud(value_usd * fx)
    if value_usd is None and value_aud is not None:
        value_usd = (value_aud / fx).quantize(Decimal("0.00000001")) if fx else None
    if event.base_token is not None:
        event.raw.setdefault("proceeds_hint_usd", str(value_usd))
        event.raw.setdefault("proceeds_hint_aud", str(value_aud))
    elif event.quote_token is not None:
        event.raw.setdefault("cost_hint_usd", str(value_usd))
        event.raw.setdefault("cost_hint_aud", str(value_aud))


def _prefetch_prices(events: list[NormalizedEvent], ctx: ValuationContext) -> None:
    requests: set[tuple[str, int]] = set()
    for event in events:
        if event.kind == "swap":
            swap_legs = event.raw.get("swap_legs")
            if isinstance(swap_legs, list):
                for leg in swap_legs:
                    mint = leg.get("mint")
                    if mint and _is_sol(mint):
                        requests.add((mint, _unix_minute_bucket(event.ts)))
            continue
        token = event.base_token or event.quote_token
        if token is None:
            continue
        if _is_stable(token.mint):
            continue
        requests.add((token.mint, _unix_minute_bucket(event.ts)))
    ctx.usd_provider.prefetch(requests)


def _swap_valuation_map(
    events: list[NormalizedEvent],
    ctx: ValuationContext,
) -> dict[str, ValuationResult]:
    grouped: dict[tuple[str, str], list[NormalizedEvent]] = {}
    for event in events:
        if event.kind != "swap":
            continue
        signature = event.raw.get("signature") or event.id
        grouped.setdefault((signature, event.wallet), []).append(event)

    results: dict[str, ValuationResult] = {}
    for (signature, _wallet), group in grouped.items():
        swap_legs = group[0].raw.get("swap_legs") if group else None
        if not isinstance(swap_legs, list):
            continue
        ins, outs = _swap_deltas(swap_legs)
        total_consideration, price_source, notes = _swap_anchor_value(ins, outs, group[0].ts, ctx)
        if total_consideration is None:
            for event in group:
                ctx.warn(
                    event,
                    "missing_swap_price",
                    f"Swap pricing unavailable for signature={signature}",
                )
                _mark_unpriced(event)
                results[event.id] = ValuationResult(None, None, "unpriced", "missing swap anchor")
            continue
        sol_price = ctx.usd_provider.price_usd(SOL_MINT, group[0].ts)
        proceeds_alloc = _allocate_by_weight(outs, total_consideration, sol_price)
        cost_alloc = _allocate_by_weight(ins, total_consideration, sol_price)
        for event in group:
            mint = None
            direction = event.raw.get("swap_direction")
            if event.base_token is not None:
                mint = event.base_token.mint
            elif event.quote_token is not None:
                mint = event.quote_token.mint
            if not mint:
                continue
            if direction == "out":
                value_usd = proceeds_alloc.get(mint)
                results[event.id] = ValuationResult(value_usd, None, price_source, notes)
            else:
                value_usd = cost_alloc.get(mint)
                results[event.id] = ValuationResult(value_usd, None, price_source, notes)
    return results


def _swap_deltas(swap_legs: list[dict]) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    ins: dict[str, Decimal] = {}
    outs: dict[str, Decimal] = {}
    for leg in swap_legs:
        mint = leg.get("mint")
        amount = leg.get("amount")
        direction = leg.get("direction")
        if not mint or amount is None:
            continue
        qty = Decimal(str(amount))
        if direction == "in":
            ins[mint] = ins.get(mint, Decimal("0")) + qty
        elif direction == "out":
            outs[mint] = outs.get(mint, Decimal("0")) + qty
    return ins, outs


def _swap_anchor_value(
    ins: dict[str, Decimal],
    outs: dict[str, Decimal],
    ts: datetime,
    ctx: ValuationContext,
) -> tuple[Optional[Decimal], str, str]:
    stable_in = {mint: amt for mint, amt in ins.items() if _is_stable(mint)}
    stable_out = {mint: amt for mint, amt in outs.items() if _is_stable(mint)}
    sol_in_qty = sum((amt for mint, amt in ins.items() if _is_sol(mint)), Decimal("0"))
    sol_out_qty = sum((amt for mint, amt in outs.items() if _is_sol(mint)), Decimal("0"))

    if stable_in:
        total = sum(stable_in.values(), Decimal("0"))
        return total, "tx-implied:stable-in", "stablecoin received"
    if sol_in_qty > 0:
        sol_price = ctx.usd_provider.price_usd(SOL_MINT, ts)
        if sol_price is None:
            return None, "tx-implied:sol-in", "missing SOL/USD anchor"
        return sol_in_qty * sol_price, "tx-implied:sol-in", "SOL received"
    if stable_out:
        total = sum(stable_out.values(), Decimal("0"))
        return total, "tx-implied:stable-out", "stablecoin spent"
    if sol_out_qty > 0:
        sol_price = ctx.usd_provider.price_usd(SOL_MINT, ts)
        if sol_price is None:
            return None, "tx-implied:sol-out", "missing SOL/USD anchor"
        return sol_out_qty * sol_price, "tx-implied:sol-out", "SOL spent"
    fallback_mint = next(iter(outs.keys() or ins.keys()), None)
    if fallback_mint:
        price = ctx.usd_provider.price_usd(fallback_mint, ts)
        if price is not None:
            qty = (outs or ins).get(fallback_mint, Decimal("0"))
            return qty * price, "timestamp:fallback", f"fallback price for {fallback_mint}"
    return None, "unpriced", "no anchor found"


def _allocate_by_weight(
    amounts: dict[str, Decimal],
    total_value: Decimal,
    sol_price: Optional[Decimal],
) -> dict[str, Decimal]:
    if not amounts:
        return {}
    weights: dict[str, Decimal] = {}
    for mint, amount in amounts.items():
        if _is_stable(mint):
            weights[mint] = amount
        elif _is_sol(mint) and sol_price is not None:
            weights[mint] = amount * sol_price
        else:
            weights[mint] = amount
    total_weight = sum(weights.values(), Decimal("0"))
    if total_weight == 0:
        return {mint: Decimal("0") for mint in amounts}
    return {mint: total_value * (weight / total_weight) for mint, weight in weights.items()}


def _is_stable(mint: str) -> bool:
    return normalize_mint(mint) in STABLECOIN_MINTS


def _is_sol(mint: str) -> bool:
    normalized = normalize_mint(mint)
    return normalized == WSOL_MINT or mint == SOL_MINT


def _unix_minute_bucket(ts: datetime) -> int:
    return int(ts.timestamp()) // 60 * 60


def _mark_unpriced(event: NormalizedEvent) -> None:
    event.raw["unpriced"] = True
    event.tags.add("unpriced")
