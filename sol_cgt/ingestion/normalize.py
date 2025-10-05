"""Normalization of raw enhanced transactions to ``NormalizedEvent`` objects."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, List, Optional

from ..types import NormalizedEvent, TokenAmount
from ..providers import birdeye

LAMPORTS_PER_SOL = Decimal("1000000000")


async def _build_token_amount(payload: Optional[dict]) -> Optional[TokenAmount]:
    if not payload:
        return None
    mint = payload.get("mint")
    if not mint:
        return None
    amount_raw = int(payload.get("amount_raw", 0))
    decimals = payload.get("decimals")
    symbol = payload.get("symbol")
    if decimals is None or symbol is None:
        metadata = await birdeye.symbol_decimals(mint)
        if metadata:
            meta_symbol, meta_decimals = metadata
            symbol = symbol or meta_symbol
            decimals = decimals if decimals is not None else meta_decimals
    decimals = int(decimals or 0)
    return TokenAmount(mint=mint, symbol=symbol, decimals=decimals, amount_raw=amount_raw)


def _lamports_to_sol(value: int | float | Decimal) -> Decimal:
    return (Decimal(str(value)) / LAMPORTS_PER_SOL).quantize(Decimal("0.000000001"))


async def normalize_wallet_events(wallet: str, raw_txs: Iterable[dict]) -> List[NormalizedEvent]:
    events: List[NormalizedEvent] = []
    for tx in raw_txs:
        signature = tx.get("signature") or tx.get("id") or "unknown"
        block_time = tx.get("timestamp") or tx.get("blockTime")
        if isinstance(block_time, (int, float)):
            ts = datetime.fromtimestamp(block_time, tz=timezone.utc)
        elif isinstance(block_time, str):
            ts = datetime.fromisoformat(block_time)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)
        entries = tx.get("events") or tx.get("actions") or []
        for idx, entry in enumerate(entries):
            kind = entry.get("type", "unknown")
            base_token = await _build_token_amount(entry.get("base"))
            quote_token = await _build_token_amount(entry.get("quote"))
            fee_sol = Decimal("0")
            if "fee_lamports" in entry:
                fee_sol = _lamports_to_sol(entry["fee_lamports"])
            elif "fee_sol" in entry:
                fee_sol = Decimal(str(entry["fee_sol"]))
            event_id = f"{signature}#{idx}"
            tags = set(entry.get("tags") or [])
            counterparty = entry.get("counterparty")
            events.append(
                NormalizedEvent(
                    id=event_id,
                    ts=ts,
                    kind=kind,
                    base_token=base_token,
                    quote_token=quote_token,
                    fee_sol=fee_sol,
                    wallet=wallet,
                    counterparty=counterparty,
                    raw=entry,
                    tags=tags,
                )
            )
    events.sort(key=lambda ev: (ev.ts, ev.id))
    return events
