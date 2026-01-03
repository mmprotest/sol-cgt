"""Normalization of raw enhanced transactions to ``NormalizedEvent`` objects."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, List, Optional

from .. import utils
from ..providers import birdeye
from ..providers import rba_fx
from ..pricing import AudPriceProvider, STABLECOIN_MINTS, WSOL_MINT
from ..types import NormalizedEvent, TokenAmount

LAMPORTS_PER_SOL = Decimal("1000000000")
SOL_MINT = "SOL"


def _lamports_to_sol(value: int | float | Decimal) -> Decimal:
    return (Decimal(str(value)) / LAMPORTS_PER_SOL).quantize(Decimal("0.000000001"))


def _amount_raw_from_decimal(amount: Decimal, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int((amount * scale).to_integral_value())


async def _metadata_for_mint(mint: str, cache: dict[str, tuple[Optional[str], Optional[int]]]) -> tuple[Optional[str], Optional[int]]:
    if mint in cache:
        return cache[mint]
    symbol, decimals = await birdeye.token_metadata(mint)
    cache[mint] = (symbol, decimals)
    return symbol, decimals


async def _build_token_amount(
    payload: Optional[dict],
    *,
    fallback_mint: Optional[str] = None,
    metadata_cache: dict[str, tuple[Optional[str], Optional[int]]],
) -> Optional[TokenAmount]:
    if not payload and not fallback_mint:
        return None
    mint = payload.get("mint") if payload else None
    mint = mint or fallback_mint
    if not mint:
        return None
    amount_raw = payload.get("amount_raw") if payload else None
    decimals = payload.get("decimals") if payload else None
    symbol = payload.get("symbol") if payload else None
    amount = payload.get("amount") if payload else None

    if decimals is None or symbol is None:
        meta_symbol, meta_decimals = await _metadata_for_mint(mint, metadata_cache)
        symbol = symbol or meta_symbol
        decimals = decimals if decimals is not None else meta_decimals
    if amount_raw is None and amount is not None and decimals is not None:
        amount_raw = _amount_raw_from_decimal(Decimal(str(amount)), int(decimals))
    if decimals is None:
        decimals = 0
    if amount_raw is None:
        amount_raw = 0
    return TokenAmount(mint=mint, symbol=symbol, decimals=int(decimals), amount_raw=int(amount_raw))


def _parse_timestamp(value: object) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        ts = datetime.fromisoformat(value)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    return datetime.now(timezone.utc)


def _transfer_matches_wallet(entry: dict, wallet: str) -> tuple[bool, Optional[str]]:
    sender = entry.get("fromUserAccount") or entry.get("source")
    receiver = entry.get("toUserAccount") or entry.get("destination")
    if sender == wallet:
        return True, "out"
    if receiver == wallet:
        return True, "in"
    return False, None


def _swap_token_payload(entry: dict) -> dict:
    payload = {
        "mint": entry.get("mint"),
        "symbol": entry.get("symbol"),
        "decimals": entry.get("decimals"),
    }
    if "rawTokenAmount" in entry and isinstance(entry["rawTokenAmount"], dict):
        raw_info = entry["rawTokenAmount"]
        payload["amount_raw"] = raw_info.get("tokenAmount")
        payload["decimals"] = raw_info.get("decimals", payload.get("decimals"))
    if payload.get("amount_raw") is None:
        for key in ("amount", "tokenAmount", "amountRaw"):
            if key in entry:
                payload["amount"] = entry[key]
                break
    return payload


def _native_amount_payload(entry: dict) -> dict:
    amount = entry.get("amount") or entry.get("lamports")
    return {
        "mint": SOL_MINT,
        "symbol": "SOL",
        "decimals": 9,
        "amount_raw": int(amount) if amount is not None else 0,
    }


def _is_stablecoin(mint: str) -> bool:
    return mint in STABLECOIN_MINTS


def _is_sol(mint: str) -> bool:
    return mint in {SOL_MINT, WSOL_MINT}


async def normalize_wallet_events(wallet: str, raw_txs: Iterable[dict]) -> List[NormalizedEvent]:
    events: List[NormalizedEvent] = []
    metadata_cache: dict[str, tuple[Optional[str], Optional[int]]] = {}
    price_provider = AudPriceProvider()

    for tx in raw_txs:
        if tx.get("transactionError"):
            continue
        signature = tx.get("signature") or tx.get("id") or "unknown"
        ts = _parse_timestamp(tx.get("timestamp") or tx.get("blockTime"))
        fee_lamports = tx.get("fee") or tx.get("nativeFee") or 0
        fee_sol = _lamports_to_sol(fee_lamports)

        swap_event = None
        events_payload = tx.get("events") or {}
        if isinstance(events_payload, dict):
            swap_event = events_payload.get("swap")

        if swap_event:
            token_inputs = swap_event.get("tokenInputs") or []
            token_outputs = swap_event.get("tokenOutputs") or []
            native_input = swap_event.get("nativeInput")
            native_output = swap_event.get("nativeOutput")

            base_payload = None
            quote_payload = None
            if token_inputs:
                base_payload = _swap_token_payload(token_inputs[0])
            elif native_input:
                base_payload = _native_amount_payload(native_input)

            if token_outputs:
                quote_payload = _swap_token_payload(token_outputs[0])
            elif native_output:
                quote_payload = _native_amount_payload(native_output)

            base_token = await _build_token_amount(base_payload, metadata_cache=metadata_cache)
            quote_token = await _build_token_amount(quote_payload, metadata_cache=metadata_cache)
            raw = {"swap": swap_event}

            proceeds_aud = None
            if base_token and quote_token:
                if _is_stablecoin(base_token.mint):
                    fx = rba_fx.usd_to_aud_rate(ts.date())
                    proceeds_aud = utils.quantize_aud(base_token.amount * fx)
                elif _is_stablecoin(quote_token.mint):
                    fx = rba_fx.usd_to_aud_rate(ts.date())
                    proceeds_aud = utils.quantize_aud(quote_token.amount * fx)
                elif _is_sol(base_token.mint):
                    proceeds_aud = utils.quantize_aud(base_token.amount * price_provider.price_aud("SOL", ts, context=raw))
                elif _is_sol(quote_token.mint):
                    proceeds_aud = utils.quantize_aud(quote_token.amount * price_provider.price_aud("SOL", ts, context=raw))
            if proceeds_aud is not None:
                raw["proceeds_aud"] = str(proceeds_aud)

            events.append(
                NormalizedEvent(
                    id=f"{signature}#swap",
                    ts=ts,
                    kind="swap",
                    base_token=base_token,
                    quote_token=quote_token,
                    fee_sol=fee_sol,
                    wallet=wallet,
                    counterparty=swap_event.get("marketplace") if isinstance(swap_event, dict) else None,
                    raw=raw,
                    tags=set(),
                )
            )

        for idx, transfer in enumerate(tx.get("tokenTransfers") or []):
            matches, direction = _transfer_matches_wallet(transfer, wallet)
            if not matches:
                continue
            payload = {
                "mint": transfer.get("mint"),
                "symbol": transfer.get("tokenSymbol"),
                "decimals": transfer.get("tokenDecimals"),
                "amount": transfer.get("tokenAmount"),
            }
            token = await _build_token_amount(payload, metadata_cache=metadata_cache)
            kind = "transfer_in" if direction == "in" else "transfer_out"
            base_token = token if kind == "transfer_out" else None
            quote_token = token if kind == "transfer_in" else None
            counterparty = transfer.get("fromUserAccount") if kind == "transfer_in" else transfer.get("toUserAccount")
            events.append(
                NormalizedEvent(
                    id=f"{signature}#tok{idx}",
                    ts=ts,
                    kind=kind,
                    base_token=base_token,
                    quote_token=quote_token,
                    fee_sol=fee_sol,
                    wallet=wallet,
                    counterparty=counterparty,
                    raw={"transfer": transfer},
                    tags=set(),
                )
            )

        for idx, transfer in enumerate(tx.get("nativeTransfers") or []):
            matches, direction = _transfer_matches_wallet(transfer, wallet)
            if not matches:
                continue
            token = await _build_token_amount(_native_amount_payload(transfer), metadata_cache=metadata_cache)
            kind = "transfer_in" if direction == "in" else "transfer_out"
            base_token = token if kind == "transfer_out" else None
            quote_token = token if kind == "transfer_in" else None
            counterparty = transfer.get("fromUserAccount") if kind == "transfer_in" else transfer.get("toUserAccount")
            events.append(
                NormalizedEvent(
                    id=f"{signature}#sol{idx}",
                    ts=ts,
                    kind=kind,
                    base_token=base_token,
                    quote_token=quote_token,
                    fee_sol=fee_sol,
                    wallet=wallet,
                    counterparty=counterparty,
                    raw={"native_transfer": transfer},
                    tags=set(),
                )
            )

    events.sort(key=lambda ev: (ev.ts, ev.id))
    return events
