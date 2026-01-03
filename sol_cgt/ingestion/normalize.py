"""Normalization of raw enhanced transactions to ``NormalizedEvent`` objects."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, List, Optional

from .. import utils
from ..providers import birdeye
from ..types import NormalizedEvent, TokenAmount

LAMPORTS_PER_SOL = Decimal("1000000000")
SOL_MINT = "SOL"


@dataclass(frozen=True)
class SwapLeg:
    token: TokenAmount
    direction: str  # "in" or "out"


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


def _fee_payer_address(tx: dict) -> Optional[str]:
    for key in ("feePayer", "feePayerAccount", "feePayerAddress"):
        value = tx.get(key)
        if isinstance(value, str) and value:
            return value
    account_data = tx.get("accountData")
    if isinstance(account_data, list):
        for entry in account_data:
            if isinstance(entry, dict):
                fee_payer = entry.get("feePayer")
                if isinstance(fee_payer, str) and fee_payer:
                    return fee_payer
    return None


def _consume_swap_leg(
    swap_legs: dict[tuple[str, str], List[Decimal]],
    mint: str,
    direction: str,
    amount: Decimal,
    *,
    tolerance: Decimal = Decimal("0.00000001"),
) -> bool:
    key = (mint, direction)
    values = swap_legs.get(key)
    if not values:
        return False
    for idx, candidate in enumerate(values):
        if (candidate - amount).copy_abs() <= tolerance:
            values.pop(idx)
            return True
    return False


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


async def _build_swap_legs(
    swap_event: dict,
    *,
    metadata_cache: dict[str, tuple[Optional[str], Optional[int]]],
) -> List[SwapLeg]:
    legs: List[SwapLeg] = []
    for entry in swap_event.get("tokenInputs") or []:
        token = await _build_token_amount(
            _swap_token_payload(entry),
            metadata_cache=metadata_cache,
        )
        if token is not None and token.amount > 0:
            legs.append(SwapLeg(token=token, direction="out"))
    for entry in swap_event.get("tokenOutputs") or []:
        token = await _build_token_amount(
            _swap_token_payload(entry),
            metadata_cache=metadata_cache,
        )
        if token is not None and token.amount > 0:
            legs.append(SwapLeg(token=token, direction="in"))
    native_input = swap_event.get("nativeInput")
    if native_input:
        token = await _build_token_amount(
            _native_amount_payload(native_input),
            metadata_cache=metadata_cache,
        )
        if token is not None and token.amount > 0:
            legs.append(SwapLeg(token=token, direction="out"))
    native_output = swap_event.get("nativeOutput")
    if native_output:
        token = await _build_token_amount(
            _native_amount_payload(native_output),
            metadata_cache=metadata_cache,
        )
        if token is not None and token.amount > 0:
            legs.append(SwapLeg(token=token, direction="in"))
    return legs


async def normalize_wallet_events(wallet: str, raw_txs: Iterable[dict]) -> List[NormalizedEvent]:
    events: List[NormalizedEvent] = []
    metadata_cache: dict[str, tuple[Optional[str], Optional[int]]] = {}

    for tx in raw_txs:
        if tx.get("transactionError"):
            continue
        signature = tx.get("signature") or tx.get("id") or "unknown"
        event_prefix = f"{signature}:{wallet}"
        ts = _parse_timestamp(tx.get("timestamp") or tx.get("blockTime"))
        fee_lamports = tx.get("fee") or tx.get("nativeFee") or 0
        fee_sol = _lamports_to_sol(fee_lamports)
        fee_payer = _fee_payer_address(tx)

        swap_event = None
        events_payload = tx.get("events") or {}
        if isinstance(events_payload, dict):
            swap_event = events_payload.get("swap")

        swap_legs: dict[tuple[str, str], List[Decimal]] = {}
        tx_events: List[NormalizedEvent] = []

        if swap_event:
            legs = await _build_swap_legs(swap_event, metadata_cache=metadata_cache)
            deltas: dict[str, Decimal] = {}
            meta: dict[str, tuple[Optional[str], Optional[int]]] = {}
            for leg in legs:
                mint = leg.token.mint
                amount = leg.token.amount or Decimal("0")
                if mint not in meta:
                    meta[mint] = (leg.token.symbol, leg.token.decimals)
                delta = amount if leg.direction == "in" else -amount
                deltas[mint] = deltas.get(mint, Decimal("0")) + delta
                key = (mint, leg.direction)
                swap_legs.setdefault(key, []).append(amount)

            for idx, mint in enumerate(sorted(deltas.keys())):
                net = deltas[mint]
                if net == 0:
                    continue
                symbol, decimals = meta.get(mint, (None, None))
                payload = {
                    "mint": mint,
                    "symbol": symbol,
                    "decimals": decimals,
                    "amount": str(abs(net)),
                }
                token = await _build_token_amount(payload, metadata_cache=metadata_cache)
                if token is None:
                    continue
                is_out = net < 0
                tx_events.append(
                    NormalizedEvent(
                        id=f"{event_prefix}#swap{idx}",
                        ts=ts,
                        kind="swap",
                        base_token=token if is_out else None,
                        quote_token=token if not is_out else None,
                        fee_sol=Decimal("0"),
                        wallet=wallet,
                        counterparty=swap_event.get("marketplace") if isinstance(swap_event, dict) else None,
                        raw={
                            "swap": swap_event,
                            "signature": signature,
                            "source": "helius_swap",
                            "swap_direction": "out" if is_out else "in",
                            "swap_net_delta": str(net),
                        },
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
            if token is None:
                continue
            if swap_event and direction and _consume_swap_leg(swap_legs, token.mint, direction, token.amount):
                continue
            kind = "transfer_in" if direction == "in" else "transfer_out"
            base_token = token if kind == "transfer_out" else None
            quote_token = token if kind == "transfer_in" else None
            counterparty = transfer.get("fromUserAccount") if kind == "transfer_in" else transfer.get("toUserAccount")
            tx_events.append(
                NormalizedEvent(
                    id=f"{event_prefix}#tok{idx}",
                    ts=ts,
                    kind=kind,
                    base_token=base_token,
                    quote_token=quote_token,
                    fee_sol=Decimal("0"),
                    wallet=wallet,
                    counterparty=counterparty,
                    raw={
                        "transfer": transfer,
                        "signature": signature,
                        "source": "helius_token_transfer",
                    },
                    tags=set(),
                )
            )

        for idx, transfer in enumerate(tx.get("nativeTransfers") or []):
            matches, direction = _transfer_matches_wallet(transfer, wallet)
            if not matches:
                continue
            token = await _build_token_amount(_native_amount_payload(transfer), metadata_cache=metadata_cache)
            if token is None:
                continue
            if swap_event and direction and _consume_swap_leg(swap_legs, token.mint, direction, token.amount):
                continue
            kind = "transfer_in" if direction == "in" else "transfer_out"
            base_token = token if kind == "transfer_out" else None
            quote_token = token if kind == "transfer_in" else None
            counterparty = transfer.get("fromUserAccount") if kind == "transfer_in" else transfer.get("toUserAccount")
            tx_events.append(
                NormalizedEvent(
                    id=f"{event_prefix}#sol{idx}",
                    ts=ts,
                    kind=kind,
                    base_token=base_token,
                    quote_token=quote_token,
                    fee_sol=Decimal("0"),
                    wallet=wallet,
                    counterparty=counterparty,
                    raw={
                        "native_transfer": transfer,
                        "signature": signature,
                        "source": "helius_native_transfer",
                    },
                    tags=set(),
                )
            )

        fee_assigned = False
        for event in tx_events:
            if fee_assigned:
                continue
            if fee_payer:
                if event.wallet != fee_payer:
                    continue
            if event.kind == "transfer_in":
                continue
            event.fee_sol = fee_sol
            fee_assigned = True

        events.extend(tx_events)

    events.sort(key=lambda ev: (ev.ts, ev.id))
    return events
