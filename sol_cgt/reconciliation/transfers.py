"""Self transfer reconciliation utilities."""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from ..types import NormalizedEvent


def normalize_wallet_address(value: Optional[str]) -> Optional[str]:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def classify_internal_transfers(
    events: Iterable[NormalizedEvent], wallets: Iterable[str]
) -> dict[str, int]:
    wallet_set = {w for w in (normalize_wallet_address(wallet) for wallet in wallets) if w}
    internal_keys: set[tuple[str, str, str, str, str, str]] = set()
    internal_count = 0
    external_in = 0
    external_out = 0
    for event in events:
        if event.kind not in {"transfer_in", "transfer_out"}:
            continue
        from_wallet = normalize_wallet_address(
            event.raw.get("transfer_from_wallet") or event.raw.get("source_wallet")
        )
        to_wallet = normalize_wallet_address(
            event.raw.get("transfer_to_wallet") or event.raw.get("destination_wallet")
        )
        mint = (event.base_token.mint if event.base_token else (event.quote_token.mint if event.quote_token else ""))
        amount = str(event.base_token.amount if event.base_token else (event.quote_token.amount if event.quote_token else Decimal("0")))
        signature = str(event.raw.get("signature") or event.id.split("#")[0])
        dedupe_key = (signature, mint, amount, from_wallet or "", to_wallet or "", event.ts.isoformat())
        is_internal = bool(from_wallet and to_wallet and from_wallet in wallet_set and to_wallet in wallet_set)
        if is_internal:
            event.kind = "transfer_internal"
            event.tags.add("self_transfer")
            event.raw.update(
                {
                    "is_internal_transfer": True,
                    "internal_transfer_reason": "both endpoints are provided wallets",
                    "from_wallet": from_wallet,
                    "to_wallet": to_wallet,
                    "asset_mint": mint,
                    "amount": amount,
                }
            )
            if dedupe_key in internal_keys:
                event.tags.add("internal_transfer_duplicate")
                event.raw["is_internal_transfer_duplicate"] = True
            else:
                internal_keys.add(dedupe_key)
                internal_count += 1
            continue
        if event.kind == "transfer_in":
            external_in += 1
        else:
            external_out += 1
    return {
        "owned_wallets": len(wallet_set),
        "internal_transfers": internal_count,
        "external_transfer_in": external_in,
        "external_transfer_out": external_out,
    }


class TransferMatch:
    __slots__ = ("out_event", "in_event")

    def __init__(self, out_event: NormalizedEvent, in_event: NormalizedEvent) -> None:
        self.out_event = out_event
        self.in_event = in_event


def detect_self_transfers(
    events: Iterable[NormalizedEvent], wallets: Iterable[str], window_minutes: int = 5
) -> List[TransferMatch]:
    wallet_set = set(wallets)
    matches: List[TransferMatch] = []
    pending: Dict[str, Deque[NormalizedEvent]] = defaultdict(deque)
    window = timedelta(minutes=window_minutes)
    sorted_events = sorted(events, key=lambda ev: (ev.ts, ev.id))

    def _signature(event: NormalizedEvent) -> Optional[str]:
        signature = event.raw.get("signature")
        if not signature or signature == "unknown":
            return None
        return str(signature)

    def _amount_matches(a: Decimal, b: Decimal, *, tolerance: Decimal = Decimal("0.00000001")) -> bool:
        return (a - b).copy_abs() <= tolerance

    def _counterparty_matches(out_event: NormalizedEvent, in_event: NormalizedEvent) -> bool:
        if not out_event.counterparty or not in_event.counterparty:
            return False
        return out_event.counterparty == in_event.wallet and in_event.counterparty == out_event.wallet

    # Prefer signature-based matching
    signature_groups: Dict[str, List[NormalizedEvent]] = defaultdict(list)
    for event in sorted_events:
        signature = _signature(event)
        if signature:
            signature_groups[signature].append(event)
    for signature, group in signature_groups.items():
        outs = [
            ev
            for ev in group
            if ev.kind == "transfer_out"
            and ev.base_token is not None
            and ev.wallet in wallet_set
        ]
        ins = [
            ev
            for ev in group
            if ev.kind == "transfer_in"
            and ev.quote_token is not None
            and ev.wallet in wallet_set
        ]
        used_in: set[str] = set()
        for out_event in outs:
            for in_event in ins:
                if in_event.id in used_in:
                    continue
                if out_event.base_token.mint != in_event.quote_token.mint:
                    continue
                if not _amount_matches(out_event.base_token.amount, in_event.quote_token.amount):
                    continue
                if out_event.wallet == in_event.wallet:
                    continue
                if out_event.counterparty and in_event.counterparty:
                    if not _counterparty_matches(out_event, in_event):
                        continue
                match = TransferMatch(out_event, in_event)
                matches.append(match)
                out_event.tags.add("self_transfer")
                in_event.tags.add("self_transfer")
                used_in.add(in_event.id)
                break

    matched_out_ids = {match.out_event.id for match in matches}
    matched_in_ids = {match.in_event.id for match in matches}

    for event in sorted_events:
        signature = _signature(event)
        if signature:
            if event.id in matched_out_ids or event.id in matched_in_ids:
                continue
        if event.kind == "transfer_out" and event.base_token is not None:
            if event.counterparty and event.counterparty not in wallet_set:
                continue
            pending[event.base_token.mint].append(event)
        elif event.kind == "transfer_in" and event.quote_token is not None:
            queue = pending.get(event.quote_token.mint)
            if not queue:
                continue
            while queue and event.ts - queue[0].ts > window:
                queue.popleft()
            match_index: Optional[int] = None
            for idx, candidate in enumerate(queue):
                if candidate.wallet == event.wallet:
                    continue
                if not _amount_matches(candidate.base_token.amount, event.quote_token.amount):
                    continue
                if candidate.counterparty and event.counterparty:
                    if not _counterparty_matches(candidate, event):
                        continue
                match_index = idx
                break
            if match_index is not None:
                candidate = queue[match_index]
                match = TransferMatch(candidate, event)
                matches.append(match)
                candidate.tags.add("self_transfer")
                event.tags.add("self_transfer")
                queue.remove(candidate)
    return matches
