"""Self transfer reconciliation utilities."""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from ..types import NormalizedEvent


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
