"""Self transfer reconciliation utilities."""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import timedelta
from decimal import Decimal
from typing import Deque, Dict, Iterable, List, Tuple

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
    pending: Dict[Tuple[str, Decimal], Deque[NormalizedEvent]] = defaultdict(deque)
    window = timedelta(minutes=window_minutes)
    for event in sorted(events, key=lambda ev: (ev.ts, ev.id)):
        if event.kind == "transfer_out" and event.base_token is not None:
            if event.counterparty and event.counterparty not in wallet_set:
                continue
            key = (event.base_token.mint, event.base_token.amount)
            pending[key].append(event)
        elif event.kind == "transfer_in" and event.quote_token is not None:
            key = (event.quote_token.mint, event.quote_token.amount)
            queue = pending.get(key)
            if not queue:
                continue
            while queue:
                candidate = queue[0]
                if event.ts - candidate.ts > window:
                    queue.popleft()
                    continue
                if candidate.wallet == event.wallet:
                    # same wallet, ignore
                    queue.popleft()
                    continue
                match = TransferMatch(candidate, event)
                matches.append(match)
                candidate.tags.add("self_transfer")
                event.tags.add("self_transfer")
                queue.popleft()
                break
    return matches
