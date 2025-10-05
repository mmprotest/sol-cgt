from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sol_cgt.reconciliation.transfers import detect_self_transfers
from sol_cgt.types import NormalizedEvent, TokenAmount


def make_transfer(event_id: str, kind: str, wallet: str, amount: int, ts: datetime) -> NormalizedEvent:
    token = TokenAmount(mint="USDC", amount_raw=amount, decimals=6, symbol="USDC")
    base = token if kind == "transfer_out" else None
    quote = token if kind == "transfer_in" else None
    return NormalizedEvent(
        id=event_id,
        ts=ts,
        kind=kind,
        base_token=base,
        quote_token=quote,
        fee_sol=Decimal("0"),
        wallet=wallet,
        counterparty=None,
        raw={},
        tags=set(),
    )


def test_detect_self_transfers_adds_tag():
    ts = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    out_event = make_transfer("t1#0", "transfer_out", "W1", 1_000_000, ts)
    in_event = make_transfer("t2#0", "transfer_in", "W2", 1_000_000, ts + timedelta(minutes=2))
    events = [out_event, in_event]
    matches = detect_self_transfers(events, wallets=["W1", "W2"])
    assert len(matches) == 1
    assert "self_transfer" in out_event.tags
    assert "self_transfer" in in_event.tags
