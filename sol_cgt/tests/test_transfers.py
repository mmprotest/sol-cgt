from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sol_cgt.reconciliation.transfers import detect_self_transfers
from sol_cgt.types import NormalizedEvent, TokenAmount


def make_transfer(
    event_id: str,
    kind: str,
    wallet: str,
    amount: int,
    ts: datetime,
    *,
    signature: str | None = None,
    counterparty: str | None = None,
) -> NormalizedEvent:
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
        counterparty=counterparty,
        raw={"signature": signature} if signature else {},
        tags=set(),
    )


def test_detect_self_transfers_adds_tag():
    ts = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    out_event = make_transfer(
        "t1#0",
        "transfer_out",
        "W1",
        1_000_000,
        ts,
        signature="sig1",
        counterparty="W2",
    )
    in_event = make_transfer(
        "t2#0",
        "transfer_in",
        "W2",
        1_000_000,
        ts + timedelta(minutes=2),
        signature="sig1",
        counterparty="W1",
    )
    events = [out_event, in_event]
    matches = detect_self_transfers(events, wallets=["W1", "W2"])
    assert len(matches) == 1
    assert "self_transfer" in out_event.tags
    assert "self_transfer" in in_event.tags


def test_detect_self_transfers_prefers_signature() -> None:
    ts = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    out_event_a = make_transfer(
        "t1#0",
        "transfer_out",
        "W1",
        1_000_000,
        ts,
        signature="sig-a",
        counterparty="W2",
    )
    in_event_a = make_transfer(
        "t1#1",
        "transfer_in",
        "W2",
        1_000_000,
        ts + timedelta(seconds=30),
        signature="sig-a",
        counterparty="W1",
    )
    out_event_b = make_transfer(
        "t2#0",
        "transfer_out",
        "W1",
        1_000_000,
        ts + timedelta(seconds=10),
        signature="sig-b",
        counterparty="W2",
    )
    in_event_b = make_transfer(
        "t2#1",
        "transfer_in",
        "W2",
        1_000_000,
        ts + timedelta(seconds=40),
        signature="sig-b",
        counterparty="W1",
    )
    events = [out_event_a, out_event_b, in_event_a, in_event_b]
    matches = detect_self_transfers(events, wallets=["W1", "W2"])
    assert len(matches) == 2
    assert all(match.out_event.raw.get("signature") == match.in_event.raw.get("signature") for match in matches)


def test_detect_self_transfers_multiple_legs_same_signature() -> None:
    ts = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    out_event_1 = make_transfer(
        "t1#0",
        "transfer_out",
        "W1",
        1_000_000,
        ts,
        signature="sig-multi",
        counterparty="W2",
    )
    out_event_2 = make_transfer(
        "t1#1",
        "transfer_out",
        "W1",
        2_000_000,
        ts,
        signature="sig-multi",
        counterparty="W3",
    )
    in_event_1 = make_transfer(
        "t1#2",
        "transfer_in",
        "W2",
        1_000_000,
        ts,
        signature="sig-multi",
        counterparty="W1",
    )
    in_event_2 = make_transfer(
        "t1#3",
        "transfer_in",
        "W3",
        2_000_000,
        ts,
        signature="sig-multi",
        counterparty="W1",
    )
    events = [out_event_1, out_event_2, in_event_1, in_event_2]
    matches = detect_self_transfers(events, wallets=["W1", "W2", "W3"])
    assert len(matches) == 2
    pairs = {(match.out_event.wallet, match.in_event.wallet) for match in matches}
    assert pairs == {("W1", "W2"), ("W1", "W3")}


def test_detect_self_transfers_signature_without_counterparty() -> None:
    ts = datetime(2023, 1, 1, 12, tzinfo=timezone.utc)
    out_event = make_transfer(
        "t1#0",
        "transfer_out",
        "W1",
        1_000_000,
        ts,
        signature="sig-no-ctpy",
        counterparty=None,
    )
    in_event = make_transfer(
        "t1#1",
        "transfer_in",
        "W2",
        1_000_000,
        ts + timedelta(seconds=30),
        signature="sig-no-ctpy",
        counterparty=None,
    )
    matches = detect_self_transfers([out_event, in_event], wallets=["W1", "W2"])
    assert len(matches) == 1
    assert "self_transfer" in out_event.tags
    assert "self_transfer" in in_event.tags
