from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.engine import AccountingEngine, SimplePriceProvider
from sol_cgt.reconciliation.transfers import detect_self_transfers
from sol_cgt.types import NormalizedEvent, TokenAmount


def _token(mint: str, amount: int, decimals: int = 0, symbol: str | None = None) -> TokenAmount:
    return TokenAmount(mint=mint, amount_raw=amount, decimals=decimals, symbol=symbol)


def _event(
    event_id: str,
    kind: str,
    *,
    ts: datetime,
    wallet: str,
    base: TokenAmount | None = None,
    quote: TokenAmount | None = None,
    fee_sol: Decimal = Decimal("0"),
    counterparty: str | None = None,
) -> NormalizedEvent:
    return NormalizedEvent(
        id=event_id,
        ts=ts,
        kind=kind,
        base_token=base,
        quote_token=quote,
        fee_sol=fee_sol,
        wallet=wallet,
        counterparty=counterparty,
        raw={"signature": event_id.split("#")[0]},
        tags=set(),
    )


def test_self_transfer_lot_move() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
    w1 = "W1"
    w2 = "W2"
    buy = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=w1,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
    )
    buy.raw["cost_aud"] = "100"
    transfer_out = _event(
        "tx2#0",
        "transfer_out",
        ts=ts2,
        wallet=w1,
        base=_token("TOKENX", 4, decimals=0, symbol="TKX"),
        counterparty=w2,
    )
    transfer_in = _event(
        "tx2#1",
        "transfer_in",
        ts=ts2,
        wallet=w2,
        quote=_token("TOKENX", 4, decimals=0, symbol="TKX"),
        counterparty=w1,
    )
    events = [buy, transfer_out, transfer_in]
    matches = detect_self_transfers(events, wallets=[w1, w2])
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("0")}))
    result = engine.process(events, wallets=[w1, w2], transfer_matches=matches)
    assert len(result.disposals) == 0
    assert len(result.lot_moves) == 1
    moved_lot = next(lot for lot in result.acquisitions if lot.wallet == w2)
    assert moved_lot.ts == buy.ts
    assert moved_lot.qty_acquired == Decimal("4")


def test_self_transfer_fee_not_disposed() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
    w1 = "W1"
    w2 = "W2"
    buy = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=w1,
        quote=_token("TOKENY", 10, decimals=0, symbol="TKY"),
    )
    buy.raw["cost_aud"] = "50"
    transfer_out = _event(
        "tx2#0",
        "transfer_out",
        ts=ts2,
        wallet=w1,
        base=_token("TOKENY", 5, decimals=0, symbol="TKY"),
        fee_sol=Decimal("0.001"),
        counterparty=w2,
    )
    transfer_in = _event(
        "tx2#1",
        "transfer_in",
        ts=ts2,
        wallet=w2,
        quote=_token("TOKENY", 5, decimals=0, symbol="TKY"),
        counterparty=w1,
    )
    events = [buy, transfer_out, transfer_in]
    matches = detect_self_transfers(events, wallets=[w1, w2])
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("100")}))
    result = engine.process(events, wallets=[w1, w2], transfer_matches=matches)
    assert len(result.disposals) == 0
    assert len(result.lot_moves) == 1
    moved_lot = next(lot for lot in result.acquisitions if lot.wallet == w2)
    assert moved_lot.fees_aud == Decimal("0.10")


def test_out_of_scope_transfer_allocates_fees() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
    wallet = "W1"
    external = "EXT"
    buy = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("TOKENZ", 10, decimals=0, symbol="TKZ"),
    )
    buy.raw["cost_aud"] = "100"
    transfer_out = _event(
        "tx2#0",
        "transfer_out",
        ts=ts2,
        wallet=wallet,
        base=_token("TOKENZ", 4, decimals=0, symbol="TKZ"),
        fee_sol=Decimal("0.002"),
        counterparty=external,
    )
    events = [buy, transfer_out]
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("50")}))
    result = engine.process(events, wallets=[wallet])
    assert len(result.disposals) == 0
    assert len(result.lot_moves) == 1
    external_lot = next(lot for lot in result.acquisitions if lot.wallet.startswith("__external__"))
    assert external_lot.qty_acquired == Decimal("4")
    assert external_lot.fees_aud == Decimal("0.10")
