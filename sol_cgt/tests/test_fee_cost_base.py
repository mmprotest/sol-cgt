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
    raw: dict | None = None,
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
        raw=raw or {"signature": event_id.split("#")[0]},
        tags=set(),
    )


def test_external_move_fees_flow_into_disposal() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
    ts3 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    wallet = "W1"
    external = "EXT1"
    buy = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        fee_sol=Decimal("0.02"),
        raw={"cost_aud": "100", "signature": "tx1"},
    )
    transfer_out = _event(
        "tx2#0",
        "transfer_out",
        ts=ts2,
        wallet=wallet,
        base=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        fee_sol=Decimal("0.01"),
        counterparty=external,
    )
    transfer_in = _event(
        "tx2#1",
        "transfer_in",
        ts=ts2,
        wallet=wallet,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        counterparty=external,
    )
    disposal = _event(
        "tx3#0",
        "sell",
        ts=ts3,
        wallet=wallet,
        base=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        fee_sol=Decimal("0.03"),
        raw={"proceeds_aud": "120", "signature": "tx3"},
    )
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("100")}))
    result = engine.process(
        [buy, transfer_out, transfer_in, disposal],
        wallets=[wallet],
        external_lot_tracking=True,
    )
    assert len(result.disposals) == 1
    disposal_record = result.disposals[0]
    assert disposal_record.gain_loss_aud == Decimal("14.00")
    returned_lot = next(
        lot for lot in result.acquisitions if lot.wallet == wallet and lot.source_type == "external_return"
    )
    assert returned_lot.fees_aud == Decimal("0")


def test_partial_disposal_prorates_fees() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
    ts3 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    wallet = "W1"
    buy = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        fee_sol=Decimal("0.02"),
        raw={"cost_aud": "100", "signature": "tx1"},
    )
    dispose_first = _event(
        "tx2#0",
        "sell",
        ts=ts2,
        wallet=wallet,
        base=_token("TOKENX", 4, decimals=0, symbol="TKX"),
        raw={"proceeds_aud": "44", "signature": "tx2"},
    )
    engine_first = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("100")}))
    result_first = engine_first.process([buy, dispose_first])
    remaining_lot = result_first.acquisitions[0]
    assert remaining_lot.remaining_qty == Decimal("6")
    assert remaining_lot.fees_aud == Decimal("1.2")
    assert result_first.disposals[0].gain_loss_aud == Decimal("3.20")

    dispose_second = _event(
        "tx3#0",
        "sell",
        ts=ts3,
        wallet=wallet,
        base=_token("TOKENX", 6, decimals=0, symbol="TKX"),
        raw={"proceeds_aud": "66", "signature": "tx3"},
    )
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("100")}))
    result = engine.process([buy, dispose_first, dispose_second])
    assert [d.gain_loss_aud for d in result.disposals] == [Decimal("3.20"), Decimal("4.80")]


def test_self_transfer_fee_in_cost_base() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 2, 1, tzinfo=timezone.utc)
    ts3 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    w1 = "W1"
    w2 = "W2"
    buy = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=w1,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        raw={"cost_aud": "100", "signature": "tx1"},
    )
    transfer_out = _event(
        "tx2#0",
        "transfer_out",
        ts=ts2,
        wallet=w1,
        base=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        fee_sol=Decimal("0.01"),
        counterparty=w2,
    )
    transfer_in = _event(
        "tx2#1",
        "transfer_in",
        ts=ts2,
        wallet=w2,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        counterparty=w1,
    )
    disposal = _event(
        "tx3#0",
        "sell",
        ts=ts3,
        wallet=w2,
        base=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        raw={"proceeds_aud": "120", "signature": "tx3"},
    )
    matches = detect_self_transfers([buy, transfer_out, transfer_in, disposal], wallets=[w1, w2])
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("100")}))
    result = engine.process([buy, transfer_out, transfer_in, disposal], wallets=[w1, w2], transfer_matches=matches)
    assert len(result.disposals) == 1
    assert result.disposals[0].gain_loss_aud == Decimal("19.00")
