from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.engine import AccountingEngine, SimplePriceProvider
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
        raw=raw or {},
        counterparty=None,
        tags=set(),
    )


def test_fifo_disposal_allocation():
    ts1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2023, 6, 1, tzinfo=timezone.utc)
    ts3 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    wallet = "W1"
    lot1 = _event(
        "tx1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("TOKENX", 10, decimals=0, symbol="TKX"),
        raw={"cost_aud": "100"},
    )
    lot2 = _event(
        "tx2#0",
        "buy",
        ts=ts2,
        wallet=wallet,
        quote=_token("TOKENX", 20, decimals=0, symbol="TKX"),
        raw={"cost_aud": "240"},
    )
    disposal = _event(
        "tx3#0",
        "sell",
        ts=ts3,
        wallet=wallet,
        base=_token("TOKENX", 15, decimals=0, symbol="TKX"),
        raw={"proceeds_aud": "450"},
        fee_sol=Decimal("0.02"),
    )
    provider = SimplePriceProvider({"SOL": Decimal("100")})
    engine = AccountingEngine(price_provider=provider)
    result = engine.process([lot1, lot2, disposal])
    acquisitions = result.acquisitions
    disposals = result.disposals
    assert len(acquisitions) == 2
    assert len(disposals) == 2
    assert disposals[0].cost_base_aud == Decimal("100.00")
    assert disposals[1].qty_disposed == Decimal("5")
    total_gain = sum((d.gain_loss_aud for d in disposals), Decimal("0"))
    expected_gain = Decimal("450") - Decimal("2.00") - Decimal("100") - Decimal("60")
    assert total_gain == expected_gain


def test_hifo_prefers_high_cost():
    ts1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2023, 6, 1, tzinfo=timezone.utc)
    ts3 = datetime(2023, 7, 1, tzinfo=timezone.utc)
    wallet = "W2"
    lot1 = _event(
        "hx1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("TOKENY", 5, decimals=0, symbol="TKY"),
        raw={"cost_aud": "50"},
    )
    lot2 = _event(
        "hx2#0",
        "buy",
        ts=ts2,
        wallet=wallet,
        quote=_token("TOKENY", 5, decimals=0, symbol="TKY"),
        raw={"cost_aud": "150"},
    )
    disposal = _event(
        "hx3#0",
        "sell",
        ts=ts3,
        wallet=wallet,
        base=_token("TOKENY", 5, decimals=0, symbol="TKY"),
        raw={"proceeds_aud": "200"},
    )
    engine = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("0")}), method="HIFO")
    disposals = engine.process([lot1, lot2, disposal]).disposals
    assert len(disposals) == 1
    assert disposals[0].cost_base_aud == Decimal("150.00")
