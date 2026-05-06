from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.engine import AccountingEngine, SimplePriceProvider
from sol_cgt.types import MissingLotIssue, NormalizedEvent, TokenAmount


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


def test_low_unit_price_cost_basis_preserved_full_disposal() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 2, tzinfo=timezone.utc)
    wallet = "W3"
    qty_raw = 358_099_601_641
    acquisition = _event(
        "lp1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("LOW", qty_raw, decimals=6, symbol="LOW"),
        raw={"cost_aud": "42.33"},
    )
    disposal = _event(
        "lp2#0",
        "sell",
        ts=ts2,
        wallet=wallet,
        base=_token("LOW", qty_raw, decimals=6, symbol="LOW"),
        raw={"proceeds_aud": "39.54"},
    )

    result = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("0")})).process([acquisition, disposal])
    assert result.acquisitions[0].unit_cost_aud > Decimal("0")
    assert len(result.disposals) == 1
    assert result.disposals[0].cost_base_aud == Decimal("42.33")


def test_low_unit_price_cost_basis_preserved_partial_disposal() -> None:
    ts1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2024, 1, 3, tzinfo=timezone.utc)
    wallet = "W4"
    qty_raw = 358_099_601_641
    sold_raw = qty_raw // 2
    qty = Decimal(qty_raw) / Decimal("1000000")
    sold_qty = Decimal(sold_raw) / Decimal("1000000")
    expected_cost_base = (Decimal("42.33") * sold_qty / qty).quantize(Decimal("0.01"))

    acquisition = _event(
        "pp1#0",
        "buy",
        ts=ts1,
        wallet=wallet,
        quote=_token("LOW", qty_raw, decimals=6, symbol="LOW"),
        raw={"cost_aud": "42.33"},
    )
    disposal = _event(
        "pp2#0",
        "sell",
        ts=ts2,
        wallet=wallet,
        base=_token("LOW", sold_raw, decimals=6, symbol="LOW"),
        raw={"proceeds_aud": "20.00"},
    )

    result = AccountingEngine(price_provider=SimplePriceProvider({"SOL": Decimal("0")})).process([acquisition, disposal])
    assert len(result.disposals) == 1
    assert result.disposals[0].cost_base_aud == expected_cost_base


def _token(mint: str, amount: int, decimals: int = 0, symbol: str | None = None) -> TokenAmount:
    return TokenAmount(mint=mint, amount_raw=amount, decimals=decimals, symbol=symbol)


def _ev(event_id: str, kind: str, ts: datetime, wallet: str, *, base=None, quote=None, counterparty=None, raw=None) -> NormalizedEvent:
    payload = {"signature": event_id.split("#")[0]}
    if raw:
        payload.update(raw)
    return NormalizedEvent(id=event_id, ts=ts, kind=kind, wallet=wallet, base_token=base, quote_token=quote, counterparty=counterparty, fee_sol=Decimal("0"), raw=payload, tags=set())


def test_medium_confidence_inferred_transfer_out_is_not_forced_taxable_disposal() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    buy = _ev("b#0", "buy", ts, "W1", quote=_token("ABC", 10, symbol="ABC"), raw={"cost_aud": "100"})
    sell = _ev("s#0", "transfer_out", ts.replace(hour=1), "W1", base=_token("ABC", 4, symbol="ABC"), counterparty="EXT", raw={"accounting_action": "taxable_disposal", "proceeds_hint_aud": "80", "valuation_method": "inferred_from_same_signature_anchor", "valuation_confidence": "medium", "source": "helius_token_transfer"})
    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([buy, sell], wallets=["W1"])
    assert len(result.disposals) == 0
    assert len(result.lot_moves) == 0


def test_medium_confidence_inferred_transfer_in_with_cost_hint_is_forced_taxable_acquisition() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    event = _ev("a#0", "transfer_in", ts, "W1", quote=_token("ABC", 5, symbol="ABC"), counterparty="EXT", raw={"accounting_action": "taxable_acquisition", "cost_hint_aud": "55", "valuation_method": "inferred_from_same_signature_anchor", "valuation_confidence": "medium", "source": "helius_token_transfer"})
    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([event], wallets=["W1"], external_lot_tracking=True)
    assert len(result.acquisitions) == 1
    assert len(result.lot_moves) == 0


def test_high_confidence_inferred_transfer_out_is_still_forced_taxable_disposal() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    buy = _ev("b#0", "buy", ts, "W1", quote=_token("ABC", 10, symbol="ABC"), raw={"cost_aud": "100"})
    sell = _ev("s#0", "transfer_out", ts.replace(hour=1), "W1", base=_token("ABC", 4, symbol="ABC"), counterparty="EXT", raw={"accounting_action": "taxable_disposal", "proceeds_hint_aud": "80", "valuation_method": "inferred_from_same_signature_anchor", "valuation_confidence": "high", "source": "helius_token_transfer"})
    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([buy, sell], wallets=["W1"])
    assert len(result.disposals) == 1


def test_swap_event_remains_taxable() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    buy = _ev("b#0", "buy", ts, "W1", quote=_token("ABC", 10, symbol="ABC"), raw={"cost_aud": "100"})
    swap = _ev("sw#0", "swap", ts.replace(hour=1), "W1", base=_token("ABC", 4, symbol="ABC"), quote=_token("XYZ", 8, symbol="XYZ"), raw={"proceeds_hint_aud": "80", "cost_hint_aud": "80", "valuation_method": "canonical_swap"})
    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([buy, swap], wallets=["W1"])
    assert len(result.disposals) == 1
    assert len(result.acquisitions) == 2


def test_swap_component_transfer_row_remains_excluded_from_taxable() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    event = _ev("c#0", "transfer_in", ts, "W1", quote=_token("WSOL", 5, symbol="WSOL"), counterparty="EXT", raw={"accounting_action": "taxable_acquisition", "cost_hint_aud": "55", "swap_component": True, "valuation_method": "inferred_from_same_signature_anchor", "source": "helius_token_transfer"})
    engine = AccountingEngine(price_provider=SimplePriceProvider({}))
    result = engine.process([event], wallets=["W1"])
    assert engine.debug_counters["valued_transfer_group_acquisitions_accounted"] == 0
    assert len(result.acquisitions) == 1
    assert len(result.lot_moves) == 0


def test_medium_confidence_inferred_transfer_in_without_cost_hint_is_not_forced_taxable() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    event = _ev(
        "a#1",
        "transfer_in",
        ts,
        "W1",
        quote=_token("ABC", 5, symbol="ABC"),
        counterparty="EXT",
        raw={
            "accounting_action": "taxable_acquisition",
            "valuation_method": "inferred_from_same_signature_anchor",
            "valuation_confidence": "medium",
            "source": "helius_token_transfer",
        },
    )
    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([event], wallets=["W1"], external_lot_tracking=True)
    assert len(result.acquisitions) == 1
    assert len(result.lot_moves) == 0
    assert result.acquisitions[0].unit_cost_aud == Decimal("0")


def test_inferred_transfer_in_lot_is_consumed_and_inferred_transfer_out_is_not_disposal() -> None:
    ts = datetime(2024, 3, 1, tzinfo=timezone.utc)
    inferred_in = _ev(
        "i#0",
        "transfer_in",
        ts,
        "W1",
        quote=_token("ABC", 5, symbol="ABC"),
        counterparty="EXT",
        raw={
            "accounting_action": "taxable_acquisition",
            "cost_hint_aud": "55",
            "valuation_method": "inferred_from_same_signature_anchor",
            "valuation_confidence": "medium",
            "source": "helius_token_transfer",
        },
    )
    inferred_out = _ev(
        "o#0",
        "transfer_out",
        ts.replace(hour=1),
        "W1",
        base=_token("ABC", 2, symbol="ABC"),
        counterparty="EXT",
        raw={
            "accounting_action": "taxable_disposal",
            "proceeds_hint_aud": "22",
            "valuation_method": "inferred_from_same_signature_anchor",
            "valuation_confidence": "medium",
            "source": "helius_token_transfer",
        },
    )
    swap = _ev(
        "sw2#0",
        "swap",
        ts.replace(hour=2),
        "W1",
        base=_token("ABC", 5, symbol="ABC"),
        quote=_token("XYZ", 10, symbol="XYZ"),
        raw={"proceeds_hint_aud": "100", "cost_hint_aud": "100", "valuation_method": "canonical_swap"},
    )

    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([inferred_in, inferred_out, swap], wallets=["W1"])
    assert len(result.disposals) == 1
    assert result.disposals[0].event_id == "sw2#0"
    assert result.disposals[0].cost_base_aud == Decimal("55.00")


def test_partial_missing_lot_disposal_processes_available_amount() -> None:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    buy = _ev("p1#0", "buy", ts, "W1", quote=_token("ABC", 10, symbol="ABC"), raw={"cost_aud": "100"})
    sell = _ev("p2#0", "sell", ts.replace(hour=1), "W1", base=_token("ABC", 12, symbol="ABC"), raw={"proceeds_aud": "120", "signature": "sigp2"})
    issues: list[MissingLotIssue] = []
    result = AccountingEngine(price_provider=SimplePriceProvider({})).process([buy, sell], strict_lots=False, missing_lot_issues=issues)
    assert len(result.disposals) == 1
    assert result.disposals[0].qty_disposed == Decimal("10")
    assert issues and issues[0].shortfall_qty == Decimal("2")
