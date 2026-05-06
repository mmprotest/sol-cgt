from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.eligibility import apply_accounting_policy
from sol_cgt.pricing import TimestampPriceProvider
from sol_cgt.pricing import valuation as valuation_module
from sol_cgt.types import NormalizedEvent, TokenAmount


def _ctx():
    return valuation_module.ValuationContext(usd_provider=TimestampPriceProvider(api_key=None), fx_rate=lambda _: Decimal("1.0"))


def _ev(id: str, kind: str, *, signature: str, base=None, quote=None, raw=None):
    return NormalizedEvent(
        id=id,
        ts=datetime(2024, 7, 3, tzinfo=timezone.utc),
        kind=kind,
        base_token=base,
        quote_token=quote,
        fee_sol=Decimal("0"),
        wallet="W",
        raw={"signature": signature, **(raw or {})},
    )


def test_prefers_wsol_over_native_sol_anchor_without_double_counting():
    target = _ev("s1#0", "transfer_in", signature="s1", quote=TokenAmount(mint="UNSUPPORTED", decimals=6, amount_raw=1_000_000))
    wsol = _ev("s1#1", "transfer_out", signature="s1", base=TokenAmount(mint="So11111111111111111111111111111111111111112", decimals=9, amount_raw=100_000_000), raw={"proceeds_hint_aud": "5"})
    sol = _ev("s1#2", "transfer_out", signature="s1", base=TokenAmount(mint="SOL", decimals=9, amount_raw=100_000_000), raw={"proceeds_hint_aud": "99"})
    valuation_module.valuate_events([target, wsol, sol], _ctx())
    assert target.raw["cost_hint_aud"] == "5"


def test_no_anchor_stays_missing_price():
    target = _ev("s2#0", "transfer_in", signature="s2", quote=TokenAmount(mint="UNSUPPORTED", decimals=6, amount_raw=1_000_000))
    warnings = valuation_module.valuate_events([target], _ctx())
    assert target.raw["valuation_method"] == "missing_token_price_no_counterparty_leg"
    assert any(w.event_id == target.id and w.code == "missing_token_price_no_counterparty_leg" for w in warnings)


def test_routed_tx_with_multiple_non_anchor_mints_still_infers_from_anchor():
    target = _ev("s3#0", "transfer_in", signature="s3", quote=TokenAmount(mint="UNSUPPORTED", decimals=6, amount_raw=1_000_000))
    other = _ev("s3#1", "transfer_out", signature="s3", base=TokenAmount(mint="OTHER_NON_ANCHOR", decimals=6, amount_raw=1_000_000), raw={"proceeds_hint_aud": "3"})
    anchor = _ev("s3#2", "transfer_out", signature="s3", base=TokenAmount(mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", decimals=6, amount_raw=4_000_000), raw={"proceeds_hint_aud": "4"})
    valuation_module.valuate_events([target, other, anchor], _ctx())
    assert target.raw["cost_hint_aud"] == "4"


def test_inferred_rows_are_taxable_and_reporting_path_does_not_retag_components():
    target = _ev("s4#0", "transfer_out", signature="s4", base=TokenAmount(mint="UNSUPPORTED", decimals=6, amount_raw=1_000_000))
    anchor = _ev("s4#1", "transfer_in", signature="s4", quote=TokenAmount(mint="So11111111111111111111111111111111111111112", decimals=9, amount_raw=100_000_000), raw={"cost_hint_aud": "10"})
    valuation_module.valuate_events([target, anchor], _ctx())
    before = target.raw.get("swap_component")
    result = apply_accounting_policy([target, anchor], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert any(e.id == target.id for e in result.taxable_events)
    assert target.raw.get("swap_component") == before


def test_token_to_token_shape_ignores_tiny_native_sol_anchor_for_inference():
    target = _ev("s5#0", "transfer_in", signature="s5", quote=TokenAmount(mint="UNSUPPORTED", decimals=6, amount_raw=1_000_000))
    out_non_anchor = _ev("s5#1", "transfer_out", signature="s5", base=TokenAmount(mint="OTHER_NON_ANCHOR", decimals=6, amount_raw=2_000_000), raw={"proceeds_hint_aud": "20"})
    tiny_sol = _ev("s5#2", "transfer_out", signature="s5", base=TokenAmount(mint="SOL", decimals=9, amount_raw=1000), raw={"proceeds_hint_aud": "0.01"})
    valuation_module.valuate_events([target, out_non_anchor, tiny_sol], _ctx())
    assert target.raw["valuation_method"] == "missing_token_price_no_counterparty_leg"
    assert target.raw.get("cost_hint_aud") is None
