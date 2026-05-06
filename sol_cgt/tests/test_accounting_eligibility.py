from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.eligibility import apply_accounting_policy
from sol_cgt.types import NormalizedEvent, TokenAmount


def _ev(kind, base=None, quote=None, raw=None):
    return NormalizedEvent(id=f"id-{kind}", ts=datetime(2024,1,1,tzinfo=timezone.utc), kind=kind, wallet="w", base_token=base, quote_token=quote, raw=raw or {}, fee_sol=Decimal("0"))


def _tok(mint, amount, decimals=9):
    return TokenAmount(mint=mint, amount_raw=amount, decimals=decimals)


def test_transfer_in_without_consideration_goes_manual_review():
    ev = _ev("transfer_in", quote=_tok("abc", 1000), raw={})
    res = apply_accounting_policy([ev], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert len(res.taxable_events) == 0
    assert res.manual_review[0]["reason"] == "external_transfer_in_unclassified"


def test_sol_dust_ignored():
    ev = _ev("sell", base=_tok("SOL", 1, decimals=9), raw={"proceeds_hint_aud": "0.001"})
    res = apply_accounting_policy([ev], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert len(res.dust_ignored) == 1


def test_missing_price_goes_manual_review():
    ev = _ev("buy", quote=_tok("UNSUPPORTED", 100), raw={"valuation_method": "missing_token_price_no_counterparty_leg", "unpriced": True})
    res = apply_accounting_policy([ev], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert res.manual_review[0]["reason"] == "missing_token_price_no_counterparty_leg"


def test_unanchored_token_to_token_group_does_not_mark_components_without_canonical_event():
    out_ev = _ev("transfer_out", base=_tok("TOKA", 5, decimals=0), raw={"signature": "sig-1", "source": "helius_token_transfer"})
    in_ev = _ev("transfer_in", quote=_tok("TOKB", 10, decimals=0), raw={"signature": "sig-1", "source": "helius_token_transfer"})
    res = apply_accounting_policy([out_ev, in_ev], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert len(res.taxable_events) == 0
    assert out_ev.raw.get("swap_component") is None
    assert in_ev.raw.get("swap_component") is None


def test_unanchored_ambiguous_group_stays_manual_review():
    a_out = _ev("transfer_out", base=_tok("TOKA", 5, decimals=0), raw={"signature": "sig-2"})
    b_in = _ev("transfer_in", quote=_tok("TOKB", 3, decimals=0), raw={"signature": "sig-2"})
    c_in = _ev("transfer_in", quote=_tok("TOKC", 2, decimals=0), raw={"signature": "sig-2"})
    res = apply_accounting_policy([a_out, b_in, c_in], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert len(res.taxable_events) == 0
    assert len(res.manual_review) == 3
