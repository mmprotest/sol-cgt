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
