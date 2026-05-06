from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.accounting.engine import AccountingEngine, SimplePriceProvider
from sol_cgt.accounting.token_to_token import canonicalize_token_to_token
from sol_cgt.accounting.eligibility import apply_accounting_policy
from sol_cgt.types import NormalizedEvent, TokenAmount


def _tok(m,a,d=0,s=None):
    return TokenAmount(mint=m, amount_raw=a, decimals=d, symbol=s)


def _ev(i,k,**kw):
    if "base" in kw:
        kw["base_token"] = kw.pop("base")
    if "quote" in kw:
        kw["quote_token"] = kw.pop("quote")
    return NormalizedEvent(id=i, ts=datetime(2024,1,1,tzinfo=timezone.utc), kind=k, wallet="W", fee_sol=Decimal("0"), raw=kw.pop("raw",{}), **kw)


def test_token_to_token_creates_carried_cost_lot_and_later_sale_uses_it():
    buy = _ev("b#0","buy",quote=_tok("A",10,s="A"),raw={"cost_aud":"100"})
    out_ev = _ev("s#0","transfer_out",base=_tok("A",5,s="A"),raw={"signature":"sig"})
    in_ev = _ev("s#1","transfer_in",quote=_tok("B",20,s="B"),raw={"signature":"sig"})
    events=[buy,out_ev,in_ev]
    canonicalize_token_to_token(events)
    sale=_ev("x#0","sell",base=_tok("B",20,s="B"),raw={"proceeds_aud":"300"})
    eligible=apply_accounting_policy(events,sol_dust_threshold=Decimal("0.00001"),aud_dust_threshold=Decimal("0.01"),include_dust=False)
    res=AccountingEngine(price_provider=SimplePriceProvider({})).process(eligible.taxable_events+[sale])
    assert any(l.token_mint=="B" and l.qty_acquired==Decimal("20") for l in res.acquisitions)
    assert sum((d.cost_base_aud for d in res.disposals if d.token_mint=="B"),Decimal("0"))==Decimal("50.00")


def test_components_are_excluded_before_eligibility_and_one_canonical_event_created():
    out_ev = _ev("s2#0","transfer_out",base=_tok("A",5,s="A"),raw={"signature":"sig-processed"})
    in_ev = _ev("s2#1","transfer_in",quote=_tok("B",20,s="B"),raw={"signature":"sig-processed"})
    events=[out_ev,in_ev]
    counters = canonicalize_token_to_token(events)
    assert counters["token_to_token_canonical_events_created"] == 1
    assert out_ev.raw.get("swap_component") is True
    assert in_ev.raw.get("swap_component") is True
    eligible=apply_accounting_policy(events,sol_dust_threshold=Decimal("0.00001"),aud_dust_threshold=Decimal("0.01"),include_dust=False)
    assert [e.kind for e in eligible.taxable_events].count("swap") == 1
    assert not any(e.id == out_ev.id for e in eligible.taxable_events)
    assert not any(e.id == in_ev.id for e in eligible.taxable_events)


def test_canonicalization_is_idempotent_and_does_not_duplicate_taxable_acquisition():
    out_ev = _ev("d#0","transfer_out",base=_tok("A",5,s="A"),raw={"signature":"sig-dupe"})
    in_ev = _ev("d#1","transfer_in",quote=_tok("B",20,s="B"),raw={"signature":"sig-dupe"})
    events=[out_ev,in_ev]
    canonicalize_token_to_token(events)
    canonicalize_token_to_token(events)
    canon = [e for e in events if e.kind == "swap" and e.raw.get("valuation_method") == "token_to_token_cost_basis_carry"]
    assert len(canon) == 1
    eligible=apply_accounting_policy(events,sol_dust_threshold=Decimal("0.00001"),aud_dust_threshold=Decimal("0.01"),include_dust=False)
    assert len([e for e in eligible.taxable_events if e.kind == "swap"]) == 1


def test_tiny_sol_with_token_to_token_still_classified_token_to_token_without_separate_anchor_trade():
    out_ev=_ev("t#0","transfer_out",base=_tok("A",5),raw={"signature":"sig2"})
    in_ev=_ev("t#1","transfer_in",quote=_tok("B",10),raw={"signature":"sig2"})
    tiny_sol=_ev("t#2","transfer_out",base=_tok("So11111111111111111111111111111111111111112",1,9),raw={"signature":"sig2","proceeds_hint_aud":"0.01"})
    from sol_cgt.pricing import valuation as valuation_module
    from sol_cgt.pricing import TimestampPriceProvider
    valuation_module.valuate_events(
        [out_ev,in_ev,tiny_sol],
        valuation_module.ValuationContext(usd_provider=TimestampPriceProvider(api_key=None), fx_rate=lambda _: Decimal("1")),
    )
    assert not any(e.raw.get("valuation_method") == "inferred_from_same_signature_anchor" for e in [out_ev, in_ev, tiny_sol])
    c=canonicalize_token_to_token([out_ev,in_ev,tiny_sol])
    assert c["token_to_token_canonical_events_created"]==1


def test_inferred_sell_group_creates_canonical_taxable_disposal_and_excludes_raw_component():
    buy = _ev("b0", "buy", quote=_tok("AAA", 10), raw={"cost_aud": "100"})
    out_ev = _ev("s3#0", "transfer_out", base=_tok("AAA", 5), raw={"signature": "sig-inf-sell", "valuation_method": "inferred_from_same_signature_anchor", "valuation_confidence": "medium", "source": "helius_token_transfer"})
    anchor_in = _ev("s3#1", "transfer_in", quote=_tok("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 20), raw={"signature": "sig-inf-sell", "cost_hint_aud": "50"})
    events = [buy, out_ev, anchor_in]
    canonicalize_token_to_token(events)
    eligible = apply_accounting_policy(events, sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert not any(e.id == out_ev.id for e in eligible.taxable_events)
    assert any(e.id.endswith("#inferred_sell") for e in eligible.taxable_events)
    res = AccountingEngine(price_provider=SimplePriceProvider({})).process(eligible.taxable_events)
    assert len([d for d in res.disposals if d.token_mint == "AAA"]) == 1


def test_inferred_buy_group_creates_canonical_taxable_acquisition_and_excludes_raw_component():
    in_ev = _ev("s4#0", "transfer_in", quote=_tok("BBB", 7), raw={"signature": "sig-inf-buy", "valuation_method": "inferred_from_same_signature_anchor", "valuation_confidence": "medium", "source": "helius_token_transfer"})
    anchor_out = _ev("s4#1", "transfer_out", base=_tok("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 20), raw={"signature": "sig-inf-buy", "proceeds_hint_aud": "70"})
    events = [in_ev, anchor_out]
    canonicalize_token_to_token(events)
    eligible = apply_accounting_policy(events, sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert not any(e.id == in_ev.id for e in eligible.taxable_events)
    assert any(e.id.endswith("#inferred_buy") for e in eligible.taxable_events)


def test_medium_confidence_not_auto_manual_review():
    ev = _ev("m#0", "transfer_out", base=_tok("AAA", 1), raw={"valuation_method": "inferred_from_same_signature_anchor", "valuation_confidence": "medium", "source": "helius_token_transfer", "proceeds_hint_aud": "10"})
    res = apply_accounting_policy([ev], sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert not res.manual_review


def test_ambiguous_multiple_non_anchor_outs_go_manual_review():
    a = _ev("a#0", "transfer_out", base=_tok("AAA", 1), raw={"signature": "sig-amb-out", "valuation_method": "inferred_from_same_signature_anchor", "source": "helius_token_transfer"})
    b = _ev("a#1", "transfer_out", base=_tok("BBB", 1), raw={"signature": "sig-amb-out", "valuation_method": "inferred_from_same_signature_anchor", "source": "helius_token_transfer"})
    c = _ev("a#2", "transfer_in", quote=_tok("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 1), raw={"signature": "sig-amb-out", "cost_hint_aud": "10"})
    events = [a, b, c]
    canonicalize_token_to_token(events)
    assert a.raw.get("accounting_action") == "manual_review"


def test_ambiguous_multiple_non_anchor_ins_go_manual_review():
    a = _ev("b#0", "transfer_in", quote=_tok("AAA", 1), raw={"signature": "sig-amb-in", "valuation_method": "inferred_from_same_signature_anchor", "source": "helius_token_transfer"})
    b = _ev("b#1", "transfer_in", quote=_tok("BBB", 1), raw={"signature": "sig-amb-in", "valuation_method": "inferred_from_same_signature_anchor", "source": "helius_token_transfer"})
    c = _ev("b#2", "transfer_out", base=_tok("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 1), raw={"signature": "sig-amb-in", "proceeds_hint_aud": "10"})
    events = [a, b, c]
    canonicalize_token_to_token(events)
    assert a.raw.get("accounting_action") == "manual_review"
