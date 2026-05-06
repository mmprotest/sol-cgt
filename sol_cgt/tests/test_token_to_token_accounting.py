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


def test_tiny_sol_with_token_to_token_still_classified_token_to_token():
    out_ev=_ev("t#0","transfer_out",base=_tok("A",5),raw={"signature":"sig2"})
    in_ev=_ev("t#1","transfer_in",quote=_tok("B",10),raw={"signature":"sig2"})
    sol=_ev("t#2","transfer_out",base=_tok("So11111111111111111111111111111111111111112",1,9),raw={"signature":"sig2"})
    events=[out_ev,in_ev,sol]
    c=canonicalize_token_to_token(events)
    assert c["token_to_token_canonical_events_created"]==1


def test_missing_outgoing_lots_stays_manual_review_and_components_not_excluded():
    out_ev=_ev("m#0","transfer_out",base=_tok("A",5),raw={"signature":"sig3"})
    in_ev=_ev("m#1","transfer_in",quote=_tok("B",10),raw={"signature":"sig3"})
    events=[out_ev,in_ev]
    canonicalize_token_to_token(events)
    eligible=apply_accounting_policy(events,sol_dust_threshold=Decimal("0.00001"),aud_dust_threshold=Decimal("0.01"),include_dust=False)
    engine=AccountingEngine(price_provider=SimplePriceProvider({}))
    res=engine.process(eligible.taxable_events,strict_lots=False)
    assert not any(a.token_mint=="B" for a in res.acquisitions)
    canonical = next(e for e in eligible.taxable_events if e.kind == "swap")
    assert canonical.raw.get("manual_review_reason") == "token_to_token_missing_outgoing_lots"
    assert any(w.code == "missing_lot_history" and canonical.id in w.message for w in res.warnings)
    assert out_ev.raw.get("swap_component") is None
    assert in_ev.raw.get("swap_component") is None


def test_components_not_excluded_before_canonical_processed():
    out_ev = _ev("s2#0","transfer_out",base=_tok("A",5,s="A"),raw={"signature":"sig-processed"})
    in_ev = _ev("s2#1","transfer_in",quote=_tok("B",20,s="B"),raw={"signature":"sig-processed"})
    events=[out_ev,in_ev]
    canonicalize_token_to_token(events)
    assert out_ev.raw.get("swap_component") is None
    assert in_ev.raw.get("swap_component") is None
