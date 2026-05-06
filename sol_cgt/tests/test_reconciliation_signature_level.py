from datetime import datetime, timezone
from decimal import Decimal

from sol_cgt.reporting import summaries
from sol_cgt.types import NormalizedEvent, TokenAmount


def _ev(sig: str, kind: str, wallet: str = "W", manual: bool = False) -> NormalizedEvent:
    raw = {"signature": sig}
    if manual:
        raw["accounting_action"] = "manual_review"
    tok = TokenAmount(mint="M", symbol="M", decimals=6, amount_raw=1)
    return NormalizedEvent(id=f"{sig}:{kind}", ts=datetime.now(timezone.utc), wallet=wallet, kind=kind, base_token=tok if kind != "transfer_in" else None, quote_token=tok if kind == "transfer_in" else None, fee_sol=Decimal("0"), raw=raw)


def test_signature_level_trade_classification_with_swap_plus_transfer() -> None:
    evs = [_ev("s1", "swap"), _ev("s1", "transfer_out")]
    rows = summaries.build_transaction_summary(evs)
    assert len(rows) == 1
    assert rows[0]["classification"] == "trade"
    assert evs[1].raw.get("swap_component") is None


def test_reconciliation_counts_signature_level_not_event_level() -> None:
    evs = [_ev("s1", "swap"), _ev("s1", "transfer_out"), _ev("s2", "transfer_in")]
    tx = summaries.build_transaction_summary(evs)
    rec = summaries.build_reconciliation_summary({"s1", "s2", "s3"}, tx, evs)[0]
    assert rec["raw_signatures_loaded"] == 3
    assert rec["normalized_signatures_produced"] == 2
    assert rec["raw_signatures_with_zero_normalized_events"] == 1
    assert rec["trade"] == 1
    assert rec["deposit"] == 1
