"""Summary helpers for reporting."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Iterable

from ..types import DisposalRecord
from ..types import NormalizedEvent


def summarize_by_token(disposals: Iterable[DisposalRecord]) -> list[dict[str, object]]:
    aggregates: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "proceeds_aud": Decimal("0"),
            "cost_base_aud": Decimal("0"),
            "fees_aud": Decimal("0"),
            "gain_loss_aud": Decimal("0"),
            "disposals": Decimal("0"),
            "qty_disposed": Decimal("0"),
            "discount_eligible_gain_aud": Decimal("0"),
            "token_symbol": "",
        }
    )
    for record in disposals:
        bucket = aggregates[record.token_mint]
        bucket["token_symbol"] = record.token_symbol or bucket["token_symbol"]
        bucket["proceeds_aud"] += record.proceeds_aud
        bucket["cost_base_aud"] += record.cost_base_aud
        bucket["fees_aud"] += record.fees_aud
        bucket["gain_loss_aud"] += record.gain_loss_aud
        bucket["qty_disposed"] += record.qty_disposed
        bucket["disposals"] += Decimal("1")
        if record.long_term and record.gain_loss_aud > 0:
            bucket["discount_eligible_gain_aud"] += record.gain_loss_aud
    rows = []
    for mint, data in aggregates.items():
        rows.append(
            {
                "token_mint": mint,
                "token_symbol": data["token_symbol"],
                "total_disposals_qty": float(data["qty_disposed"]),
                "total_proceeds_aud": float(data["proceeds_aud"]),
                "total_cost_base_aud": float(data["cost_base_aud"]),
                "fees_aud": float(data["fees_aud"]),
                "net_gain_loss_aud": float(data["gain_loss_aud"]),
                "discount_eligible_gain_aud": float(data["discount_eligible_gain_aud"]),
                "disposals": int(data["disposals"]),
            }
        )
    return rows


def summarize_overall(disposals: Iterable[DisposalRecord]) -> list[dict[str, object]]:
    proceeds = Decimal("0")
    cost = Decimal("0")
    fees = Decimal("0")
    gain = Decimal("0")
    discount_eligible_gain = Decimal("0")
    count = 0
    for record in disposals:
        proceeds += record.proceeds_aud
        cost += record.cost_base_aud
        fees += record.fees_aud
        gain += record.gain_loss_aud
        if record.long_term and record.gain_loss_aud > 0:
            discount_eligible_gain += record.gain_loss_aud
        count += 1
    return [
        {
            "proceeds_aud": float(proceeds),
            "cost_base_aud": float(cost),
            "fees_aud": float(fees),
            "gain_loss_aud": float(gain),
            "discount_eligible_gain_aud": float(discount_eligible_gain),
            "disposals": count,
        }
    ] if count else []


def summarize_by_wallet(disposals: Iterable[DisposalRecord]) -> list[dict[str, object]]:
    aggregates: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "proceeds_aud": Decimal("0"),
            "cost_base_aud": Decimal("0"),
            "fees_aud": Decimal("0"),
            "gain_loss_aud": Decimal("0"),
            "discount_eligible_gain_aud": Decimal("0"),
            "disposals": Decimal("0"),
        }
    )
    for record in disposals:
        bucket = aggregates[record.wallet]
        bucket["proceeds_aud"] += record.proceeds_aud
        bucket["cost_base_aud"] += record.cost_base_aud
        bucket["fees_aud"] += record.fees_aud
        bucket["gain_loss_aud"] += record.gain_loss_aud
        if record.long_term and record.gain_loss_aud > 0:
            bucket["discount_eligible_gain_aud"] += record.gain_loss_aud
        bucket["disposals"] += Decimal("1")
    rows = []
    for wallet, data in aggregates.items():
        rows.append(
            {
                "wallet": wallet,
                "total_proceeds_aud": float(data["proceeds_aud"]),
                "total_cost_base_aud": float(data["cost_base_aud"]),
                "fees_aud": float(data["fees_aud"]),
                "net_gain_loss_aud": float(data["gain_loss_aud"]),
                "discount_eligible_gain_aud": float(data["discount_eligible_gain_aud"]),
                "disposals": int(data["disposals"]),
            }
        )
    return rows


def build_transaction_summary(events: Iterable[NormalizedEvent]) -> list[dict[str, object]]:
    by_sig: dict[str, list[NormalizedEvent]] = defaultdict(list)
    for event in events:
        sig = event.raw.get("signature") or event.id.split("#")[0]
        by_sig[str(sig)].append(event)
    rows: list[dict[str, object]] = []
    for signature, sig_events in sorted(by_sig.items()):
        kinds = {e.kind for e in sig_events}
        has_trade = "swap" in kinds or any("swap" in str(e.raw.get("classification", "")) for e in sig_events)
        if has_trade:
            classification = "trade"
        elif any(e.raw.get("accounting_action") == "manual_review" or e.raw.get("manual_review_reason") for e in sig_events):
            classification = "mixed_or_ambiguous"
        elif kinds == {"transfer_in"} and not any(e.raw.get("is_internal_transfer") for e in sig_events):
            classification = "deposit"
        elif kinds == {"transfer_out"} and not any(e.raw.get("is_internal_transfer") for e in sig_events):
            classification = "withdrawal"
        elif "transfer_internal" in kinds or any(e.raw.get("is_internal_transfer") for e in sig_events):
            classification = "internal_transfer"
        elif all((e.base_token is None and e.quote_token is None and e.fee_sol > 0) for e in sig_events):
            classification = "dust_or_noise"
        elif all(e.raw.get("accounting_action") == "ignore_dust" for e in sig_events):
            classification = "dust_or_noise"
        elif len(kinds) > 1:
            classification = "mixed_or_ambiguous"
        else:
            classification = "mixed_or_ambiguous"
        rows.append(
            {
                "signature": signature,
                "timestamp": min(e.ts for e in sig_events).isoformat(),
                "event_count": len(sig_events),
                "classification": classification,
                "review_status": "needs_review" if any(e.raw.get("accounting_action") == "manual_review" for e in sig_events) else "ok",
                "wallets": ",".join(sorted({e.wallet for e in sig_events})),
            }
        )
    return rows


def build_reconciliation_summary(raw_signatures: set[str], transaction_summary: list[dict[str, object]], events: Iterable[NormalizedEvent]) -> list[dict[str, object]]:
    tx_sigs = {str(row.get("signature")) for row in transaction_summary}
    event_sigs = {str((event.raw.get("signature") or event.id.split("#")[0])) for event in events}
    counts: dict[str, int] = defaultdict(int)
    for row in transaction_summary:
        counts[str(row.get("classification") or "unknown")] += 1
    return [{
        "raw_signatures_loaded": len(raw_signatures),
        "normalized_signatures_produced": len(tx_sigs),
        "raw_signatures_with_zero_normalized_events": len(raw_signatures - tx_sigs),
        "normalized_signatures_missing_from_raw_source": len(tx_sigs - raw_signatures),
        "signatures_with_multiple_normalized_rows": sum(1 for row in transaction_summary if int(row.get("event_count") or 0) > 1),
        "trade": counts.get("trade", 0),
        "deposit": counts.get("deposit", 0),
        "withdrawal": counts.get("withdrawal", 0),
        "internal_transfer": counts.get("internal_transfer", 0),
        "dust_or_noise": counts.get("dust_or_noise", 0),
        "manual_review": sum(1 for row in transaction_summary if row.get("review_status") == "needs_review"),
        "mixed_or_ambiguous": counts.get("mixed_or_ambiguous", 0),
        "normalized_event_signatures_seen": len(event_sigs),
    }]
