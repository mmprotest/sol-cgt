from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from ..types import NormalizedEvent


@dataclass
class EligibilityResult:
    taxable_events: list[NormalizedEvent]
    manual_review: list[dict[str, object]]
    dust_ignored: list[dict[str, object]]


def apply_accounting_policy(
    events: Iterable[NormalizedEvent],
    *,
    sol_dust_threshold: Decimal,
    aud_dust_threshold: Decimal,
    include_dust: bool,
) -> EligibilityResult:
    taxable: list[NormalizedEvent] = []
    manual_review: list[dict[str, object]] = []
    dust_ignored: list[dict[str, object]] = []

    for event in events:
        _classify_event(event)
        reason = _policy_reason(event, sol_dust_threshold, aud_dust_threshold, include_dust)
        if reason is None:
            taxable.append(event)
            continue
        row = _manual_row(event, reason)
        if reason == "native_sol_dust":
            event.raw["accounting_action"] = "ignore_dust"
            dust_ignored.append(row)
        else:
            event.raw["accounting_action"] = "manual_review"
            manual_review.append(row)

    return EligibilityResult(taxable_events=taxable, manual_review=manual_review, dust_ignored=dust_ignored)


def _classify_event(event: NormalizedEvent) -> None:
    if event.raw.get("is_internal_transfer") or event.kind == "transfer_internal":
        event.raw["classification"] = "internal_transfer"
        event.raw["accounting_action"] = "internal_transfer"
        event.raw.setdefault("valuation_status", "no_consideration")
        return
    if event.kind == "swap":
        if event.base_token is not None:
            event.raw["classification"] = "swap_sell"
            event.raw["accounting_action"] = "taxable_disposal"
        else:
            event.raw["classification"] = "swap_buy"
            event.raw["accounting_action"] = "taxable_acquisition"
    elif event.kind in {"sell", "transfer_out", "burn"} and event.base_token is not None:
        event.raw["classification"] = "external_transfer_out"
        event.raw["accounting_action"] = "taxable_disposal"
    elif event.kind in {"buy", "transfer_in", "airdrop", "mint"} and event.quote_token is not None:
        event.raw["classification"] = "external_transfer_in"
        event.raw["accounting_action"] = "taxable_acquisition"
    else:
        event.raw["classification"] = "ambiguous"
        event.raw["accounting_action"] = "manual_review"


def _policy_reason(event: NormalizedEvent, sol_dust_threshold: Decimal, aud_dust_threshold: Decimal, include_dust: bool) -> str | None:
    if event.raw.get("accounting_action") in {"internal_transfer", "manual_review"}:
        return None if event.raw.get("accounting_action") == "internal_transfer" else "ambiguous"
    if event.raw.get("unpriced") or event.raw.get("valuation_method") in {"missing_token_price_no_counterparty_leg", "ambiguous_multi_token_swap"}:
        event.raw["valuation_status"] = "missing_price"
        return "missing_token_price_no_counterparty_leg"
    if event.kind == "transfer_in" and not event.raw.get("cost_hint_aud") and not event.raw.get("cost_aud"):
        return "external_transfer_in_unclassified"
    if event.kind == "transfer_out" and not event.raw.get("proceeds_hint_aud") and not event.raw.get("proceeds_aud"):
        return "external_transfer_out_unclassified"
    if event.kind == "swap" and event.quote_token is not None and not (event.raw.get("cost_hint_aud") or event.raw.get("cost_aud")):
        return "swap_missing_consideration_value"

    token = event.base_token or event.quote_token
    if token and token.mint.upper() == "SOL" and token.amount < sol_dust_threshold and not include_dust:
        return "native_sol_dust"
    hint = event.raw.get("proceeds_hint_aud") or event.raw.get("cost_hint_aud")
    if hint is not None and Decimal(str(hint)).copy_abs() < aud_dust_threshold and not include_dust:
        return "native_sol_dust"
    return None


def _manual_row(event: NormalizedEvent, reason: str) -> dict[str, object]:
    token = event.base_token or event.quote_token
    return {
        "timestamp": event.ts.isoformat(),
        "wallet": event.wallet,
        "signature": event.raw.get("signature"),
        "event_id": event.id,
        "event_type": event.kind,
        "token_mint": token.mint if token else None,
        "token_symbol": token.symbol if token else None,
        "amount": str(token.amount) if token else None,
        "reason": reason,
        "classification": event.raw.get("classification", "ambiguous"),
        "valuation_status": event.raw.get("valuation_status", "ambiguous"),
        "suggested_action": "manual_review",
        "raw_reference": event.raw.get("signature") or event.id,
    }
