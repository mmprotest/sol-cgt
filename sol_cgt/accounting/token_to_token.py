from __future__ import annotations

from decimal import Decimal

from ..pricing import WSOL_MINT, normalize_mint
from ..pricing.valuation import SOL_MINT
from ..types import NormalizedEvent, TokenAmount

ANCHOR_MINTS = {normalize_mint(SOL_MINT), normalize_mint(WSOL_MINT), normalize_mint("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"), normalize_mint("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")}
ANCHOR_DUST = Decimal("0.02")


def canonicalize_token_to_token(events: list[NormalizedEvent]) -> dict[str, Decimal | int]:
    events_list = events
    grouped: dict[tuple[str, str], list[NormalizedEvent]] = {}
    for ev in events_list:
        sig = ev.raw.get("signature")
        if sig:
            grouped.setdefault((str(sig), ev.wallet), []).append(ev)
    counters = {
        "token_to_token_groups_detected": 0,
        "token_to_token_canonical_events_created": 0,
        "token_to_token_cost_basis_carried_aud": Decimal("0"),
        "token_to_token_groups_missing_outgoing_lots": 0,
        "token_to_token_component_rows_excluded": 0,
        "inferred_trade_canonical_events_created": 0,
        "inferred_trade_groups_manual_review": 0,
    }

    for (sig, wallet), group in grouped.items():
        if any(e.kind == "swap" for e in group):
            continue
        transfers = [e for e in group if e.kind in {"transfer_in", "transfer_out"} and not e.raw.get("swap_component")]
        if not transfers:
            continue

        out_non_anchor = [e for e in transfers if e.kind == "transfer_out" and e.base_token and normalize_mint(e.base_token.mint) not in ANCHOR_MINTS]
        in_non_anchor = [e for e in transfers if e.kind == "transfer_in" and e.quote_token and normalize_mint(e.quote_token.mint) not in ANCHOR_MINTS]
        anchor_out = [e for e in transfers if e.kind == "transfer_out" and e.base_token and normalize_mint(e.base_token.mint) in ANCHOR_MINTS and e.base_token.amount.copy_abs() > ANCHOR_DUST]
        anchor_in = [e for e in transfers if e.kind == "transfer_in" and e.quote_token and normalize_mint(e.quote_token.mint) in ANCHOR_MINTS and e.quote_token.amount.copy_abs() > ANCHOR_DUST]

        if len(out_non_anchor) > 1 or len(in_non_anchor) > 1:
            for component in transfers:
                component.raw["accounting_action"] = "manual_review"
                component.raw.setdefault("manual_review_reason", "ambiguous_same_signature_group")
            counters["inferred_trade_groups_manual_review"] += 1
            continue

        # token-to-token (existing behavior)
        if len(out_non_anchor) == 1 and len(in_non_anchor) == 1 and not anchor_in and not anchor_out:
            counters["token_to_token_groups_detected"] += 1
            out_ev = out_non_anchor[0]
            in_ev = in_non_anchor[0]
            outgoing = out_ev.base_token
            incoming = in_ev.quote_token
            assert outgoing is not None and incoming is not None
            canonical = NormalizedEvent(
                id=f"{sig}#token_to_token",
                ts=min(e.ts for e in transfers),
                kind="swap",
                base_token=TokenAmount(mint=outgoing.mint, symbol=outgoing.symbol, decimals=outgoing.decimals, amount_raw=outgoing.amount_raw),
                quote_token=TokenAmount(mint=incoming.mint, symbol=incoming.symbol, decimals=incoming.decimals, amount_raw=incoming.amount_raw),
                wallet=wallet,
                counterparty="TOKEN_TO_TOKEN",
                raw={"signature": sig, "valuation_method": "token_to_token_cost_basis_carry", "accounting_action": "taxable_token_to_token_swap"},
                fee_sol=Decimal("0"),
            )
            events_list.append(canonical)
            counters["token_to_token_canonical_events_created"] += 1
            for component in transfers:
                component.raw["swap_component"] = True
                component.raw["accounting_action"] = "component_of_token_to_token_swap"
                component.raw["canonical_replacement_event_id"] = canonical.id
                counters["token_to_token_component_rows_excluded"] += 1
            continue

        # inferred sell
        if len(out_non_anchor) == 1 and len(in_non_anchor) == 0 and anchor_in and not anchor_out:
            out_ev = out_non_anchor[0]
            proceeds = next((e.raw.get("cost_hint_aud") or e.raw.get("proceeds_hint_aud") for e in anchor_in if (e.raw.get("cost_hint_aud") or e.raw.get("proceeds_hint_aud"))), None)
            if proceeds is None:
                for c in transfers:
                    c.raw["accounting_action"] = "manual_review"
                    c.raw.setdefault("manual_review_reason", "missing_anchor_value")
                counters["inferred_trade_groups_manual_review"] += 1
                continue
            outgoing = out_ev.base_token
            assert outgoing is not None
            canonical = NormalizedEvent(
                id=f"{sig}#inferred_sell",
                ts=min(e.ts for e in transfers),
                kind="sell",
                base_token=TokenAmount(mint=outgoing.mint, symbol=outgoing.symbol, decimals=outgoing.decimals, amount_raw=outgoing.amount_raw),
                quote_token=None,
                wallet=wallet,
                counterparty="INFERRED_ANCHOR",
                raw={"signature": sig, "valuation_method": "inferred_same_signature_canonical", "proceeds_hint_aud": str(proceeds), "accounting_action": "taxable_disposal"},
                fee_sol=Decimal("0"),
            )
            events_list.append(canonical)
            counters["inferred_trade_canonical_events_created"] += 1
            for c in transfers:
                c.raw["swap_component"] = True
                c.raw["accounting_action"] = "component_of_inferred_trade"
                c.raw["canonical_replacement_event_id"] = canonical.id
            continue

        # inferred buy
        if len(in_non_anchor) == 1 and len(out_non_anchor) == 0 and anchor_out and not anchor_in:
            in_ev = in_non_anchor[0]
            cost = next((e.raw.get("proceeds_hint_aud") or e.raw.get("cost_hint_aud") for e in anchor_out if (e.raw.get("proceeds_hint_aud") or e.raw.get("cost_hint_aud"))), None)
            if cost is None:
                for c in transfers:
                    c.raw["accounting_action"] = "manual_review"
                    c.raw.setdefault("manual_review_reason", "missing_anchor_value")
                counters["inferred_trade_groups_manual_review"] += 1
                continue
            incoming = in_ev.quote_token
            assert incoming is not None
            canonical = NormalizedEvent(
                id=f"{sig}#inferred_buy",
                ts=min(e.ts for e in transfers),
                kind="buy",
                base_token=None,
                quote_token=TokenAmount(mint=incoming.mint, symbol=incoming.symbol, decimals=incoming.decimals, amount_raw=incoming.amount_raw),
                wallet=wallet,
                counterparty="INFERRED_ANCHOR",
                raw={"signature": sig, "valuation_method": "inferred_same_signature_canonical", "cost_hint_aud": str(cost), "accounting_action": "taxable_acquisition"},
                fee_sol=Decimal("0"),
            )
            events_list.append(canonical)
            counters["inferred_trade_canonical_events_created"] += 1
            for c in transfers:
                c.raw["swap_component"] = True
                c.raw["accounting_action"] = "component_of_inferred_trade"
                c.raw["canonical_replacement_event_id"] = canonical.id
            continue

        if any(e.raw.get("valuation_method") == "inferred_from_same_signature_anchor" for e in transfers):
            for c in transfers:
                c.raw["accounting_action"] = "manual_review"
                c.raw.setdefault("manual_review_reason", "ambiguous_same_signature_group")
            counters["inferred_trade_groups_manual_review"] += 1
    return counters
