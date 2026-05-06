from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from ..pricing import WSOL_MINT, normalize_mint
from ..pricing.valuation import SOL_MINT
from ..types import NormalizedEvent, TokenAmount

ANCHOR_MINTS = {normalize_mint(SOL_MINT), normalize_mint(WSOL_MINT), normalize_mint("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"), normalize_mint("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")}
ANCHOR_DUST = Decimal("0.02")
REL_DUST = Decimal("0.000001")


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
    }
    for (sig, wallet), group in grouped.items():
        transfers = [e for e in group if e.kind in {"transfer_in", "transfer_out"} and not e.raw.get("swap_component")]
        if not transfers:
            continue
        deltas: dict[str, Decimal] = {}
        token_by_mint: dict[str, TokenAmount] = {}
        for ev in transfers:
            tok = ev.base_token if ev.kind == "transfer_out" else ev.quote_token
            if tok is None:
                continue
            mint = normalize_mint(tok.mint)
            amt = tok.amount if ev.kind == "transfer_in" else -tok.amount
            deltas[mint] = deltas.get(mint, Decimal("0")) + amt
            token_by_mint[mint] = tok
        non_zero = {m: q for m, q in deltas.items() if q != 0}
        if len(non_zero) < 2:
            continue
        non_anchor = {m: q for m, q in non_zero.items() if m not in ANCHOR_MINTS}
        neg = [(m, q) for m, q in non_anchor.items() if q < 0]
        pos = [(m, q) for m, q in non_anchor.items() if q > 0]
        anchor_material = any(m in ANCHOR_MINTS and q.copy_abs() > ANCHOR_DUST for m, q in non_zero.items())
        if len(neg) != 1 or len(pos) != 1 or neg[0][0] == pos[0][0] or anchor_material:
            continue
        counters["token_to_token_groups_detected"] += 1
        out_m, out_q = neg[0]
        in_m, in_q = pos[0]
        outgoing = token_by_mint[out_m]
        incoming = token_by_mint[in_m]
        canonical = NormalizedEvent(
            id=f"{sig}#token_to_token",
            ts=min(e.ts for e in transfers),
            kind="swap",
            base_token=TokenAmount(mint=outgoing.mint, symbol=outgoing.symbol, decimals=outgoing.decimals, amount_raw=int((-out_q) * (Decimal(10) ** outgoing.decimals))),
            quote_token=TokenAmount(mint=incoming.mint, symbol=incoming.symbol, decimals=incoming.decimals, amount_raw=int(in_q * (Decimal(10) ** incoming.decimals))),
            wallet=wallet,
            counterparty="TOKEN_TO_TOKEN",
            raw={"signature": sig, "valuation_method": "token_to_token_cost_basis_carry", "accounting_action": "taxable_token_to_token_swap"},
            fee_sol=Decimal("0"),
        )
        events_list.append(canonical)
        for ev in transfers:
            ev.raw["swap_component"] = True
            ev.raw["accounting_action"] = "component_of_token_to_token_swap"
            counters["token_to_token_component_rows_excluded"] += 1
        counters["token_to_token_canonical_events_created"] += 1
    return counters
