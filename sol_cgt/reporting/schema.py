"""Column schemas for report outputs."""
from __future__ import annotations

ACQUISITION_COLUMNS = [
    "lot_id",
    "wallet",
    "ts",
    "token_mint",
    "token_symbol",
    "qty_acquired",
    "unit_cost_aud",
    "fees_aud",
    "remaining_qty",
    "source_event",
    "source_type",
]

DISPOSAL_COLUMNS = [
    "event_id",
    "wallet",
    "ts",
    "token_mint",
    "token_symbol",
    "qty_disposed",
    "proceeds_aud",
    "cost_base_aud",
    "fees_aud",
    "gain_loss_aud",
    "long_term",
    "held_days",
    "method",
    "signature",
    "notes",
]

SUMMARY_BY_TOKEN_COLUMNS = [
    "token_mint",
    "token_symbol",
    "total_disposals_qty",
    "total_proceeds_aud",
    "total_cost_base_aud",
    "fees_aud",
    "net_gain_loss_aud",
    "discount_eligible_gain_aud",
    "disposals",
]

SUMMARY_OVERALL_COLUMNS = [
    "proceeds_aud",
    "cost_base_aud",
    "fees_aud",
    "gain_loss_aud",
    "discount_eligible_gain_aud",
    "disposals",
]

WALLET_SUMMARY_COLUMNS = [
    "wallet",
    "total_proceeds_aud",
    "total_cost_base_aud",
    "fees_aud",
    "net_gain_loss_aud",
    "discount_eligible_gain_aud",
    "disposals",
]
