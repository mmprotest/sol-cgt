"""Event taxonomy mapping for AU CGT treatment."""
from __future__ import annotations

from enum import Enum

from ..types import NormalizedEvent


class TaxTreatment(str, Enum):
    acquisition = "acquisition"
    disposal = "disposal"
    income = "income"
    lot_move = "lot_move"
    out_of_scope = "out_of_scope"
    ignore = "ignore"


EVENT_TAX_TREATMENT: dict[str, TaxTreatment] = {
    "swap": TaxTreatment.disposal,
    "sell": TaxTreatment.disposal,
    "buy": TaxTreatment.acquisition,
    "transfer_in": TaxTreatment.acquisition,
    "transfer_out": TaxTreatment.out_of_scope,
    "airdrop": TaxTreatment.income,
    "mint": TaxTreatment.acquisition,
    "burn": TaxTreatment.disposal,
    "wrap": TaxTreatment.disposal,
    "unwrap": TaxTreatment.acquisition,
    "liquidity_add": TaxTreatment.disposal,
    "liquidity_remove": TaxTreatment.acquisition,
    "unknown": TaxTreatment.ignore,
}


def classify_event(event: NormalizedEvent) -> TaxTreatment:
    if "self_transfer" in event.tags:
        return TaxTreatment.lot_move
    return EVENT_TAX_TREATMENT.get(event.kind, TaxTreatment.ignore)
