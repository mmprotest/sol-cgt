"""Acquisition lot ledger management."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple

from ..types import AcquisitionLot


class LotLedger:
    def __init__(self) -> None:
        self._lots: Dict[Tuple[str, str], List[AcquisitionLot]] = defaultdict(list)

    def add_lot(self, lot: AcquisitionLot) -> None:
        key = (lot.wallet, lot.token_mint)
        self._lots[key].append(lot)
        self._lots[key].sort(key=lambda l: (l.ts, l.lot_id))

    def lots_for(self, wallet: str, token_mint: str) -> List[AcquisitionLot]:
        return list(self._lots.get((wallet, token_mint), []))

    def update_remaining(self, lot: AcquisitionLot, qty_used: Decimal) -> None:
        lot.remaining_qty = (lot.remaining_qty - qty_used).quantize(Decimal("0.000000001"))
        if lot.remaining_qty < Decimal("0"):
            lot.remaining_qty = Decimal("0")

    def all_lots(self) -> List[AcquisitionLot]:
        items: List[AcquisitionLot] = []
        for lots in self._lots.values():
            items.extend(lots)
        return items

    def reset(self) -> None:
        self._lots.clear()
