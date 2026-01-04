"""Lot selection strategies for disposal matching."""
from __future__ import annotations

from decimal import Decimal
from typing import Dict, Iterable, List, Sequence, Tuple

from ..types import AcquisitionLot, MissingLotIssue

LotAllocation = List[Tuple[AcquisitionLot, Decimal]]


class LotSelectionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        issue: MissingLotIssue | None = None,
        partial_result: object | None = None,
    ) -> None:
        super().__init__(message)
        self.issue = issue
        self.partial_result = partial_result


class SpecificLotMap:
    """Helper class for Specific ID selection."""

    def __init__(self, mapping: Dict[str, List[Tuple[str, Decimal]]]):
        self.mapping = {
            event_id: [(lot_id, Decimal(str(qty))) for lot_id, qty in entries]
            for event_id, entries in mapping.items()
        }

    def lots_for(self, event_id: str) -> List[Tuple[str, Decimal]]:
        return list(self.mapping.get(event_id, []))


def order_lots(lots: Sequence[AcquisitionLot], method: str) -> List[AcquisitionLot]:
    method = method.upper()
    if method == "FIFO":
        return sorted(lots, key=lambda lot: (lot.ts, lot.lot_id))
    if method == "LIFO":
        return sorted(lots, key=lambda lot: (lot.ts, lot.lot_id), reverse=True)
    if method == "HIFO":
        return sorted(lots, key=lambda lot: (lot.unit_cost_aud, lot.ts), reverse=True)
    raise LotSelectionError(f"Unsupported method: {method}")


def allocate(
    lots: Sequence[AcquisitionLot],
    qty: Decimal,
    method: str,
    *,
    specific: SpecificLotMap | None = None,
    event_id: str | None = None,
    issue_context: dict[str, object] | None = None,
) -> LotAllocation:
    if qty <= 0:
        return []
    available = [lot for lot in lots if lot.remaining_qty > 0]
    available_qty = sum((lot.remaining_qty for lot in available), Decimal("0"))
    if not available:
        issue = _build_missing_lot_issue(
            issue_context,
            required_qty=qty,
            available_qty=available_qty,
            message="No acquisition lots available",
        )
        raise LotSelectionError("No acquisition lots available", issue=issue)
    if method.upper() == "SPECIFIC":
        if specific is None or not event_id:
            raise LotSelectionError("Specific ID method requires mapping and event_id")
        selections = specific.lots_for(event_id)
        if not selections:
            raise LotSelectionError(f"No specific lots defined for {event_id}")
        ordered: List[AcquisitionLot] = []
        plan: List[Tuple[AcquisitionLot, Decimal]] = []
        lot_lookup = {lot.lot_id: lot for lot in available}
        for lot_id, lot_qty in selections:
            lot = lot_lookup.get(lot_id)
            if lot is None:
                raise LotSelectionError(f"Lot {lot_id} not available for {event_id}")
            if lot_qty > lot.remaining_qty:
                issue = _build_missing_lot_issue(
                    issue_context,
                    required_qty=qty,
                    available_qty=available_qty,
                    message=f"Lot {lot_id} insufficient quantity for {event_id}",
                )
                raise LotSelectionError(
                    f"Lot {lot_id} insufficient quantity for {event_id}",
                    issue=issue,
                )
            plan.append((lot, lot_qty))
        total = sum((alloc for _, alloc in plan), Decimal("0"))
        if total != qty:
            raise LotSelectionError(
                f"Specific allocation quantity mismatch for {event_id}: {total} vs {qty}"
            )
        return plan
    ordered = order_lots(available, method)
    remaining = qty
    allocation: LotAllocation = []
    for lot in ordered:
        if remaining <= 0:
            break
        take = min(lot.remaining_qty, remaining)
        if take > 0:
            allocation.append((lot, take))
            remaining -= take
    if remaining > 0:
        issue = _build_missing_lot_issue(
            issue_context,
            required_qty=qty,
            available_qty=available_qty,
            message="Insufficient lot quantity to satisfy disposal",
        )
        raise LotSelectionError("Insufficient lot quantity to satisfy disposal", issue=issue)
    return allocation


def _build_missing_lot_issue(
    context: dict[str, object] | None,
    *,
    required_qty: Decimal,
    available_qty: Decimal,
    message: str,
) -> MissingLotIssue | None:
    if not context:
        return None
    if context.get("ts") is None:
        return None
    shortfall = required_qty - available_qty
    return MissingLotIssue(
        wallet=str(context.get("wallet") or ""),
        mint=str(context.get("mint") or ""),
        symbol=context.get("symbol") if context.get("symbol") is not None else None,
        ts=context.get("ts"),  # type: ignore[arg-type]
        signature=context.get("signature") if context.get("signature") is not None else None,
        event_id=str(context.get("event_id") or ""),
        event_type=str(context.get("event_type") or ""),
        required_qty=required_qty,
        available_qty=available_qty,
        shortfall_qty=shortfall if shortfall > 0 else Decimal("0"),
        message=message,
    )
