"""Core accounting engine to transform events into disposals and acquisitions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Protocol

from .. import utils
from ..reconciliation.transfers import TransferMatch
from ..types import (
    AcquisitionLot,
    DisposalRecord,
    LotMoveRecord,
    MissingLotIssue,
    NormalizedEvent,
    TokenAmount,
    WarningRecord,
)
from . import lots as lots_module
from . import methods


class PriceProvider(Protocol):
    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Optional[Decimal]:
        ...


@dataclass
class AccountingResult:
    acquisitions: List[AcquisitionLot]
    disposals: List[DisposalRecord]
    lot_moves: List[LotMoveRecord]
    warnings: List[WarningRecord]


@dataclass
class SimplePriceProvider:
    """Price provider that resolves from event hints or static overrides."""

    overrides: Dict[str, Decimal]

    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Optional[Decimal]:  # noqa: D401
        context = context or {}
        # direct hints
        hints = context.get("price_aud")
        if isinstance(hints, dict) and mint in hints:
            return Decimal(str(hints[mint]))
        key = f"{mint}_price_aud"
        if key in context:
            return Decimal(str(context[key]))
        if mint in self.overrides:
            return self.overrides[mint]
        return None


class AccountingEngine:
    def __init__(
        self,
        *,
        method: str = "FIFO",
        price_provider: Optional[PriceProvider] = None,
        specific_map: Optional[Dict[str, List[tuple[str, Decimal]]]] = None,
        long_term_days: int = 365,
    ) -> None:
        self.method = method.upper()
        self.price_provider = price_provider or SimplePriceProvider({})
        self.long_term_days = long_term_days
        self.ledger = lots_module.LotLedger()
        self.specific = methods.SpecificLotMap(specific_map or {}) if specific_map else None

    def process(
        self,
        events: Iterable[NormalizedEvent],
        *,
        wallets: Optional[Iterable[str]] = None,
        transfer_matches: Optional[Iterable[TransferMatch]] = None,
        external_lot_tracking: bool = False,
        strict_lots: bool = True,
        missing_lot_issues: Optional[List[MissingLotIssue]] = None,
    ) -> AccountingResult:
        acquisitions: List[AcquisitionLot] = []
        disposals: List[DisposalRecord] = []
        lot_moves: List[LotMoveRecord] = []
        warnings: List[WarningRecord] = []
        wallet_set = {wallet for wallet in wallets} if wallets else set()
        matches = list(transfer_matches or [])
        match_by_out = {match.out_event.id: match for match in matches}
        matched_in_ids = {match.in_event.id for match in matches}
        swap_hint_warned: set[str] = set()
        default_decimals_warned: set[tuple[Optional[str], str]] = set()
        missing_price_warned: set[tuple[str, str, str]] = set()

        for event in sorted(events, key=lambda ev: (ev.ts, ev.id)):
            if event.id in matched_in_ids:
                continue
            if event.id in match_by_out:
                try:
                    move_record, moved_lots = self._handle_self_transfer(
                        match_by_out[event.id],
                        warnings,
                        missing_price_warned,
                    )
                    lot_moves.append(move_record)
                    acquisitions.extend(moved_lots)
                    continue
                except methods.LotSelectionError as exc:
                    issue = exc.issue
                    if issue and missing_lot_issues is not None:
                        missing_lot_issues.append(issue)
                    if strict_lots:
                        raise methods.LotSelectionError(
                            str(exc),
                            issue=issue,
                            partial_result=AccountingResult(
                                acquisitions=acquisitions,
                                disposals=disposals,
                                lot_moves=lot_moves,
                                warnings=warnings,
                            ),
                        ) from exc
                    if issue is not None:
                        synthetic = self._add_synthetic_lot(issue, event, source_type="synthetic_missing_basis")
                        acquisitions.append(synthetic)
                        warnings.append(
                            WarningRecord(
                                ts=event.ts,
                                wallet=event.wallet,
                                signature=event.raw.get("signature"),
                                code="missing_lots_synthetic",
                                message=(
                                    "Synthetic acquisition lot created to cover missing basis; "
                                    "gain/loss results are unreliable."
                                ),
                            )
                        )
                        move_record, moved_lots = self._handle_self_transfer(
                            match_by_out[event.id],
                            warnings,
                            missing_price_warned,
                        )
                        lot_moves.append(move_record)
                        acquisitions.extend(moved_lots)
                        continue
                    raise
            if event.kind == "transfer_out" and event.base_token is not None:
                if not event.counterparty or event.counterparty not in wallet_set:
                    move_record, moved_lots = self._handle_out_of_scope_transfer(
                        event,
                        warnings,
                        missing_price_warned,
                        external_lot_tracking,
                    )
                    if move_record is not None:
                        lot_moves.append(move_record)
                    acquisitions.extend(moved_lots)
                    continue
            if event.kind == "transfer_in" and event.quote_token is not None:
                if not event.counterparty or event.counterparty not in wallet_set:
                    moved = False
                    if external_lot_tracking:
                        moved, moved_lots = self._handle_external_return(
                            event,
                            lot_moves,
                            warnings,
                            missing_price_warned,
                        )
                        if moved:
                            acquisitions.extend(moved_lots)
                            continue
                    warnings.append(
                        WarningRecord(
                            ts=event.ts,
                            wallet=event.wallet,
                            signature=event.raw.get("signature"),
                            code="external_transfer_in",
                            message=(
                                "Transfer in from external wallet treated as acquisition at spot price."
                                if external_lot_tracking
                                else "Transfer in from external wallet treated as acquisition at spot price. "
                                "Enable external_lot_tracking to attempt matching."
                            ),
                        )
                    )
            if event.kind == "swap":
                signature = event.raw.get("signature")
                if signature and event.raw.get("swap_hint_missing") and signature not in swap_hint_warned:
                    swap_hint_warned.add(signature)
                    missing = event.raw.get("swap_hint_missing")
                    warnings.append(
                        WarningRecord(
                            ts=event.ts,
                            wallet=event.wallet,
                            signature=signature,
                            code="swap_hint_missing_prices",
                            message=f"Swap pricing hints missing for mints: {missing}",
                        )
                    )
            defaulted_mints = event.raw.get("decimals_defaulted_mints")
            if isinstance(defaulted_mints, list):
                for mint in defaulted_mints:
                    key = (event.raw.get("signature"), mint)
                    if key in default_decimals_warned:
                        continue
                    default_decimals_warned.add(key)
                    warnings.append(
                        WarningRecord(
                            ts=event.ts,
                            wallet=event.wallet,
                            signature=event.raw.get("signature"),
                            code="default_decimals",
                            message=(
                                f"Token decimals missing for mint {mint}; defaulted to 0. "
                                "Verify token metadata."
                            ),
                        )
                    )

            base_token = event.base_token
            quote_token = event.quote_token
            fee_aud = self._fee_to_aud(event, warnings, missing_price_warned)
            event.raw["fee_aud"] = str(fee_aud)
            proceeds_aud: Optional[Decimal] = None
            if base_token is not None and base_token.amount > 0:
                proceeds_aud = self._resolve_proceeds(
                    event,
                    base_token,
                    quote_token,
                    warnings,
                    missing_price_warned,
                )
                try:
                    disposals.extend(
                        self._handle_disposal(event, base_token, proceeds_aud, fee_aud)
                    )
                except methods.LotSelectionError as exc:
                    issue = exc.issue
                    if issue and missing_lot_issues is not None:
                        missing_lot_issues.append(issue)
                    if strict_lots:
                        raise methods.LotSelectionError(
                            str(exc),
                            issue=issue,
                            partial_result=AccountingResult(
                                acquisitions=acquisitions,
                                disposals=disposals,
                                lot_moves=lot_moves,
                                warnings=warnings,
                            ),
                        ) from exc
                    if issue is None:
                        raise
                    synthetic = self._add_synthetic_lot(issue, event, source_type="synthetic_missing_basis")
                    acquisitions.append(synthetic)
                    warnings.append(
                        WarningRecord(
                            ts=event.ts,
                            wallet=event.wallet,
                            signature=event.raw.get("signature"),
                            code="missing_lots_synthetic",
                            message=(
                                "Synthetic acquisition lot created to cover missing basis; "
                                "gain/loss results are unreliable."
                            ),
                        )
                    )
                    new_records = self._handle_disposal(event, base_token, proceeds_aud, fee_aud)
                    for record in new_records:
                        record.notes = _append_note(record.notes, "unreliable_missing_lots")
                    disposals.extend(new_records)
            if quote_token is not None and quote_token.amount > 0:
                acquisitions.append(
                    self._handle_acquisition(
                        event,
                        quote_token,
                        proceeds_aud,
                        fee_aud,
                        warnings,
                        missing_price_warned,
                    )
                )
        return AccountingResult(
            acquisitions=acquisitions,
            disposals=disposals,
            lot_moves=lot_moves,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    def _fee_to_aud(
        self,
        event: NormalizedEvent,
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
    ) -> Decimal:
        if event.fee_sol == 0:
            return Decimal("0")
        price = self.price_provider.price_aud("SOL", event.ts, context=event.raw)
        if price is None:
            self._warn_missing_price(event, "SOL", warnings, warned, code="missing_fee_price")
            return Decimal("0")
        return utils.quantize_aud(event.fee_sol * price)

    def _resolve_price(
        self,
        event: NormalizedEvent,
        token: TokenAmount,
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
    ) -> Optional[Decimal]:
        context = dict(event.raw)
        context.setdefault("mint", token.mint)
        price = self.price_provider.price_aud(token.mint, event.ts, context=context)
        if price is None:
            hint_key = "price_aud"
            if hint_key in event.raw:
                value = event.raw[hint_key]
                if isinstance(value, (int, float, str)):
                    return Decimal(str(value))
            self._warn_missing_price(event, token.mint, warnings, warned)
            return None
        return price

    def _resolve_proceeds(
        self,
        event: NormalizedEvent,
        base: TokenAmount,
        quote: Optional[TokenAmount],
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
    ) -> Decimal:
        if "proceeds_aud" in event.raw:
            return Decimal(str(event.raw["proceeds_aud"]))
        if "proceeds_hint_aud" in event.raw:
            return Decimal(str(event.raw["proceeds_hint_aud"]))
        if "proceeds_hint_usd" in event.raw:
            return self._convert_usd_hint(event, "proceeds_hint_usd")
        if quote is not None:
            price = self._resolve_price(event, quote, warnings, warned)
            if price is None:
                return Decimal("0")
            return utils.quantize_aud(price * quote.amount)
        price = self._resolve_price(event, base, warnings, warned)
        if price is None:
            return Decimal("0")
        return utils.quantize_aud(price * base.amount)

    def _handle_disposal(
        self,
        event: NormalizedEvent,
        base: TokenAmount,
        proceeds_aud: Decimal,
        fee_aud: Decimal,
    ) -> List[DisposalRecord]:
        lots = self.ledger.lots_for(event.wallet, base.mint)
        allocation = methods.allocate(
            lots,
            base.amount,
            self.method,
            specific=self.specific,
            event_id=event.id,
            issue_context=_issue_context(event, base),
        )
        if not allocation:
            raise methods.LotSelectionError(
                f"No lots available for disposal {event.id}"
            )
        records: List[DisposalRecord] = []
        gross_proceeds = proceeds_aud
        for lot, qty_used in allocation:
            portion = qty_used / base.amount if base.amount > 0 else Decimal("0")
            remaining_qty = lot.remaining_qty if lot.remaining_qty else lot.qty_acquired
            lot_fee_share = (
                lot.fees_aud * (qty_used / remaining_qty) if remaining_qty else Decimal("0")
            )
            lot_cost = utils.quantize_aud(lot.unit_cost_aud * qty_used)
            fee_share = utils.quantize_aud(fee_aud * portion)
            proceeds_share = utils.quantize_aud(gross_proceeds * portion)
            cost_base = lot_cost + lot_fee_share
            gain_loss = utils.quantize_aud(proceeds_share - fee_share - cost_base)
            held_days = utils.holding_period_days(lot.ts, event.ts)
            long_term = held_days >= self.long_term_days
            record = DisposalRecord(
                event_id=event.id,
                wallet=event.wallet,
                ts=event.ts,
                token_mint=base.mint,
                token_symbol=base.symbol,
                qty_disposed=qty_used,
                proceeds_aud=proceeds_share,
                cost_base_aud=cost_base,
                fees_aud=fee_share,
                gain_loss_aud=gain_loss,
                long_term=long_term,
                held_days=held_days,
                method=self.method,
                signature=event.raw.get("signature"),
                notes=f"lot_id={lot.lot_id}",
            )
            records.append(record)
            lot.fees_aud = lot.fees_aud - lot_fee_share
            if lot.fees_aud < Decimal("0"):
                lot.fees_aud = Decimal("0")
            self.ledger.update_remaining(lot, qty_used)
        return records

    def _handle_acquisition(
        self,
        event: NormalizedEvent,
        quote: TokenAmount,
        proceeds_hint: Optional[Decimal],
        fee_aud: Decimal,
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
    ) -> AcquisitionLot:
        qty = quote.amount
        if qty <= 0:
            raise ValueError("Acquisition quantity must be positive")
        if "cost_aud" in event.raw:
            total_cost = Decimal(str(event.raw["cost_aud"]))
        elif "cost_hint_aud" in event.raw:
            total_cost = Decimal(str(event.raw["cost_hint_aud"]))
        elif "cost_hint_usd" in event.raw:
            total_cost = self._convert_usd_hint(event, "cost_hint_usd")
        elif proceeds_hint is not None:
            total_cost = proceeds_hint
        else:
            price = self._resolve_price(event, quote, warnings, warned)
            if price is None:
                total_cost = Decimal("0")
            else:
                total_cost = utils.quantize_aud(price * qty)
        lot_fee = fee_aud if event.base_token is None else Decimal("0")
        unit_cost = utils.quantize_aud(total_cost / qty) if qty != 0 else Decimal("0")
        lot = AcquisitionLot(
            lot_id=f"{event.id}:{quote.mint}",
            wallet=event.wallet,
            ts=event.ts,
            token_mint=quote.mint,
            token_symbol=quote.symbol,
            qty_acquired=qty,
            unit_cost_aud=unit_cost,
            fees_aud=lot_fee,
            remaining_qty=qty,
            source_event=event.id,
            source_type=event.kind,
        )
        self.ledger.add_lot(lot)
        return lot

    def _handle_self_transfer(
        self,
        match: TransferMatch,
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
    ) -> tuple[LotMoveRecord, List[AcquisitionLot]]:
        out_event = match.out_event
        in_event = match.in_event
        if out_event.base_token is None or in_event.quote_token is None:
            raise ValueError("Self transfer events missing token data")
        token = out_event.base_token
        fee_aud = self._fee_to_aud(out_event, warnings, warned)
        allocation = methods.allocate(
            self.ledger.lots_for(out_event.wallet, token.mint),
            token.amount,
            "FIFO",
            event_id=out_event.id,
            issue_context=_issue_context(out_event, token),
        )
        move_fee_allocations = allocate_fee_over_consumed_parts(
            [(lot, qty_used) for lot, qty_used in allocation],
            fee_aud,
        )
        lots_consumed: List[dict[str, str]] = []
        lots_created: List[dict[str, str]] = []
        moved_lots: List[AcquisitionLot] = []
        for (lot, qty_used), allocated_move_fee in zip(allocation, move_fee_allocations):
            remaining_qty = lot.remaining_qty if lot.remaining_qty else lot.qty_acquired
            portion = qty_used / remaining_qty if remaining_qty else Decimal("0")
            moved_fees = lot.fees_aud * portion
            lot.fees_aud = (lot.fees_aud - moved_fees)
            if lot.fees_aud < Decimal("0"):
                lot.fees_aud = Decimal("0")
            self.ledger.update_remaining(lot, qty_used)
            new_lot = AcquisitionLot(
                lot_id=f"{lot.lot_id}:move:{out_event.id}",
                wallet=in_event.wallet,
                ts=lot.ts,
                token_mint=lot.token_mint,
                token_symbol=lot.token_symbol,
                qty_acquired=qty_used,
                unit_cost_aud=lot.unit_cost_aud,
                fees_aud=moved_fees + allocated_move_fee,
                remaining_qty=qty_used,
                source_event=out_event.id,
                source_type="lot_move",
            )
            self.ledger.add_lot(new_lot)
            moved_lots.append(new_lot)
            lots_consumed.append({"lot_id": lot.lot_id, "qty": str(qty_used)})
            lots_created.append({"lot_id": new_lot.lot_id, "qty": str(qty_used)})
        move = LotMoveRecord(
            tx_signature=out_event.raw.get("signature") or out_event.id.split("#")[0],
            ts=out_event.ts,
            src_wallet=out_event.wallet,
            dst_wallet=in_event.wallet,
            mint=token.mint,
            symbol=token.symbol,
            amount=token.amount,
            fee_aud=fee_aud,
            lots_consumed=lots_consumed,
            lots_created=lots_created,
        )
        return move, moved_lots

    def _handle_out_of_scope_transfer(
        self,
        event: NormalizedEvent,
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
        external_lot_tracking: bool,
    ) -> tuple[Optional[LotMoveRecord], List[AcquisitionLot]]:
        token = event.base_token
        if token is None:
            return None, []
        try:
            allocation = methods.allocate(
                self.ledger.lots_for(event.wallet, token.mint),
                token.amount,
                "FIFO",
                event_id=event.id,
                issue_context=_issue_context(event, token),
            )
        except methods.LotSelectionError as exc:
            warnings.append(
                WarningRecord(
                    ts=event.ts,
                    wallet=event.wallet,
                    signature=event.raw.get("signature"),
                    code="external_transfer_out_no_lots",
                    message=str(exc),
                )
            )
            return None, []
        external_wallet = self._external_wallet_id(event.counterparty)
        move_fee_allocations = allocate_fee_over_consumed_parts(
            [(lot, qty_used) for lot, qty_used in allocation],
            self._fee_to_aud(event, warnings, warned),
        )
        lots_consumed: List[dict[str, str]] = []
        lots_created: List[dict[str, str]] = []
        moved_lots: List[AcquisitionLot] = []
        for (lot, qty_used), allocated_move_fee in zip(allocation, move_fee_allocations):
            remaining_qty = lot.remaining_qty if lot.remaining_qty else lot.qty_acquired
            portion = qty_used / remaining_qty if remaining_qty else Decimal("0")
            moved_fees = lot.fees_aud * portion
            lot.fees_aud = (lot.fees_aud - moved_fees)
            if lot.fees_aud < Decimal("0"):
                lot.fees_aud = Decimal("0")
            self.ledger.update_remaining(lot, qty_used)
            new_lot = AcquisitionLot(
                lot_id=f"{lot.lot_id}:external:{event.id}",
                wallet=external_wallet,
                ts=lot.ts,
                token_mint=lot.token_mint,
                token_symbol=lot.token_symbol,
                qty_acquired=qty_used,
                unit_cost_aud=lot.unit_cost_aud,
                fees_aud=moved_fees + allocated_move_fee,
                remaining_qty=qty_used,
                source_event=event.id,
                source_type="external_move",
            )
            self.ledger.add_lot(new_lot)
            moved_lots.append(new_lot)
            lots_consumed.append({"lot_id": lot.lot_id, "qty": str(qty_used)})
            lots_created.append({"lot_id": new_lot.lot_id, "qty": str(qty_used)})
        warnings.append(
            WarningRecord(
                ts=event.ts,
                wallet=event.wallet,
                signature=event.raw.get("signature"),
                code="external_transfer_out",
                message=(
                    "Transfer out to external wallet treated as out-of-scope. Lots moved to external bucket"
                    + (" (tracking enabled)." if external_lot_tracking else ".")
                ),
            )
        )
        move = LotMoveRecord(
            tx_signature=event.raw.get("signature") or event.id.split("#")[0],
            ts=event.ts,
            src_wallet=event.wallet,
            dst_wallet=external_wallet,
            mint=token.mint,
            symbol=token.symbol,
            amount=token.amount,
            fee_aud=self._fee_to_aud(event, warnings, warned),
            lots_consumed=lots_consumed,
            lots_created=lots_created,
        )
        return move, moved_lots

    def _add_synthetic_lot(
        self,
        issue: MissingLotIssue,
        event: NormalizedEvent,
        *,
        source_type: str,
    ) -> AcquisitionLot:
        lot = AcquisitionLot(
            lot_id=f"synthetic:{event.id}:{issue.mint}",
            wallet=issue.wallet,
            ts=issue.ts,
            token_mint=issue.mint,
            token_symbol=issue.symbol,
            qty_acquired=issue.shortfall_qty,
            unit_cost_aud=Decimal("0"),
            fees_aud=Decimal("0"),
            remaining_qty=issue.shortfall_qty,
            source_event=issue.event_id,
            source_type=source_type,
        )
        self.ledger.add_lot(lot)
        return lot


def _issue_context(event: NormalizedEvent, token: TokenAmount) -> dict[str, object]:
    return {
        "wallet": event.wallet,
        "mint": token.mint,
        "symbol": token.symbol,
        "ts": event.ts,
        "signature": event.raw.get("signature"),
        "event_id": event.id,
        "event_type": event.kind,
    }


def _append_note(existing: Optional[str], note: str) -> str:
    if not existing:
        return note
    if note in existing:
        return existing
    return f"{existing}; {note}"

    def _handle_external_return(
        self,
        event: NormalizedEvent,
        lot_moves: List[LotMoveRecord],
        warnings: List[WarningRecord],
        warned: set[tuple[str, str, str]],
    ) -> tuple[bool, List[AcquisitionLot]]:
        token = event.quote_token
        if token is None:
            return False, []
        external_wallet = self._external_wallet_id(event.counterparty)
        available = self.ledger.lots_for(external_wallet, token.mint)
        if not available:
            return False, []
        try:
            allocation = methods.allocate(available, token.amount, "FIFO", event_id=event.id)
        except methods.LotSelectionError:
            return False, []
        lots_consumed: List[dict[str, str]] = []
        lots_created: List[dict[str, str]] = []
        moved_lots: List[AcquisitionLot] = []
        move_fee_allocations = allocate_fee_over_consumed_parts(
            [(lot, qty_used) for lot, qty_used in allocation],
            self._fee_to_aud(event, warnings, warned),
        )
        for (lot, qty_used), allocated_move_fee in zip(allocation, move_fee_allocations):
            remaining_qty = lot.remaining_qty if lot.remaining_qty else lot.qty_acquired
            portion = qty_used / remaining_qty if remaining_qty else Decimal("0")
            moved_fees = lot.fees_aud * portion
            lot.fees_aud = (lot.fees_aud - moved_fees)
            if lot.fees_aud < Decimal("0"):
                lot.fees_aud = Decimal("0")
            self.ledger.update_remaining(lot, qty_used)
            new_lot = AcquisitionLot(
                lot_id=f"{lot.lot_id}:return:{event.id}",
                wallet=event.wallet,
                ts=lot.ts,
                token_mint=lot.token_mint,
                token_symbol=lot.token_symbol,
                qty_acquired=qty_used,
                unit_cost_aud=lot.unit_cost_aud,
                fees_aud=moved_fees + allocated_move_fee,
                remaining_qty=qty_used,
                source_event=event.id,
                source_type="external_return",
            )
            self.ledger.add_lot(new_lot)
            moved_lots.append(new_lot)
            lots_consumed.append({"lot_id": lot.lot_id, "qty": str(qty_used)})
            lots_created.append({"lot_id": new_lot.lot_id, "qty": str(qty_used)})
        lot_moves.append(
            LotMoveRecord(
                tx_signature=event.raw.get("signature") or event.id.split("#")[0],
                ts=event.ts,
                src_wallet=external_wallet,
                dst_wallet=event.wallet,
                mint=token.mint,
                symbol=token.symbol,
                amount=token.amount,
                fee_aud=self._fee_to_aud(event, warnings, warned),
                lots_consumed=lots_consumed,
                lots_created=lots_created,
            )
        )
        warnings.append(
            WarningRecord(
                ts=event.ts,
                wallet=event.wallet,
                signature=event.raw.get("signature"),
                code="external_transfer_return",
                message="Transfer in matched against external lots.",
            )
        )
        return True, moved_lots

    def _warn_missing_price(
        self,
        event: NormalizedEvent,
        mint: str,
        warnings: List[WarningRecord],
        warned: set[tuple[str, str]],
        *,
        code: str = "missing_price",
    ) -> None:
        signature = event.raw.get("signature") or event.id
        key = (signature, mint, code)
        if key in warned:
            return
        warned.add(key)
        event.raw["unpriced"] = True
        event.tags.add("unpriced")
        warnings.append(
            WarningRecord(
                ts=event.ts,
                wallet=event.wallet,
                signature=event.raw.get("signature"),
                code=code,
                message=f"Price not available for mint={mint} at {event.ts.isoformat()}",
            )
        )

    def _convert_usd_hint(self, event: NormalizedEvent, key: str) -> Decimal:
        usd_value = Decimal(str(event.raw[key]))
        fx_rate = getattr(self.price_provider, "fx_rate", None)
        if callable(fx_rate):
            return utils.quantize_aud(usd_value * fx_rate(event.ts))
        return utils.quantize_aud(usd_value)

    def _external_wallet_id(self, counterparty: Optional[str]) -> str:
        address = counterparty or "unknown"
        return f"__external__:{address}"


def allocate_fee_over_consumed_parts(
    consumed_parts: List[tuple[AcquisitionLot, Decimal]],
    fee_aud: Decimal,
) -> List[Decimal]:
    if not consumed_parts:
        return []
    total_qty = sum((qty for _, qty in consumed_parts), Decimal("0"))
    if fee_aud == 0 or total_qty == 0:
        return [Decimal("0") for _ in consumed_parts]
    allocations: List[Decimal] = []
    running_total = Decimal("0")
    for lot, qty in consumed_parts[:-1]:
        portion = qty / total_qty
        allocated = fee_aud * portion
        allocations.append(allocated)
        running_total += allocated
    allocations.append(fee_aud - running_total)
    return allocations
