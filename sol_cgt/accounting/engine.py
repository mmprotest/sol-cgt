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
    NormalizedEvent,
    TokenAmount,
    WarningRecord,
)
from . import lots as lots_module
from . import methods


class PriceNotAvailable(RuntimeError):
    pass


class PriceProvider(Protocol):
    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Decimal:
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

    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Decimal:  # noqa: D401
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
        raise PriceNotAvailable(f"Price not available for {mint}")


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
    ) -> AccountingResult:
        acquisitions: List[AcquisitionLot] = []
        disposals: List[DisposalRecord] = []
        lot_moves: List[LotMoveRecord] = []
        warnings: List[WarningRecord] = []
        wallet_set = {wallet for wallet in wallets} if wallets else set()
        matches = list(transfer_matches or [])
        match_by_out = {match.out_event.id: match for match in matches}
        matched_in_ids = {match.in_event.id for match in matches}

        for event in sorted(events, key=lambda ev: (ev.ts, ev.id)):
            if event.id in matched_in_ids:
                continue
            if event.id in match_by_out:
                move_record, moved_lots = self._handle_self_transfer(match_by_out[event.id])
                lot_moves.append(move_record)
                acquisitions.extend(moved_lots)
                continue
            if event.kind == "transfer_out" and event.base_token is not None:
                if not event.counterparty or event.counterparty not in wallet_set:
                    self._handle_out_of_scope_transfer(event, warnings, external_lot_tracking)
                    continue
            if event.kind == "transfer_in" and event.quote_token is not None:
                if not event.counterparty or event.counterparty not in wallet_set:
                    moved = False
                    if external_lot_tracking:
                        moved, moved_lots = self._handle_external_return(event, lot_moves, warnings)
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
                                "Transfer in from external wallet treated as acquisition at spot price. "
                                "Enable external_lot_tracking to attempt matching."
                            ),
                        )
                    )

            base_token = event.base_token
            quote_token = event.quote_token
            fee_aud = self._fee_to_aud(event)
            event.raw["fee_aud"] = str(fee_aud)
            proceeds_aud: Optional[Decimal] = None
            if base_token is not None and base_token.amount > 0:
                proceeds_aud = self._resolve_proceeds(event, base_token, quote_token)
                disposals.extend(
                    self._handle_disposal(event, base_token, proceeds_aud, fee_aud)
                )
            if quote_token is not None and quote_token.amount > 0:
                acquisitions.append(
                    self._handle_acquisition(event, quote_token, proceeds_aud, fee_aud)
                )
        return AccountingResult(
            acquisitions=acquisitions,
            disposals=disposals,
            lot_moves=lot_moves,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    def _fee_to_aud(self, event: NormalizedEvent) -> Decimal:
        if event.fee_sol == 0:
            return Decimal("0")
        try:
            price = self.price_provider.price_aud("SOL", event.ts, context=event.raw)
        except PriceNotAvailable:
            return Decimal("0")
        return utils.quantize_aud(event.fee_sol * price)

    def _resolve_price(self, event: NormalizedEvent, token: TokenAmount) -> Decimal:
        context = dict(event.raw)
        context.setdefault("mint", token.mint)
        try:
            return self.price_provider.price_aud(token.mint, event.ts, context=context)
        except PriceNotAvailable as exc:
            hint_key = "price_aud"
            if hint_key in event.raw:
                value = event.raw[hint_key]
                if isinstance(value, (int, float, str)):
                    return Decimal(str(value))
            raise exc

    def _resolve_proceeds(
        self,
        event: NormalizedEvent,
        base: TokenAmount,
        quote: Optional[TokenAmount],
    ) -> Decimal:
        if "proceeds_aud" in event.raw:
            return Decimal(str(event.raw["proceeds_aud"]))
        if quote is not None:
            price = self._resolve_price(event, quote)
            return utils.quantize_aud(price * quote.amount)
        price = self._resolve_price(event, base)
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
        )
        if not allocation:
            raise methods.LotSelectionError(
                f"No lots available for disposal {event.id}"
            )
        records: List[DisposalRecord] = []
        gross_proceeds = proceeds_aud
        for lot, qty_used in allocation:
            portion = qty_used / base.amount if base.amount > 0 else Decimal("0")
            lot_cost = utils.quantize_aud(lot.unit_cost_aud * qty_used)
            fee_share = utils.quantize_aud(fee_aud * portion)
            proceeds_share = utils.quantize_aud(gross_proceeds * portion)
            gain_loss = utils.quantize_aud(proceeds_share - fee_share - lot_cost)
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
                cost_base_aud=lot_cost,
                fees_aud=fee_share,
                gain_loss_aud=gain_loss,
                long_term=long_term,
                held_days=held_days,
                method=self.method,
                signature=event.raw.get("signature"),
                notes=f"lot_id={lot.lot_id}",
            )
            records.append(record)
            self.ledger.update_remaining(lot, qty_used)
        return records

    def _handle_acquisition(
        self,
        event: NormalizedEvent,
        quote: TokenAmount,
        proceeds_hint: Optional[Decimal],
        fee_aud: Decimal,
    ) -> AcquisitionLot:
        qty = quote.amount
        if qty <= 0:
            raise ValueError("Acquisition quantity must be positive")
        if "cost_aud" in event.raw:
            total_cost = Decimal(str(event.raw["cost_aud"]))
        elif proceeds_hint is not None:
            total_cost = proceeds_hint
        else:
            price = self._resolve_price(event, quote)
            total_cost = utils.quantize_aud(price * qty)
        if event.base_token is None:
            total_cost += fee_aud
            lot_fee = fee_aud
        else:
            lot_fee = Decimal("0")
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

    def _handle_self_transfer(self, match: TransferMatch) -> tuple[LotMoveRecord, List[AcquisitionLot]]:
        out_event = match.out_event
        in_event = match.in_event
        if out_event.base_token is None or in_event.quote_token is None:
            raise ValueError("Self transfer events missing token data")
        token = out_event.base_token
        fee_aud = self._fee_to_aud(out_event)
        allocation = methods.allocate(
            self.ledger.lots_for(out_event.wallet, token.mint),
            token.amount,
            "FIFO",
            event_id=out_event.id,
        )
        lots_consumed: List[dict[str, str]] = []
        lots_created: List[dict[str, str]] = []
        moved_lots: List[AcquisitionLot] = []
        for lot, qty_used in allocation:
            self.ledger.update_remaining(lot, qty_used)
            portion = qty_used / lot.qty_acquired if lot.qty_acquired else Decimal("0")
            moved_fees = utils.quantize_aud(lot.fees_aud * portion)
            new_lot = AcquisitionLot(
                lot_id=f"{lot.lot_id}:move:{out_event.id}",
                wallet=in_event.wallet,
                ts=lot.ts,
                token_mint=lot.token_mint,
                token_symbol=lot.token_symbol,
                qty_acquired=qty_used,
                unit_cost_aud=lot.unit_cost_aud,
                fees_aud=moved_fees,
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
        external_lot_tracking: bool,
    ) -> None:
        token = event.base_token
        if token is None:
            return
        try:
            allocation = methods.allocate(
                self.ledger.lots_for(event.wallet, token.mint),
                token.amount,
                "FIFO",
                event_id=event.id,
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
            return
        external_wallet = f"external:{event.wallet}"
        for lot, qty_used in allocation:
            self.ledger.update_remaining(lot, qty_used)
            portion = qty_used / lot.qty_acquired if lot.qty_acquired else Decimal("0")
            moved_fees = utils.quantize_aud(lot.fees_aud * portion)
            new_lot = AcquisitionLot(
                lot_id=f"{lot.lot_id}:external:{event.id}",
                wallet=external_wallet,
                ts=lot.ts,
                token_mint=lot.token_mint,
                token_symbol=lot.token_symbol,
                qty_acquired=qty_used,
                unit_cost_aud=lot.unit_cost_aud,
                fees_aud=moved_fees,
                remaining_qty=qty_used,
                source_event=event.id,
                source_type="external_move",
            )
            self.ledger.add_lot(new_lot)
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

    def _handle_external_return(
        self,
        event: NormalizedEvent,
        lot_moves: List[LotMoveRecord],
        warnings: List[WarningRecord],
    ) -> tuple[bool, List[AcquisitionLot]]:
        token = event.quote_token
        if token is None:
            return False, []
        external_wallet = f"external:{event.wallet}"
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
        for lot, qty_used in allocation:
            self.ledger.update_remaining(lot, qty_used)
            portion = qty_used / lot.qty_acquired if lot.qty_acquired else Decimal("0")
            moved_fees = utils.quantize_aud(lot.fees_aud * portion)
            new_lot = AcquisitionLot(
                lot_id=f"{lot.lot_id}:return:{event.id}",
                wallet=event.wallet,
                ts=lot.ts,
                token_mint=lot.token_mint,
                token_symbol=lot.token_symbol,
                qty_acquired=qty_used,
                unit_cost_aud=lot.unit_cost_aud,
                fees_aud=moved_fees,
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
                fee_aud=self._fee_to_aud(event),
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
