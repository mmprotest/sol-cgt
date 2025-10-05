"""Core accounting engine to transform events into disposals and acquisitions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Protocol

from .. import utils
from ..types import AcquisitionLot, NormalizedEvent, TokenAmount, DisposalRecord
from . import lots as lots_module
from . import methods


class PriceNotAvailable(RuntimeError):
    pass


class PriceProvider(Protocol):
    def price_aud(self, mint: str, ts: datetime, *, context: Optional[dict] = None) -> Decimal:
        ...


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

    def process(self, events: Iterable[NormalizedEvent]) -> tuple[List[AcquisitionLot], List[DisposalRecord]]:
        acquisitions: List[AcquisitionLot] = []
        disposals: List[DisposalRecord] = []
        for event in sorted(events, key=lambda ev: (ev.ts, ev.id)):
            if event.kind == "transfer_out" and "self_transfer" in event.tags:
                continue
            base_token = event.base_token
            quote_token = event.quote_token
            fee_aud = self._fee_to_aud(event)
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
        return acquisitions, disposals

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
            long_term = utils.holding_period_days(lot.ts, event.ts) >= self.long_term_days
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
                method=self.method,
                notes=None,
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
        )
        self.ledger.add_lot(lot)
        return lot
