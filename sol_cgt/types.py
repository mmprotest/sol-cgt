"""Core pydantic data models used across the project."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ChainTxRef(BaseModel):
    """Reference to an on-chain Solana transaction."""

    model_config = ConfigDict(frozen=True)

    signature: str
    slot: int
    block_time: datetime


class TokenAmount(BaseModel):
    """Represents an SPL token amount with decimal precision."""

    model_config = ConfigDict(frozen=True)

    mint: str
    symbol: Optional[str] = None
    decimals: int = Field(ge=0)
    amount_raw: int
    amount: Optional[Decimal] = None

    @model_validator(mode="before")
    @classmethod
    def _fill_amount(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if values.get("amount") is not None:
            return values
        amount_raw = values.get("amount_raw")
        decimals = values.get("decimals")
        if amount_raw is None or decimals is None:
            return values
        quantizer = Decimal(10) ** -int(decimals)
        values["amount"] = (Decimal(amount_raw) * quantizer).quantize(quantizer)
        return values

    @field_validator("amount", mode="before")
    @classmethod
    def _derive_amount(cls, value: Any, values: dict[str, Any]) -> Decimal:
        if isinstance(value, Decimal):
            return value
        amount_raw = values.get("amount_raw")
        decimals = values.get("decimals")
        if amount_raw is None or decimals is None:
            raise ValueError("amount_raw and decimals must be provided")
        quantizer = Decimal(10) ** -int(decimals)
        return (Decimal(amount_raw) * quantizer).quantize(quantizer)


NormalizedEventKind = Literal[
    "swap",
    "sell",
    "buy",
    "transfer_in",
    "transfer_out",
    "airdrop",
    "mint",
    "burn",
    "wrap",
    "unwrap",
    "liquidity_add",
    "liquidity_remove",
    "unknown",
]


class NormalizedEvent(BaseModel):
    """Normalized representation of a wallet level event."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str
    ts: datetime
    kind: NormalizedEventKind
    base_token: Optional[TokenAmount] = None
    quote_token: Optional[TokenAmount] = None
    fee_sol: Decimal = Decimal("0")
    wallet: str
    counterparty: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict)
    tags: Set[str] = Field(default_factory=set)

    @field_validator("tags", mode="before")
    @classmethod
    def _ensure_set(cls, value: Any) -> Set[str]:
        if value is None:
            return set()
        if isinstance(value, set):
            return value
        if isinstance(value, (list, tuple)):
            return set(value)
        if isinstance(value, str):
            return {value}
        raise TypeError("tags must be iterable of strings")


class AcquisitionLot(BaseModel):
    """Represents an acquisition lot for a specific wallet and token."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    lot_id: str
    wallet: str
    ts: datetime
    token_mint: str
    token_symbol: Optional[str] = None
    qty_acquired: Decimal
    unit_cost_aud: Decimal
    fees_aud: Decimal
    remaining_qty: Decimal
    source_event: Optional[str] = None
    source_type: Optional[str] = None


class DisposalRecord(BaseModel):
    """Represents a disposal matched against one or more acquisition lots."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    event_id: str
    wallet: str
    ts: datetime
    token_mint: str
    token_symbol: Optional[str] = None
    qty_disposed: Decimal
    proceeds_aud: Decimal
    cost_base_aud: Decimal
    fees_aud: Decimal
    gain_loss_aud: Decimal
    long_term: bool
    held_days: int
    method: str
    signature: Optional[str] = None
    notes: Optional[str] = None


class LotMoveRecord(BaseModel):
    """Represents a non-taxable lot movement between wallets."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    tx_signature: str
    ts: datetime
    src_wallet: str
    dst_wallet: str
    mint: str
    symbol: Optional[str] = None
    amount: Decimal
    fee_aud: Decimal
    lots_consumed: list[dict[str, str]] = Field(default_factory=list)
    lots_created: list[dict[str, str]] = Field(default_factory=list)


class WarningRecord(BaseModel):
    """Represents a warning generated during processing."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    ts: datetime
    wallet: Optional[str] = None
    signature: Optional[str] = None
    code: str
    message: str


class MissingLotIssue(BaseModel):
    """Represents missing acquisition lots needed to satisfy a disposal."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    wallet: str
    mint: str
    symbol: Optional[str] = None
    ts: datetime
    signature: Optional[str] = None
    event_id: str
    event_type: str
    required_qty: Decimal
    available_qty: Decimal
    shortfall_qty: Decimal
    message: str
