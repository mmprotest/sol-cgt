"""Console rendering helpers using rich."""
from __future__ import annotations

from decimal import Decimal
from typing import Iterable, Sequence

from rich.console import Console
from rich.table import Table

from ..types import AcquisitionLot, DisposalRecord


def render_summary(disposals: Sequence[DisposalRecord], acquisitions: Sequence[AcquisitionLot]) -> None:
    console = Console()
    if disposals:
        table = Table(title="Disposals Summary", show_lines=False)
        table.add_column("Event")
        table.add_column("Token")
        table.add_column("Qty")
        table.add_column("Proceeds (AUD)")
        table.add_column("Gain/Loss (AUD)")
        for record in disposals[:10]:
            table.add_row(
                record.event_id,
                record.token_symbol or record.token_mint,
                f"{record.qty_disposed}",
                f"{record.proceeds_aud:.2f}",
                f"{record.gain_loss_aud:.2f}",
            )
        console.print(table)
    else:
        console.print("[yellow]No disposals processed.[/yellow]")
    unresolved = [lot for lot in acquisitions if lot.remaining_qty > 0 and lot.unit_cost_aud == 0]
    if unresolved:
        warn_table = Table(title="Lots with zero cost", show_lines=False)
        warn_table.add_column("Lot")
        warn_table.add_column("Token")
        warn_table.add_column("Qty")
        for lot in unresolved:
            warn_table.add_row(lot.lot_id, lot.token_symbol or lot.token_mint, f"{lot.remaining_qty}")
        console.print(warn_table)
