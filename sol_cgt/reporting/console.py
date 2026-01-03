"""Console rendering helpers."""
from __future__ import annotations

from typing import Sequence

try:
    from rich.console import Console
    from rich.table import Table
except ImportError:  # pragma: no cover - depends on optional extra
    Console = None
    Table = None

from ..types import AcquisitionLot, DisposalRecord, WarningRecord


def render_summary(
    disposals: Sequence[DisposalRecord],
    acquisitions: Sequence[AcquisitionLot],
    warnings: Sequence[WarningRecord] | None = None,
) -> None:
    if Console and Table:
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
        if warnings:
            console.print(f"[yellow]Warnings: {len(warnings)}[/yellow]")
        return

    if disposals:
        print("Disposals Summary")
        for record in disposals[:10]:
            print(
                f"- {record.event_id} | {record.token_symbol or record.token_mint} | "
                f"{record.qty_disposed} | {record.proceeds_aud:.2f} | {record.gain_loss_aud:.2f}"
            )
    else:
        print("No disposals processed.")
    unresolved = [lot for lot in acquisitions if lot.remaining_qty > 0 and lot.unit_cost_aud == 0]
    if unresolved:
        print("Lots with zero cost")
        for lot in unresolved:
            print(f"- {lot.lot_id} | {lot.token_symbol or lot.token_mint} | {lot.remaining_qty}")
    if warnings:
        print(f"Warnings: {len(warnings)}")
