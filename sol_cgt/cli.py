"""Typer CLI for the sol_cgt application."""
from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

import typer

from .accounting.engine import AccountingEngine, SimplePriceProvider
from .config import load_settings
from .ingestion import fetch as fetch_mod
from .ingestion import normalize
from .reconciliation import transfers
from .reporting import console as console_report
from .reporting import formats, summaries
from .types import NormalizedEvent

app = typer.Typer(help="Solana capital gains tooling")


async def _normalize_wallet(wallet: str, raw_items: list[dict]) -> List[NormalizedEvent]:
    return await normalize.normalize_wallet_events(wallet, raw_items)


@app.command()
def fetch(
    wallet: List[str] = typer.Option([], "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    before: Optional[str] = typer.Option(None, help="Pagination cursor"),
) -> None:
    """Fetch raw transactions for the supplied wallets."""

    overrides = {"wallets": wallet} if wallet else {}
    settings = load_settings(config, overrides)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    asyncio.run(fetch_mod.fetch_many(wallets, before=before))
    typer.echo(f"Fetched transactions for {len(wallets)} wallet(s)")


@app.command()
def report(
    wallet: List[str] = typer.Option([], "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    outdir: Optional[Path] = typer.Option(None, "--outdir", help="Output directory"),
    method: Optional[str] = typer.Option(None, "--method", help="Lot selection method"),
    fmt: str = typer.Option("csv", "--format", help="Report format", show_default=True),
) -> None:
    overrides = {"wallets": wallet} if wallet else {}
    if method:
        overrides["method"] = method
    settings = load_settings(config, overrides)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    events: List[NormalizedEvent] = []
    for addr in wallets:
        raw_items = fetch_mod.load_cached(addr)
        events.extend(asyncio.run(_normalize_wallet(addr, raw_items)))
    transfers.detect_self_transfers(events, wallets)
    price_provider = SimplePriceProvider({"SOL": Decimal("0")})
    engine = AccountingEngine(method=settings.method, price_provider=price_provider)
    acquisitions, disposals = engine.process(events)
    summary_by_token = summaries.summarize_by_token(disposals)
    summary_overall = summaries.summarize_overall(disposals)
    output_dir = outdir or Path("./reports") / ("combined" if len(wallets) > 1 else wallets[0])
    formats.export_reports(output_dir, acquisitions, disposals, summary_by_token, summary_overall, fmt=fmt)
    console_report.render_summary(disposals, acquisitions)


@app.command()
def audit(
    wallet: List[str] = typer.Option([], "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
) -> None:
    """Display information about cached data and unresolved lots."""

    overrides = {"wallets": wallet} if wallet else {}
    settings = load_settings(config, overrides)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    events: List[NormalizedEvent] = []
    for addr in wallets:
        raw_items = fetch_mod.load_cached(addr)
        events.extend(asyncio.run(_normalize_wallet(addr, raw_items)))
    zero_cost = [ev for ev in events if ev.kind == "transfer_in" and (ev.quote_token and ev.quote_token.amount > 0) and ev.raw.get("cost_aud") is None]
    typer.echo(f"Loaded {len(events)} normalized events")
    typer.echo(f"Found {len(zero_cost)} transfer_in events without cost metadata")
