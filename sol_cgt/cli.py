"""Typer CLI for the sol_cgt application."""

import asyncio
import inspect
from pathlib import Path
from typing import Optional

import click
import typer
import typer.core

from .accounting.engine import AccountingEngine
from .config import load_settings
from .ingestion import fetch as fetch_mod
from .ingestion import normalize
from .pricing import AudPriceProvider
from .reconciliation import transfers
from .reporting import console as console_report
from .reporting import formats, summaries
from .types import NormalizedEvent
from .utils import australian_financial_year_bounds

_orig_option_init = typer.core.TyperOption.__init__


def _patched_option_init(self: typer.core.TyperOption, **kwargs) -> None:
    option_type = kwargs.get("type")
    if kwargs.get("is_flag") is None and option_type is not None and option_type is not bool:
        if not isinstance(option_type, click.types.BoolParamType):
            kwargs["is_flag"] = False
    _orig_option_init(self, **kwargs)


typer.core.TyperOption.__init__ = _patched_option_init  # type: ignore[assignment]

app = typer.Typer(help="Solana capital gains tooling")

if "ctx" in inspect.signature(click.Parameter.make_metavar).parameters:
    _orig_make_metavar = click.Parameter.make_metavar

    def _patched_make_metavar(self: click.Parameter, ctx: click.Context | None = None) -> str:
        return _orig_make_metavar(self, ctx)

    click.Parameter.make_metavar = _patched_make_metavar  # type: ignore[assignment]


async def _normalize_wallet(wallet: str, raw_items: list[dict]) -> list[NormalizedEvent]:
    return await normalize.normalize_wallet_events(wallet, raw_items)


@app.command()
def fetch(
    wallet: Optional[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    before: Optional[str] = typer.Option(None, help="Pagination cursor"),
    append: bool = typer.Option(False, "--append", help="Append to cache instead of overwriting"),
) -> None:
    """Fetch raw transactions for the supplied wallets."""

    overrides = {"wallets": [w.strip() for w in wallet.split(",") if w.strip()]} if wallet else {}
    settings = load_settings(config, overrides)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    api_key = settings.api_keys.helius
    append_flag = append if isinstance(append, bool) else str(append).lower() in {"1", "true", "yes"}
    asyncio.run(fetch_mod.fetch_many(wallets, before=before, api_key=api_key, append=append_flag))
    typer.echo(f"Fetched transactions for {len(wallets)} wallet(s)")


@app.command()
def report(
    wallet: Optional[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    outdir: Optional[Path] = typer.Option(None, "--outdir", help="Output directory"),
    method: Optional[str] = typer.Option(None, "--method", help="Lot selection method"),
    fy: Optional[str] = typer.Option(None, "--fy", help="Australian financial year (e.g. 2024-2025)"),
    fmt: str = typer.Option("csv", "--format", help="Report format", show_default=True),
) -> None:
    overrides = {"wallets": [w.strip() for w in wallet.split(",") if w.strip()]} if wallet else {}
    if method:
        overrides["method"] = method
    settings = load_settings(config, overrides)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    events: List[NormalizedEvent] = []
    fy_period = australian_financial_year_bounds(fy) if fy else None
    for addr in wallets:
        raw_items = fetch_mod.load_cached(addr)
        wallet_events = asyncio.run(_normalize_wallet(addr, raw_items))
        if fy_period:
            wallet_events = [ev for ev in wallet_events if ev.ts <= fy_period.end]
        events.extend(wallet_events)
    transfers.detect_self_transfers(events, wallets)
    if settings.api_keys.birdeye:
        price_provider = AudPriceProvider(api_key=settings.api_keys.birdeye)
    else:
        price_provider = AudPriceProvider()
    engine = AccountingEngine(method=settings.method, price_provider=price_provider)
    acquisitions, disposals = engine.process(events)
    if fy_period:
        disposals = [d for d in disposals if fy_period.start <= d.ts <= fy_period.end]
        used_lot_ids = {
            d.notes.split("lot_id=")[-1]
            for d in disposals
            if d.notes and "lot_id=" in d.notes
        }
        acquisitions = [
            lot for lot in acquisitions
            if fy_period.start <= lot.ts <= fy_period.end or lot.lot_id in used_lot_ids
        ]
    summary_by_token = summaries.summarize_by_token(disposals)
    summary_overall = summaries.summarize_overall(disposals)
    output_dir = outdir or Path("./reports") / ("combined" if len(wallets) > 1 else wallets[0])
    formats.export_reports(output_dir, acquisitions, disposals, summary_by_token, summary_overall, fmt=fmt)
    console_report.render_summary(disposals, acquisitions)


@app.command()
def audit(
    wallet: Optional[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
) -> None:
    """Display information about cached data and unresolved lots."""

    overrides = {"wallets": [w.strip() for w in wallet.split(",") if w.strip()]} if wallet else {}
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
