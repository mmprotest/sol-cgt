"""Typer CLI for the sol_cgt application."""

import asyncio
import inspect
import logging
import os
import shutil
import sys
from datetime import timezone
from decimal import Decimal
from importlib import metadata
from pathlib import Path
from typing import List, Optional

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
from .reporting import formats, summaries, xlsx
from .types import NormalizedEvent
from . import utils
from .utils import australian_financial_year_bounds, parse_local_date
from .providers import birdeye as birdeye_provider
from .providers import jupiter as jupiter_provider

_orig_option_init = typer.core.TyperOption.__init__
logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    level_name = os.getenv("SOLCGT_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    root_logger.setLevel(level)


def _patched_option_init(self: typer.core.TyperOption, **kwargs) -> None:
    option_type = kwargs.get("type")
    if kwargs.get("is_flag") is None and option_type is not None and option_type is not bool:
        if not isinstance(option_type, click.types.BoolParamType):
            kwargs["is_flag"] = False
    _orig_option_init(self, **kwargs)


typer.core.TyperOption.__init__ = _patched_option_init  # type: ignore[assignment]

app = typer.Typer(help="Solana capital gains tooling")
debug_app = typer.Typer(help="Debugging helpers")
app.add_typer(debug_app, name="debug")

if "ctx" in inspect.signature(click.Parameter.make_metavar).parameters:
    _orig_make_metavar = click.Parameter.make_metavar

    def _patched_make_metavar(self: click.Parameter, ctx: click.Context | None = None) -> str:
        return _orig_make_metavar(self, ctx)

    click.Parameter.make_metavar = _patched_make_metavar  # type: ignore[assignment]


async def _normalize_wallet(wallet: str, raw_items: list[dict]) -> list[NormalizedEvent]:
    return await normalize.normalize_wallet_events(wallet, raw_items)


def _collect_wallets(wallet_values: Optional[List[str]]) -> List[str]:
    wallets: List[str] = []
    for entry in wallet_values or []:
        wallets.extend([w.strip() for w in entry.split(",") if w.strip()])
    return wallets


def _resolve_fy_period(fy: Optional[str], fy_start: Optional[str], fy_end: Optional[str]) -> tuple[Optional[str], Optional[utils.Period]]:
    if fy and (fy_start or fy_end):
        raise typer.BadParameter("Provide either --fy or --fy-start/--fy-end, not both")
    if fy_start and fy_end:
        start = parse_local_date(fy_start)
        end = parse_local_date(fy_end).replace(hour=23, minute=59, second=59)
        label = f"{fy_start}_to_{fy_end}"
        return label, utils.Period(start=start.astimezone(timezone.utc), end=end.astimezone(timezone.utc))
    if fy:
        return fy, australian_financial_year_bounds(fy)
    return None, None


def _summary_value(rows: list[dict[str, object]], key: str, default: object = 0) -> object:
    if rows:
        return rows[0].get(key, default)
    return default


def _apply_api_keys_to_env(settings) -> None:
    if settings.api_keys.birdeye:
        os.environ.setdefault("BIRDEYE_API_KEY", settings.api_keys.birdeye)
    if settings.api_keys.jupiter:
        os.environ.setdefault("JUP_API_KEY", settings.api_keys.jupiter)


def _dependency_versions(packages: list[str]) -> dict[str, str]:
    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = "not-installed"
    return versions


@app.command()
def fetch(
    wallet: List[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    before: Optional[str] = typer.Option(None, help="Pagination cursor"),
    after: Optional[str] = typer.Option(None, help="Pagination start signature"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Helius page size (1-100)"),
    max_pages: Optional[int] = typer.Option(None, "--max-pages", help="Maximum pages to fetch"),
    fy: Optional[str] = typer.Option(None, "--fy", help="Australian financial year (e.g. 2024-2025)"),
    fy_start: Optional[str] = typer.Option(None, "--fy-start", help="Financial year start (YYYY-MM-DD)"),
    fy_end: Optional[str] = typer.Option(None, "--fy-end", help="Financial year end (YYYY-MM-DD)"),
    append: bool = typer.Option(False, "--append", help="Append to cache instead of overwriting"),
) -> None:
    """Fetch raw transactions for the supplied wallets."""

    _configure_logging()
    parsed_wallets = _collect_wallets(wallet)
    overrides = {"wallets": parsed_wallets} if parsed_wallets else {}
    settings = load_settings(config, overrides)
    _apply_api_keys_to_env(settings)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    api_key = settings.api_keys.helius
    base_url = settings.helius_base_url
    resolved_limit = limit if limit is not None else settings.helius_tx_limit
    resolved_max_pages = max_pages if max_pages is not None else settings.helius_max_pages
    _, fy_period = _resolve_fy_period(fy, fy_start, fy_end)
    gte_time = int(fy_period.start.timestamp()) if fy_period else None
    lte_time = int(fy_period.end.timestamp()) if fy_period else None
    append_flag = append if isinstance(append, bool) else str(append).lower() in {"1", "true", "yes"}
    asyncio.run(
        fetch_mod.fetch_many(
            wallets,
            before_signature=before,
            after_signature=after,
            limit=resolved_limit,
            api_key=api_key,
            base_url=base_url,
            gte_time=gte_time,
            lte_time=lte_time,
            max_pages=resolved_max_pages,
            append=append_flag,
        )
    )
    typer.echo(f"Fetched transactions for {len(wallets)} wallet(s)")


@app.command()
def compute(
    wallet: List[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    outdir: Optional[Path] = typer.Option(None, "--outdir", help="Output directory"),
    method: Optional[str] = typer.Option(None, "--method", help="Lot selection method"),
    fy: Optional[str] = typer.Option(None, "--fy", help="Australian financial year (e.g. 2024-2025)"),
    fy_start: Optional[str] = typer.Option(None, "--fy-start", help="Financial year start (YYYY-MM-DD)"),
    fy_end: Optional[str] = typer.Option(None, "--fy-end", help="Financial year end (YYYY-MM-DD)"),
    fmt: str = typer.Option("csv", "--format", help="Report format", show_default=True),
    xlsx_path: Optional[Path] = typer.Option(None, "--xlsx", help="Output XLSX path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Normalize only, no accounting"),
    fetch: bool = typer.Option(
        True,
        "--fetch/--no-fetch",
        help="Fetch txs from Helius if cache is empty or missing",
    ),
) -> None:
    _configure_logging()
    parsed_wallets = _collect_wallets(wallet)
    overrides = {"wallets": parsed_wallets} if parsed_wallets else {}
    if method:
        overrides["method"] = method
    settings = load_settings(config, overrides)
    _apply_api_keys_to_env(settings)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    fy_label, fy_period = _resolve_fy_period(fy, fy_start, fy_end)
    events: List[NormalizedEvent] = []
    kind_counts: dict[str, int] = {}
    gte_time = int(fy_period.start.timestamp()) if fy_period else None
    lte_time = int(fy_period.end.timestamp()) if fy_period else None
    for addr in wallets:
        if not fetch_mod.cache_has_data(addr):
            if fetch:
                asyncio.run(
                    fetch_mod.fetch_wallet(
                        addr,
                        api_key=settings.api_keys.helius,
                        base_url=settings.helius_base_url,
                        limit=settings.helius_tx_limit,
                        max_pages=settings.helius_max_pages,
                        gte_time=gte_time,
                        lte_time=lte_time,
                    )
                )
            else:
                typer.echo(
                    f"Skipping {addr}: cache empty or missing and --no-fetch specified"
                )
                continue
        raw_items = fetch_mod.load_cached(addr)
        wallet_events = asyncio.run(_normalize_wallet(addr, raw_items))
        logger.info(
            "Wallet %s raw_txs_loaded=%s normalized_events_count=%s",
            addr,
            len(raw_items),
            len(wallet_events),
        )
        events.extend(wallet_events)
        for ev in wallet_events:
            kind_counts[ev.kind] = kind_counts.get(ev.kind, 0) + 1
    if kind_counts:
        breakdown = ", ".join(f"{kind}={count}" for kind, count in sorted(kind_counts.items()))
        logger.info("Normalized event breakdown: %s", breakdown)
    else:
        logger.info("Normalized event breakdown: none")
    matches = transfers.detect_self_transfers(events, wallets)
    if settings.api_keys.birdeye:
        price_provider = AudPriceProvider(
            api_key=settings.api_keys.birdeye,
            jupiter_api_key=settings.api_keys.jupiter,
            fx_source=settings.fx_source,
        )
    else:
        price_provider = AudPriceProvider(
            jupiter_api_key=settings.api_keys.jupiter,
            fx_source=settings.fx_source,
        )
    if dry_run:
        typer.echo(f"Loaded {len(events)} normalized events across {len(wallets)} wallet(s)")
        return
    engine = AccountingEngine(method=settings.method, price_provider=price_provider)
    result = engine.process(
        events,
        wallets=wallets,
        transfer_matches=matches,
        external_lot_tracking=settings.external_lot_tracking,
    )
    disposals = result.disposals
    acquisitions = result.acquisitions
    lot_moves = result.lot_moves
    warnings = result.warnings
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
        lot_moves = [m for m in lot_moves if fy_period.start <= m.ts <= fy_period.end]
        warnings = [w for w in warnings if fy_period.start <= w.ts <= fy_period.end]
    summary_by_token = summaries.summarize_by_token(disposals)
    summary_overall = summaries.summarize_overall(disposals)
    wallet_summary = summaries.summarize_by_wallet(disposals)
    if not disposals:
        breakdown = ", ".join(f"{kind}={count}" for kind, count in sorted(kind_counts.items())) or "none"
        logger.warning(
            "No disposals detected. Event breakdown: %s. Check for swap/outflow events or empty cache.",
            breakdown,
        )
    output_dir = outdir or Path("./reports") / ("combined" if len(wallets) > 1 else wallets[0])
    if fy_label:
        output_dir = output_dir / fy_label
    formats.export_reports(output_dir, acquisitions, disposals, summary_by_token, summary_overall, fmt=fmt)
    if xlsx_path:
        for event in events:
            if event.fee_sol and "fee_aud" not in event.raw:
                fee_price = price_provider.price_aud("SOL", event.ts, context=event.raw)
                event.raw["fee_aud"] = str(utils.quantize_aud(event.fee_sol * fee_price))
        fees_total = sum((d.fees_aud for d in disposals), Decimal("0")) + sum(
            (m.fee_aud for m in lot_moves), Decimal("0")
        )
        short_term_gain = sum((d.gain_loss_aud for d in disposals if not d.long_term), Decimal("0"))
        long_term_gain = sum((d.gain_loss_aud for d in disposals if d.long_term), Decimal("0"))
        xlsx.export_xlsx(
            xlsx_path,
            overview={
                "Financial year": fy_label or "all",
                "Wallets": ", ".join(wallets),
                "Method": settings.method,
                "Total proceeds (AUD)": str(_summary_value(summary_overall, "proceeds_aud", 0)),
                "Total cost base (AUD)": str(_summary_value(summary_overall, "cost_base_aud", 0)),
                "Net gain/loss (AUD)": str(_summary_value(summary_overall, "gain_loss_aud", 0)),
                "Short-term gain/loss (AUD)": str(short_term_gain),
                "Discount-eligible gain/loss (AUD)": str(long_term_gain),
                "Discount eligible gain (AUD)": str(_summary_value(summary_overall, "discount_eligible_gain_aud", 0)),
                "Fees total (AUD)": str(fees_total),
                "Warnings": str(len(warnings)),
            },
            events=[ev for ev in events if not fy_period or fy_period.start <= ev.ts <= fy_period.end],
            lots=acquisitions,
            disposals=disposals,
            summary_by_token=summary_by_token,
            wallet_summary=wallet_summary,
            lot_moves=lot_moves,
            warnings=warnings,
            price_provider=price_provider,
        )
    console_report.render_summary(disposals, acquisitions, warnings)


@app.command()
def report(
    wallet: List[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    outdir: Optional[Path] = typer.Option(None, "--outdir", help="Output directory"),
    method: Optional[str] = typer.Option(None, "--method", help="Lot selection method"),
    fy: Optional[str] = typer.Option(None, "--fy", help="Australian financial year (e.g. 2024-2025)"),
    fmt: str = typer.Option("csv", "--format", help="Report format", show_default=True),
) -> None:
    """Backward-compatible alias for compute."""
    _configure_logging()
    compute(wallet=wallet, config=config, outdir=outdir, method=method, fy=fy, fmt=fmt)


@app.command()
def audit(
    wallet: List[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
) -> None:
    """Display information about cached data and unresolved lots."""

    _configure_logging()
    parsed_wallets = _collect_wallets(wallet)
    overrides = {"wallets": parsed_wallets} if parsed_wallets else {}
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


@debug_app.command("env")
def debug_env() -> None:
    """Print environment details for troubleshooting."""
    executable = shutil.which("solcgt") or sys.argv[0]
    typer.echo(f"solcgt executable: {executable}")
    import sol_cgt  # imported here to avoid startup overhead

    typer.echo(f"sol_cgt package: {sol_cgt.__file__}")
    versions = _dependency_versions(
        [
            "sol-cgt",
            "httpx",
            "typer",
            "pydantic",
            "pydantic-settings",
        ]
    )
    for name, version in versions.items():
        typer.echo(f"{name} version: {version}")
    typer.echo(f"Birdeye metadata URL: {birdeye_provider.METADATA_URL}")
    typer.echo(f"Jupiter token v1 URL: {jupiter_provider.JUPITER_TOKENS_V1_URL}")
    typer.echo(f"Jupiter token v2 URL: {jupiter_provider.JUPITER_TOKENS_V2_URL}")
    typer.echo(f"Jupiter price base URL: {jupiter_provider._price_base_url(os.getenv('JUP_API_KEY'))}")
    typer.echo(f"Jupiter RPC URL: {jupiter_provider._rpc_url()}")
