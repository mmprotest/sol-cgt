"""Typer CLI for the sol_cgt application."""

import asyncio
import json
import inspect
import logging
import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from importlib import metadata
from pathlib import Path
from typing import List, Optional

import click
import typer
import typer.core

from .accounting.engine import AccountingEngine, AccountingResult
from .accounting.eligibility import apply_accounting_policy
from .accounting.token_to_token import canonicalize_token_to_token
from .accounting import methods as accounting_methods
from .config import load_settings
from .ingestion import fetch as fetch_mod
from .ingestion import normalize
from .pricing import AudPriceProvider, TimestampPriceProvider
from .pricing import valuation as valuation_module
from .reconciliation import transfers
from .reporting import console as console_report
from .reporting import formats, summaries, xlsx
from .types import MissingLotIssue, NormalizedEvent
from .types import WarningRecord
from . import utils
from .utils import australian_financial_year_bounds, parse_local_date
from .providers import jupiter as jupiter_provider
from .providers import fx_price_table, sol_price_table

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


async def _normalize_wallet(
    wallet: str,
    raw_items: list[dict],
    *,
    prefetch_mints: bool = True,
    rpc_url: Optional[str] = None,
) -> list[NormalizedEvent]:
    return await normalize.normalize_wallet_events(
        wallet,
        raw_items,
        prefetch_mints=prefetch_mints,
        rpc_url=rpc_url,
    )


def _load_and_normalize(
    wallets: list[str],
    *,
    settings,
    gte_time: Optional[int],
    lte_time: Optional[int],
    fetch: bool,
    prefetch_mints: bool,
    rpc_url: Optional[str],
    force_fetch: bool = False,
) -> tuple[list[NormalizedEvent], dict[str, int]]:
    events: list[NormalizedEvent] = []
    kind_counts: dict[str, int] = {}
    for addr in wallets:
        if force_fetch and fetch and (gte_time is not None or lte_time is not None):
            asyncio.run(
                fetch_mod.fetch_wallet(
                    addr,
                    api_key=settings.api_keys.helius,
                    base_url=settings.helius_enhanced_base_url,
                    limit=settings.helius_tx_limit,
                    max_pages=settings.helius_max_pages,
                    gte_time=gte_time,
                    lte_time=lte_time,
                    append=True,
                )
            )
        if not fetch_mod.cache_has_data(addr):
            if fetch:
                asyncio.run(
                    fetch_mod.fetch_wallet(
                        addr,
                        api_key=settings.api_keys.helius,
                        base_url=_resolve_rpc_url(settings),
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
        wallet_events = asyncio.run(
            _normalize_wallet(
                addr,
                raw_items,
                prefetch_mints=prefetch_mints,
                rpc_url=rpc_url,
            )
        )
        logger.info(
            "Wallet %s raw_txs_loaded=%s normalized_events_count=%s",
            addr,
            len(raw_items),
            len(wallet_events),
        )
        events.extend(wallet_events)
        for ev in wallet_events:
            kind_counts[ev.kind] = kind_counts.get(ev.kind, 0) + 1
    return events, kind_counts


def _apply_kind_breakdown(kind_counts: dict[str, int]) -> None:
    if kind_counts:
        breakdown = ", ".join(f"{kind}={count}" for kind, count in sorted(kind_counts.items()))
        logger.info("Normalized event breakdown: %s", breakdown)
    else:
        logger.info("Normalized event breakdown: none")


def _run_accounting(
    *,
    events: list[NormalizedEvent],
    wallets: list[str],
    settings,
    price_provider: AudPriceProvider,
    strict_lots: bool,
    missing_lot_issues: list[MissingLotIssue],
) -> tuple[AccountingResult, bool]:
    engine = AccountingEngine(method=settings.method, price_provider=price_provider)
    try:
        result = engine.process(
            events,
            wallets=wallets,
            transfer_matches=transfers.detect_self_transfers(events, wallets),
            external_lot_tracking=settings.external_lot_tracking,
            strict_lots=strict_lots,
            missing_lot_issues=missing_lot_issues,
        )
        return result, False
    except accounting_methods.LotSelectionError as exc:
        if exc.issue and exc.issue not in missing_lot_issues:
            missing_lot_issues.append(exc.issue)
        if strict_lots:
            raise
        partial = exc.partial_result if isinstance(exc.partial_result, AccountingResult) else AccountingResult(acquisitions=[], disposals=[], lot_moves=[], warnings=[])
        return partial, True


def _ensure_cache_coverage(wallets: list[str], *, gte_time: Optional[int], lte_time: Optional[int], fetch: bool, settings, refresh_raw_cache: bool = False) -> tuple[bool, dict[str, fetch_mod.CacheCoverage]]:
    complete = True
    coverage_by_wallet: dict[str, fetch_mod.CacheCoverage] = {}
    for addr in wallets:
        attempts = 0
        while attempts < 3:
            attempts += 1
            coverage = fetch_mod.inspect_raw_cache_coverage(addr, gte_time, lte_time)
            coverage_by_wallet[addr] = coverage
            logger.info("Raw cache coverage wallet=%s cache_path=%s count=%s cache_min=%s cache_max=%s requested_start=%s requested_end=%s complete=%s", addr, coverage.cache_path, coverage.raw_tx_count, coverage.cache_min_timestamp, coverage.cache_max_timestamp, coverage.requested_start, coverage.requested_end, coverage.coverage_complete)
            if coverage.coverage_complete and not refresh_raw_cache:
                break
            if not fetch:
                break
            ranges = coverage.missing_ranges or []
            if refresh_raw_cache and gte_time is not None and lte_time is not None:
                ranges = [{"start": gte_time, "end": lte_time, "reason": "refresh"}]
            if not ranges:
                break
            had_additions = False
            for missing in ranges:
                start = int(missing["start"])
                end = int(missing["end"])
                reason = str(missing.get("reason", "unknown"))
                logger.info("Raw cache missing range wallet=%s start=%s end=%s reason=%s", addr, start, end, reason)
                before_count = len(fetch_mod.load_cached(addr))
                rows = asyncio.run(
                    fetch_mod.fetch_wallet(
                        addr,
                        api_key=settings.api_keys.helius,
                        base_url=_resolve_rpc_url(settings),
                        limit=settings.helius_tx_limit,
                        max_pages=settings.helius_max_pages,
                        gte_time=start,
                        lte_time=end,
                        append=True,
                    )
                )
                after_count = len(fetch_mod.load_cached(addr))
                added = max(0, after_count - before_count)
                had_additions = had_additions or added > 0
                logger.info("Raw cache fetch complete wallet=%s added=%s total=%s", addr, added, after_count)
                if refresh_raw_cache and reason == "refresh":
                    checked = fetch_mod.get_provider_checked_ranges(addr)
                    if checked:
                        latest = checked[-1]
                        logger.info(
                            "Raw provider range verified wallet=%s provider=%s start=%s end=%s exhausted=%s rows=%s unique=%s earliest_returned=%s latest_returned=%s",
                            addr,
                            latest.get("provider"),
                            latest.get("checked_start"),
                            latest.get("checked_end"),
                            latest.get("exhausted"),
                            latest.get("rows_returned"),
                            latest.get("unique_signatures"),
                            latest.get("earliest_returned_timestamp"),
                            latest.get("latest_returned_timestamp"),
                        )
            if not had_additions:
                break
            if refresh_raw_cache:
                break
        coverage = fetch_mod.inspect_raw_cache_coverage(addr, gte_time, lte_time)
        coverage_by_wallet[addr] = coverage
        logger.info("Raw cache coverage verified wallet=%s complete=%s reason=%s count=%s cache_min=%s cache_max=%s", addr, coverage.coverage_complete, coverage.coverage_complete_reason, coverage.raw_tx_count, coverage.cache_min_timestamp, coverage.cache_max_timestamp)
        if not coverage.coverage_complete:
            complete = False
            if not fetch:
                logger.error(
                    "Incomplete raw cache coverage wallet=%s cache_path=%s cache_min=%s cache_max=%s requested_start=%s requested_end=%s missing_ranges=%s rerun without --no-fetch or refresh cache",
                    addr,
                    coverage.cache_path,
                    coverage.cache_min_timestamp,
                    coverage.cache_max_timestamp,
                    coverage.requested_start,
                    coverage.requested_end,
                    coverage.missing_ranges,
                )
    return complete, coverage_by_wallet


def _collect_wallets(wallet_values: Optional[List[str]]) -> List[str]:
    wallets: List[str] = []
    seen: set[str] = set()
    for entry in wallet_values or []:
        for wallet in [w.strip() for w in entry.split(",") if w.strip()]:
            normalized = transfers.normalize_wallet_address(wallet)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            wallets.append(normalized)
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




def _required_price_dates(events: list[NormalizedEvent], fy_period: Optional[utils.Period]) -> tuple[date, date] | None:
    if fy_period is not None:
        return utils.to_au_local(fy_period.start).date(), utils.to_au_local(fy_period.end).date()
    if not events:
        return None
    days = [utils.to_au_local(ev.ts).date() for ev in events]
    return min(days), max(days)


def _events_required_for_fy(events: list[NormalizedEvent], fy_period: Optional[utils.Period]) -> list[NormalizedEvent]:
    if fy_period is None:
        return events
    fy_end = utils.to_au_local(fy_period.end).date()
    return [event for event in events if utils.to_au_local(event.ts).date() <= fy_end]


def _json_default(value):
    from dataclasses import asdict, is_dataclass
    from enum import Enum
    from decimal import Decimal
    from datetime import date as dt_date, datetime as dt_datetime
    from pathlib import Path as _Path
    if is_dataclass(value):
        return asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt_datetime, dt_date)):
        return value.isoformat()
    if isinstance(value, _Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _export_raw_transactions(xlsx_path: Path, wallets: list[str], fy_label: Optional[str], source: str, raw_by_wallet: dict[str, list[dict]]) -> Path:
    output_dir = xlsx_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    total_count = sum(len(items) for items in raw_by_wallet.values())
    payload = {
        "wallets": wallets,
        "fy": fy_label or "all",
        "raw_txs_count": total_count,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "transactions_by_wallet": raw_by_wallet,
    }
    export_path = output_dir / "raw_transactions.json"
    pretty_path = output_dir / "raw_transactions.pretty.json"
    export_path.write_text(json.dumps(payload, default=_json_default, separators=(",", ":")), encoding="utf-8")
    pretty_path.write_text(json.dumps(payload, default=_json_default, indent=2), encoding="utf-8")
    return export_path

def _summary_value(rows: list[dict[str, object]], key: str, default: object = 0) -> object:
    if rows:
        return rows[0].get(key, default)
    return default


def _apply_api_keys_to_env(settings) -> None:
    if settings.api_keys.jupiter:
        os.environ.setdefault("JUP_API_KEY", settings.api_keys.jupiter)
    if settings.api_keys.coingecko:
        os.environ.setdefault("COINGECKO_API_KEY", settings.api_keys.coingecko)


def _dependency_versions(packages: list[str]) -> dict[str, str]:
    versions: dict[str, str] = {}
    for package in packages:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = "not-installed"
    return versions


def _resolve_rpc_url(settings) -> Optional[str]:
    if settings.helius_rpc_url:
        return settings.helius_rpc_url
    return None


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
    helius_token_accounts: str = typer.Option("balanceChanged", "--helius-token-accounts", help="Helius tokenAccounts filter: balanceChanged|all|none"),
    helius_history_provider: str = typer.Option("auto", "--helius-history-provider", help="Helius history provider: auto|enhanced|getTransactionsForAddress"),
    helius_rate_limit_rps: float = typer.Option(2.0, "--helius-rate-limit-rps", help="Helius request rate limit (requests/sec)"),
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
    base_url = _resolve_rpc_url(settings)
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
            provider=helius_history_provider,
            token_accounts=helius_token_accounts,
            rate_limit_rps=helius_rate_limit_rps,
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
    sol_price_csv: Optional[Path] = typer.Option(None, "--sol-price-csv", help="Manual SOL/USD daily CSV path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Normalize only, no accounting"),
    fetch: bool = typer.Option(
        True,
        "--fetch/--no-fetch",
        help="Fetch txs from Helius if cache is empty or missing",
    ),
    refresh_raw_cache: bool = typer.Option(False, "--refresh-raw-cache", help="Refetch requested raw transaction window even if cache appears complete"),
    helius_history_provider: str = typer.Option("auto", "--helius-history-provider", help="Helius history provider: auto|enhanced|getTransactionsForAddress"),
    helius_token_accounts: str = typer.Option("balanceChanged", "--helius-token-accounts", help="Helius tokenAccounts filter: balanceChanged|all|none"),
    helius_rate_limit_rps: float = typer.Option(2.0, "--helius-rate-limit-rps", help="Helius request rate limit (requests/sec)"),
    expected_transaction_count: Optional[int] = typer.Option(None, "--expected-transaction-count", help="Optional expected unique signature count for diagnostics"),
    prefetch_mints: bool = typer.Option(
        True,
        "--prefetch-mints/--no-prefetch-mints",
        help="Prefetch mint decimals via batch RPC before normalization",
    ),
    strict_lots: Optional[bool] = typer.Option(
        None,
        "--strict-lots/--no-strict-lots",
        help="Stop processing when lots are missing (default from config)",
    ),
    enable_auto_backfill: bool = typer.Option(
        False,
        "--enable-auto-backfill",
        help="Enable automatic backfill for missing lot history",
    ),
    backfill_step_days: Optional[int] = typer.Option(
        None,
        "--backfill-step-days",
        help="Days per backfill step when auto-backfill is enabled",
    ),
    max_backfill_days: Optional[int] = typer.Option(
        None,
        "--max-backfill-days",
        help="Maximum days to backfill when auto-backfill is enabled",
    ),
    sol_dust_threshold: str = typer.Option("0.00001", "--sol-dust-threshold", help="SOL dust threshold"),
    aud_dust_threshold: str = typer.Option("0.01", "--aud-dust-threshold", help="AUD dust threshold"),
    include_dust: bool = typer.Option(False, "--include-dust", help="Include dust events"),
    strict: bool = typer.Option(False, "--strict", help="Fail if unsafe taxable rows are detected"),
) -> None:
    _configure_logging()
    parsed_wallets = _collect_wallets(wallet)
    overrides = {"wallets": parsed_wallets} if parsed_wallets else {}
    if method:
        overrides["method"] = method
    if strict_lots is not None:
        overrides["strict_lots"] = strict_lots
    if enable_auto_backfill:
        overrides["auto_backfill"] = True
    if backfill_step_days is not None:
        overrides["backfill_step_days"] = backfill_step_days
    if max_backfill_days is not None:
        overrides["max_backfill_days"] = max_backfill_days
    settings = load_settings(config, overrides)
    if sol_price_csv is not None:
        os.environ["SOL_CGT_SOL_PRICE_CSV"] = str(sol_price_csv)
    _apply_api_keys_to_env(settings)
    wallets = settings.wallets
    if not wallets:
        raise typer.BadParameter("No wallets provided")
    fy_label, fy_period = _resolve_fy_period(fy, fy_start, fy_end)
    gte_time = int(fy_period.start.timestamp()) if fy_period else None
    lte_time = int(fy_period.end.timestamp()) if fy_period else None
    rpc_url = _resolve_rpc_url(settings)
    raw_by_wallet: dict[str, list[dict]] = {}
    cache_coverage_complete, coverage_by_wallet = _ensure_cache_coverage(wallets, gte_time=gte_time, lte_time=lte_time, fetch=fetch, settings=settings, refresh_raw_cache=refresh_raw_cache)
    if not cache_coverage_complete and not fetch:
        raise typer.BadParameter("Cache coverage is incomplete for requested period and --no-fetch was specified. See logs for wallet cache min/max and missing ranges.")
    for addr in wallets:
        if not coverage_by_wallet.get(addr) or coverage_by_wallet[addr].raw_tx_count == 0:
            if fetch:
                asyncio.run(
                    fetch_mod.fetch_wallet(
                        addr,
                        api_key=settings.api_keys.helius,
                        base_url=settings.helius_enhanced_base_url,
                        limit=settings.helius_tx_limit,
                        max_pages=settings.helius_max_pages,
                        gte_time=gte_time,
                        lte_time=lte_time,
                        provider=helius_history_provider,
                        token_accounts=helius_token_accounts,
                        rate_limit_rps=helius_rate_limit_rps,
                    )
                )
            else:
                typer.echo(f"Skipping {addr}: cache empty or missing and --no-fetch specified")
                continue
        raw_items = fetch_mod.load_cached(addr)
        raw_by_wallet[addr] = raw_items

    if xlsx_path and raw_by_wallet:
        try:
            export_path = _export_raw_transactions(xlsx_path, wallets, fy_label, "helius-cache", raw_by_wallet)
        except Exception as exc:
            raise RuntimeError(f"Failed to export raw transactions path={xlsx_path.parent}: {exc}") from exc
        logger.info("Raw transactions exported path=%s wallets=%s count=%s", export_path, len(raw_by_wallet), sum(len(v) for v in raw_by_wallet.values()))

    events, kind_counts = _load_and_normalize(
        wallets,
        settings=settings,
        gte_time=gte_time,
        lte_time=lte_time,
        fetch=fetch,
        prefetch_mints=prefetch_mints,
        rpc_url=rpc_url,
        force_fetch=False,
    )
    _apply_kind_breakdown(kind_counts)
    transfer_stats = transfers.classify_internal_transfers(events, wallets)
    logger.info("Internal transfer classification complete owned_wallets=%s internal_transfers=%s external_transfer_in=%s external_transfer_out=%s", transfer_stats["owned_wallets"], transfer_stats["internal_transfers"], transfer_stats["external_transfer_in"], transfer_stats["external_transfer_out"])
    scoped_events = _events_required_for_fy(events, fy_period)
    if fy_period is not None:
        fy_end = utils.to_au_local(fy_period.end).date().isoformat()
        dropped_post_fy = len(events) - len(scoped_events)
        logger.info(
            "FY event scope applied fy=%s original_events=%s scoped_events=%s dropped_post_fy=%s fy_end=%s",
            fy_label,
            len(events),
            len(scoped_events),
            dropped_post_fy,
            fy_end,
        )
    price_dates = _required_price_dates(scoped_events, fy_period)
    if price_dates is not None:
        start_day, end_day = price_dates
        cache_path = asyncio.run(sol_price_table.ensure_sol_usd_daily_prices(start_day, end_day))
        rows, _, _ = sol_price_table.cache_stats(cache_path)
        logger.info("SOL/USD daily price table ready path=%s start=%s end=%s rows=%s source=kraken", cache_path, start_day.isoformat(), end_day.isoformat(), rows)
        fx_cache_path = asyncio.run(fx_price_table.ensure_usd_aud_daily_rates(start_day, end_day))
        fx_rows, _, _ = fx_price_table.cache_stats(fx_cache_path)
        logger.info("USD/AUD daily FX table ready path=%s start=%s end=%s rows=%s source=frankfurter", fx_cache_path, start_day.isoformat(), end_day.isoformat(), fx_rows)
    usd_provider = TimestampPriceProvider()
    price_provider = AudPriceProvider(
        fx_source=settings.fx_source,
        usd_provider=usd_provider,
    )
    if dry_run:
        typer.echo(f"Loaded {len(scoped_events)} normalized events across {len(wallets)} wallet(s)")
        return
    missing_lot_issues: list[MissingLotIssue] = []
    sol_dust_threshold_dec = Decimal(str(sol_dust_threshold))
    aud_dust_threshold_dec = Decimal(str(aud_dust_threshold))

    valuation_warnings = valuation_module.valuate_events(
        scoped_events,
        valuation_module.ValuationContext(
            usd_provider=usd_provider,
            fx_rate=price_provider.fx_rate,
        ),
    )
    t2t_counters = canonicalize_token_to_token(scoped_events)
    logger.info("token_to_token_groups_detected=%s", t2t_counters["token_to_token_groups_detected"])
    logger.info("token_to_token_canonical_events_created=%s", t2t_counters["token_to_token_canonical_events_created"])
    logger.info("token_to_token_cost_basis_carried_aud=%s", t2t_counters["token_to_token_cost_basis_carried_aud"])
    logger.info("token_to_token_groups_missing_outgoing_lots=%s", t2t_counters["token_to_token_groups_missing_outgoing_lots"])
    logger.info("token_to_token_component_rows_excluded=%s", t2t_counters["token_to_token_component_rows_excluded"])
    t2t_counters = canonicalize_token_to_token(scoped_events)
    logger.info("token_to_token_groups_detected=%s", t2t_counters["token_to_token_groups_detected"])
    logger.info("token_to_token_canonical_events_created=%s", t2t_counters["token_to_token_canonical_events_created"])
    logger.info("token_to_token_cost_basis_carried_aud=%s", t2t_counters["token_to_token_cost_basis_carried_aud"])
    logger.info("token_to_token_groups_missing_outgoing_lots=%s", t2t_counters["token_to_token_groups_missing_outgoing_lots"])
    logger.info("token_to_token_component_rows_excluded=%s", t2t_counters["token_to_token_component_rows_excluded"])
    eligibility = apply_accounting_policy(
        scoped_events,
        sol_dust_threshold=sol_dust_threshold_dec,
        aud_dust_threshold=aud_dust_threshold_dec,
        include_dust=include_dust,
    )
    result, stopped_for_missing = _run_accounting(
        events=eligibility.taxable_events,
        wallets=wallets,
        settings=settings,
        price_provider=price_provider,
        strict_lots=settings.strict_lots,
        missing_lot_issues=missing_lot_issues,
    )
    if stopped_for_missing and missing_lot_issues and not settings.auto_backfill:
        logger.warning("Auto-backfill disabled; missing lot history will be reported instead of fetched.")
    if stopped_for_missing and settings.auto_backfill and missing_lot_issues:
        issue = missing_lot_issues[-1]
        backfill_end = issue.ts
        if fy_period and backfill_end < fy_period.start:
            backfill_end = fy_period.start
        backfill_days = 0
        missing_lot_count_before = len(missing_lot_issues)
        while backfill_days < settings.max_backfill_days:
            backfill_days += settings.backfill_step_days
            new_start = backfill_end - timedelta(days=backfill_days)
            raw_before = sum(len(fetch_mod.load_cached(addr)) for addr in wallets)
            events_before = len(scoped_events)
            logger.info(
                "Auto-backfill attempt=%s reason=missing_lot_history days=%s start=%s end=%s",
                (backfill_days // settings.backfill_step_days),
                backfill_days,
                new_start.isoformat(),
                backfill_end.isoformat(),
            )
            events, kind_counts = _load_and_normalize(
                wallets,
                settings=settings,
                gte_time=int(new_start.timestamp()),
                lte_time=int(backfill_end.timestamp()),
                fetch=True,
                prefetch_mints=prefetch_mints,
                rpc_url=rpc_url,
                force_fetch=True,
            )
            _apply_kind_breakdown(kind_counts)
            transfer_stats = transfers.classify_internal_transfers(events, wallets)
            logger.info("Internal transfer classification complete owned_wallets=%s internal_transfers=%s external_transfer_in=%s external_transfer_out=%s", transfer_stats["owned_wallets"], transfer_stats["internal_transfers"], transfer_stats["external_transfer_in"], transfer_stats["external_transfer_out"])
            scoped_events = _events_required_for_fy(events, fy_period)
            if fy_period is not None:
                fy_end = utils.to_au_local(fy_period.end).date().isoformat()
                dropped_post_fy = len(events) - len(scoped_events)
                logger.info(
                    "FY event scope applied fy=%s original_events=%s scoped_events=%s dropped_post_fy=%s fy_end=%s",
                    fy_label,
                    len(events),
                    len(scoped_events),
                    dropped_post_fy,
                    fy_end,
                )
            missing_lot_issues.clear()

            valuation_warnings = valuation_module.valuate_events(
                scoped_events,
                valuation_module.ValuationContext(
                    usd_provider=usd_provider,
                    fx_rate=price_provider.fx_rate,
                ),
            )
            eligibility = apply_accounting_policy(
                scoped_events,
                sol_dust_threshold=sol_dust_threshold_dec,
                aud_dust_threshold=aud_dust_threshold_dec,
                include_dust=include_dust,
            )
            result, stopped_for_missing = _run_accounting(
                events=eligibility.taxable_events,
                wallets=wallets,
                settings=settings,
                price_provider=price_provider,
                strict_lots=settings.strict_lots,
                missing_lot_issues=missing_lot_issues,
            )
            raw_after = sum(len(fetch_mod.load_cached(addr)) for addr in wallets)
            events_after = len(scoped_events)
            missing_lot_count_after = len(missing_lot_issues)
            if raw_after <= raw_before and events_after <= events_before:
                logger.warning("Auto-backfill stopped reason=no_new_events")
                break
            if missing_lot_count_after >= missing_lot_count_before:
                logger.warning("Auto-backfill stopped reason=no_missing_lot_improvement")
                break
            missing_lot_count_before = missing_lot_count_after
            if not stopped_for_missing:
                break
        if stopped_for_missing:
            logger.warning(
                "Auto-backfill reached max days (%s) with missing lots still present.",
                settings.max_backfill_days,
            )
    disposals = result.disposals
    acquisitions = result.acquisitions
    lot_moves = result.lot_moves
    warnings = [*valuation_warnings, *result.warnings]
    warnings.extend(
        WarningRecord(
            ts=issue.ts,
            wallet=issue.wallet,
            signature=issue.signature,
            event_id=issue.event_id,
            mint=issue.mint,
            amount=issue.shortfall_qty,
            reason="missing_lot_history",
            code="missing_lot_history",
            message=issue.message,
        )
        for issue in missing_lot_issues
    )
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
    violations = [
        lot for lot in acquisitions
        if lot.unit_cost_aud == Decimal("0") and lot.source_type not in {"airdrop", "income", "gift"}
    ]
    if violations and strict:
        raise typer.BadParameter(f"Unsafe zero-cost lots detected: {len(violations)}")
    if violations and not strict:
        blocked_ids = {lot.source_event for lot in violations}
        acquisitions = [lot for lot in acquisitions if lot.source_event not in blocked_ids]
        eligibility.manual_review.extend({"event_id": e.id, "reason": "zero_cost_lot_blocked", "timestamp": e.ts.isoformat(), "wallet": e.wallet} for e in scoped_events if e.id in blocked_ids)
    missing_lot_warnings = [w for w in warnings if w.code == "missing_lot_history"]
    valuation_warning_rows = [w.model_dump() for w in warnings if "price" in w.code]
    raw_signatures = {str((item.get("signature") or item.get("id"))) for addr in wallets for item in fetch_mod.load_cached(addr) if (item.get("signature") or item.get("id"))}
    transaction_summary = summaries.build_transaction_summary(scoped_events)
    reconciliation_summary = summaries.build_reconciliation_summary(raw_signatures, transaction_summary, scoped_events)
    if reconciliation_summary:
        reconciliation_summary[0]["history_provider"] = "enhanced" if helius_history_provider == "auto" else helius_history_provider
        reconciliation_summary[0]["token_accounts_mode"] = helius_token_accounts
        reconciliation_summary[0]["refreshed_this_run"] = refresh_raw_cache
        if expected_transaction_count is not None:
            delta = len(raw_signatures) - expected_transaction_count
            reconciliation_summary[0]["expected_transaction_count"] = expected_transaction_count
            reconciliation_summary[0]["expected_count_delta"] = delta
            reconciliation_summary[0]["reconciliation_status"] = "ok" if delta == 0 else "mismatch"
        if coverage_by_wallet:
            first_wallet = wallets[0]
            checked = coverage_by_wallet[first_wallet].provider_checked_ranges or []
            latest = checked[-1] if checked else {}
            reconciliation_summary[0]["provider_checked_start"] = latest.get("checked_start")
            reconciliation_summary[0]["provider_checked_end"] = latest.get("checked_end")
            reconciliation_summary[0]["provider_exhausted"] = latest.get("exhausted")
            reconciliation_summary[0]["coverage_complete_reason"] = coverage_by_wallet[first_wallet].coverage_complete_reason
            reconciliation_summary[0]["earliest_returned_timestamp"] = latest.get("earliest_returned_timestamp")
            reconciliation_summary[0]["latest_returned_timestamp"] = latest.get("latest_returned_timestamp")
    excluded_events = [*eligibility.manual_review, *eligibility.dust_ignored]
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
    incomplete_wallets = [w for w, c in coverage_by_wallet.items() if not c.coverage_complete]
    overview_row = {
        "accounting_complete": not bool(excluded_events or missing_lot_issues),
        "cache_coverage_complete": cache_coverage_complete,
        "cache_coverage_checked": True,
        "wallets_with_incomplete_cache": ",".join(incomplete_wallets),
        "raw_tx_count_by_wallet": utils.json_dumps({w: c.raw_tx_count for w, c in coverage_by_wallet.items()}),
        "cache_min_by_wallet": utils.json_dumps({w: c.cache_min_timestamp for w, c in coverage_by_wallet.items()}),
        "cache_max_by_wallet": utils.json_dumps({w: c.cache_max_timestamp for w, c in coverage_by_wallet.items()}),
        "missing_ranges_by_wallet": utils.json_dumps({w: c.missing_ranges for w, c in coverage_by_wallet.items()}),
        "taxable_events_processed": len(eligibility.taxable_events),
        "manual_review_events": len(eligibility.manual_review),
        "missing_lot_events": len(missing_lot_issues),
        "excluded_events": len(excluded_events),
        "missing_price_events": len([w for w in warnings if w.code == "missing_price"]),
        "dust_ignored_events": len(eligibility.dust_ignored),
    }
    formats.export_reports(
        output_dir, acquisitions, disposals, summary_by_token, summary_overall, fmt=fmt,
        manual_review=eligibility.manual_review, dust_ignored=eligibility.dust_ignored,
        internal_transfers=lot_moves, transaction_summary=transaction_summary,
        excluded_events=excluded_events, valuation_warnings=valuation_warning_rows,
        missing_lot_warnings=[w.model_dump() for w in missing_lot_warnings], overview=[overview_row]
    )
    if xlsx_path:
        for event in scoped_events:
            if event.fee_sol and "fee_aud" not in event.raw:
                fee_price = price_provider.price_aud("SOL", event.ts, context=event.raw)
                if fee_price is None:
                    logger.warning("Missing SOL fee price for %s at %s", event.id, event.ts.isoformat())
                    continue
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
                "Missing lots": str(len(missing_lot_issues)),
                "Accounting complete": str(not bool(excluded_events or missing_lot_issues)),
                "Cache coverage complete": str(cache_coverage_complete),
            },
            events=[ev for ev in scoped_events if not fy_period or fy_period.start <= ev.ts <= fy_period.end],
            lots=acquisitions,
            disposals=disposals,
            summary_by_token=summary_by_token,
            wallet_summary=wallet_summary,
            lot_moves=lot_moves,
            warnings=warnings,
            missing_lots=missing_lot_issues,
            price_provider=price_provider,
            transaction_summary=transaction_summary,
            manual_review=eligibility.manual_review,
            excluded_events=excluded_events,
            valuation_warnings=valuation_warning_rows,
            missing_lot_warnings=[w.model_dump() for w in missing_lot_warnings],
            taxable_acquisitions=[lot.model_dump() for lot in acquisitions],
            taxable_disposals=[d.model_dump() for d in disposals],
            normalized_events_debug=[ev.model_dump() for ev in scoped_events],
            dust_ignored=eligibility.dust_ignored,
            reconciliation=reconciliation_summary,
        )
    console_report.render_summary(disposals, acquisitions, warnings)
    typer.echo("Manual review items were excluded from taxable CGT totals.")
    typer.echo(f"Excluded: internal_transfers={len(lot_moves)} dust_ignored={len(eligibility.dust_ignored)} manual_review={len(eligibility.manual_review)}")
    if stopped_for_missing and settings.strict_lots:
        logger.error(
            "Missing lots detected (count=%s). Results may be incomplete until resolved.",
            len(missing_lot_issues),
        )
        raise typer.Exit(code=1)


@app.command()
def reconcile(
    wallet: List[str] = typer.Option(None, "--wallet", "-w", help="Wallet address", show_default=False),
    config: Optional[Path] = typer.Option(None, "--config", help="Config YAML"),
    fy: Optional[str] = typer.Option(None, "--fy", help="Australian financial year (e.g. 2024-2025)"),
    fetch: bool = typer.Option(True, "--fetch/--no-fetch", help="Fetch missing raw transactions"),
) -> None:
    _configure_logging()
    settings = load_settings(config)
    wallets = _collect_wallets(wallet) or settings.wallets
    _, fy_period = _resolve_fy_period(fy, None, None)
    gte_time = int(fy_period.start.timestamp()) if fy_period else None
    lte_time = int(fy_period.end.timestamp()) if fy_period else None
    _ensure_cache_coverage(wallets, gte_time=gte_time, lte_time=lte_time, fetch=fetch, settings=settings, refresh_raw_cache=True)
    events, _ = _load_and_normalize(wallets, settings=settings, gte_time=gte_time, lte_time=lte_time, fetch=fetch, prefetch_mints=True, rpc_url=None, force_fetch=False)
    transfers.classify_internal_transfers(events, wallets)
    transaction_summary = summaries.build_transaction_summary(events)
    raw_signatures = {str((item.get("signature") or item.get("id"))) for addr in wallets for item in fetch_mod.load_cached(addr) if (item.get("signature") or item.get("id"))}
    rec = summaries.build_reconciliation_summary(raw_signatures, transaction_summary, events)
    typer.echo(json.dumps(rec[0], indent=2))


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
    typer.echo(f"Jupiter token v1 URL: {jupiter_provider.JUPITER_TOKENS_V1_URL}")
    typer.echo(f"Jupiter token v2 URL: {jupiter_provider.JUPITER_TOKENS_V2_URL}")
    typer.echo(f"Jupiter price base URL: {jupiter_provider._price_base_url(os.getenv('JUP_API_KEY'))}")
    typer.echo(f"Jupiter RPC URL: {utils.redact_api_key(jupiter_provider._rpc_url())}")
