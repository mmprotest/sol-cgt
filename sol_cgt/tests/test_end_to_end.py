from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from sol_cgt.accounting.engine import AccountingEngine
from sol_cgt.ingestion import normalize
from sol_cgt.reconciliation.transfers import detect_self_transfers


class FakePriceProvider:
    def price_aud(self, mint: str, ts: datetime, *, context: dict | None = None) -> Decimal:
        key = (mint, ts.date().isoformat())
        prices = {
            ("SOL", "2024-01-01"): Decimal("30"),
            ("SOL", "2024-02-01"): Decimal("30"),
            ("TOKENX", "2024-02-01"): Decimal("7.5"),
            ("TOKENX", "2024-04-01"): Decimal("9"),
            ("USDC", "2024-04-01"): Decimal("1.5"),
        }
        if key in prices:
            return prices[key]
        raise ValueError(f"missing price for {key}")


def test_end_to_end_pipeline(monkeypatch) -> None:
    async def fake_metadata(mint: str):
        return (mint[:3], 6)

    async def fake_jupiter_metadata(mint: str):
        return (None, None)

    monkeypatch.setattr(normalize.jupiter, "token_metadata", fake_jupiter_metadata)
    monkeypatch.setattr(normalize.birdeye, "token_metadata", fake_metadata)

    wallet1 = "W1"
    wallet2 = "W2"

    raw_txs_w1 = [
        {
            "signature": "buy1",
            "timestamp": int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp()),
            "fee": 1000,
            "nativeTransfers": [
                {
                    "fromUserAccount": "EXCHANGE",
                    "toUserAccount": wallet1,
                    "amount": 1_000_000_000,
                }
            ],
        },
        {
            "signature": "swap1",
            "timestamp": int(datetime(2024, 2, 1, tzinfo=timezone.utc).timestamp()),
            "fee": 1000,
            "events": {
                "swap": {
                    "nativeInput": {"amount": 1_000_000_000},
                    "tokenOutputs": [{"mint": "TOKENX", "decimals": 6, "amount": "4"}],
                }
            },
        },
        {
            "signature": "move1",
            "timestamp": int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp()),
            "tokenTransfers": [
                {
                    "mint": "TOKENX",
                    "tokenAmount": "4",
                    "tokenDecimals": 6,
                    "tokenSymbol": "TKX",
                    "fromUserAccount": wallet1,
                    "toUserAccount": wallet2,
                }
            ],
        },
    ]

    raw_txs_w2 = [
        {
            "signature": "move1",
            "timestamp": int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp()),
            "tokenTransfers": [
                {
                    "mint": "TOKENX",
                    "tokenAmount": "4",
                    "tokenDecimals": 6,
                    "tokenSymbol": "TKX",
                    "fromUserAccount": wallet1,
                    "toUserAccount": wallet2,
                }
            ],
        },
        {
            "signature": "sell1",
            "timestamp": int(datetime(2024, 4, 1, tzinfo=timezone.utc).timestamp()),
            "events": {
                "swap": {
                    "tokenInputs": [{"mint": "TOKENX", "decimals": 6, "amount": "4"}],
                    "tokenOutputs": [{"mint": "USDC", "decimals": 6, "amount": "24"}],
                }
            },
        },
    ]

    events = []
    events.extend(asyncio.run(normalize.normalize_wallet_events(wallet1, raw_txs_w1)))
    events.extend(asyncio.run(normalize.normalize_wallet_events(wallet2, raw_txs_w2)))

    matches = detect_self_transfers(events, wallets=[wallet1, wallet2])
    engine = AccountingEngine(price_provider=FakePriceProvider())
    result = engine.process(events, wallets=[wallet1, wallet2], transfer_matches=matches)

    assert len(result.lot_moves) == 1
    assert len(result.disposals) == 2
    disposal = next(d for d in result.disposals if d.token_mint == "TOKENX")
    assert disposal.wallet == wallet2
    assert disposal.gain_loss_aud == Decimal("6.00")


def test_pipeline_metadata_failures_do_not_crash(monkeypatch) -> None:
    async def failing_jupiter_metadata(mint: str):
        raise httpx.ConnectError("down", request=httpx.Request("GET", "https://rpc"))

    async def failing_birdeye_metadata(mint: str):
        raise httpx.ConnectError("down", request=httpx.Request("GET", "https://birdeye"))

    monkeypatch.setattr(normalize.jupiter, "token_metadata", failing_jupiter_metadata)
    monkeypatch.setattr(normalize.birdeye, "token_metadata", failing_birdeye_metadata)

    raw_txs = [
        {
            "signature": "meta1",
            "timestamp": int(datetime(2024, 5, 1, tzinfo=timezone.utc).timestamp()),
            "tokenTransfers": [
                {
                    "mint": "TOKENZ",
                    "tokenAmount": "1.0",
                    "tokenDecimals": None,
                    "tokenSymbol": None,
                    "fromUserAccount": "OTHER",
                    "toUserAccount": "WALLET",
                }
            ],
        }
    ]

    events = asyncio.run(normalize.normalize_wallet_events("WALLET", raw_txs))

    class FixedPriceProvider:
        def price_aud(self, mint: str, ts: datetime, *, context: dict | None = None) -> Decimal:
            return Decimal("2")

    engine = AccountingEngine(price_provider=FixedPriceProvider())
    result = engine.process(events, wallets=["WALLET"])

    assert result.acquisitions
    assert any(warning.code == "default_decimals" for warning in result.warnings)
