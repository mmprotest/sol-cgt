from __future__ import annotations

import asyncio
from decimal import Decimal

from sol_cgt.ingestion import normalize
from sol_cgt.accounting.eligibility import apply_accounting_policy


def test_routed_swap_canonicalization(tmp_path) -> None:

    raw_tx = {
        "signature": "sigswap",
        "timestamp": 1700000000,
        "fee": 5000,
        "events": {
            "swap": {
                "tokenInputs": [
                    {"mint": "TOKENA", "decimals": 6, "amount": "5"},
                    {"mint": "TOKENB", "decimals": 6, "amount": "3"},
                ],
                "tokenOutputs": [
                    {"mint": "TOKENC", "decimals": 6, "amount": "7"},
                    {"mint": "TOKEND", "decimals": 6, "amount": "1"},
                ],
            }
        },
        "tokenTransfers": [
            {
                "mint": "TOKENA",
                "tokenAmount": "5",
                "tokenDecimals": 6,
                "tokenSymbol": "TKA",
                "fromUserAccount": "WALLET",
                "toUserAccount": "POOL",
            },
            {
                "mint": "TOKENB",
                "tokenAmount": "3",
                "tokenDecimals": 6,
                "tokenSymbol": "TKB",
                "fromUserAccount": "WALLET",
                "toUserAccount": "POOL",
            },
            {
                "mint": "TOKENC",
                "tokenAmount": "7",
                "tokenDecimals": 6,
                "tokenSymbol": "TKC",
                "fromUserAccount": "POOL",
                "toUserAccount": "WALLET",
            },
            {
                "mint": "TOKEND",
                "tokenAmount": "1",
                "tokenDecimals": 6,
                "tokenSymbol": "TKD",
                "fromUserAccount": "POOL",
                "toUserAccount": "WALLET",
            },
        ],
    }

    events = asyncio.run(
        normalize.normalize_wallet_events(
            "WALLET",
            [raw_tx],
            mint_cache_path=tmp_path / "mint_meta.json",
        )
    )
    assert all(ev.kind == "swap" for ev in events)
    assert len(events) == 4
    totals = {}
    for ev in events:
        token = ev.base_token or ev.quote_token
        assert token is not None
        totals[token.mint] = totals.get(token.mint, Decimal("0")) + (
            -token.amount if ev.base_token is not None else token.amount
        )
    assert totals == {
        "TOKENA": Decimal("-5"),
        "TOKENB": Decimal("-3"),
        "TOKENC": Decimal("7"),
        "TOKEND": Decimal("1"),
    }


def test_infer_buy_from_transfer_group_anchor(tmp_path) -> None:
    tx = {
        "signature": "sigbuy",
        "timestamp": 1700000001,
        "tokenTransfers": [
            {"mint": "TOKENX", "tokenAmount": "100", "tokenDecimals": 6, "tokenSymbol": "TKX", "fromUserAccount": "POOL", "toUserAccount": "WALLET"},
        ],
        "nativeTransfers": [
            {"amount": 1_000_000_000, "fromUserAccount": "WALLET", "toUserAccount": "POOL"},
            {"amount": 1_000, "fromUserAccount": "WALLET", "toUserAccount": "RENT"},
        ],
    }
    evs = asyncio.run(normalize.normalize_wallet_events("WALLET", [tx], mint_cache_path=tmp_path / "mint_meta.json"))
    for ev in evs:
        if ev.kind == "transfer_out" and (ev.base_token and ev.base_token.mint == "SOL"):
            ev.raw["proceeds_hint_aud"] = "40" if ev.base_token.amount > Decimal("0.1") else "0"
    evs = normalize._canonicalize_transfer_group_as_swap_if_possible(evs, "sigbuy", "WALLET")
    inferred = [e for e in evs if e.raw.get("source") == "inferred_transfer_swap"]
    assert len(inferred) == 1
    assert inferred[0].quote_token is not None
    assert inferred[0].raw.get("cost_hint_aud") == "40"
    res = apply_accounting_policy(evs, sol_dust_threshold=Decimal("0.00001"), aud_dust_threshold=Decimal("0.01"), include_dust=False)
    assert len([e for e in res.taxable_events if e.raw.get("source") == "inferred_transfer_swap"]) == 1
    assert any(r["reason"] == "swap_component" for r in res.manual_review)


def test_infer_sell_from_transfer_group_anchor(tmp_path) -> None:
    tx = {
        "signature": "sigsell",
        "timestamp": 1700000002,
        "tokenTransfers": [
            {"mint": "TOKENY", "tokenAmount": "50", "tokenDecimals": 6, "tokenSymbol": "TKY", "fromUserAccount": "WALLET", "toUserAccount": "POOL"},
        ],
        "nativeTransfers": [
            {"amount": 500_000_000, "fromUserAccount": "POOL", "toUserAccount": "WALLET"},
        ],
    }
    evs = asyncio.run(normalize.normalize_wallet_events("WALLET", [tx], mint_cache_path=tmp_path / "mint_meta.json"))
    for ev in evs:
        if ev.kind == "transfer_in" and (ev.quote_token and ev.quote_token.mint == "SOL"):
            ev.raw["cost_hint_aud"] = "25"
    evs = normalize._canonicalize_transfer_group_as_swap_if_possible(evs, "sigsell", "WALLET")
    inferred = [e for e in evs if e.raw.get("source") == "inferred_transfer_swap"]
    assert len(inferred) == 1
    assert inferred[0].base_token is not None
    assert inferred[0].raw.get("proceeds_hint_aud") == "25"


def test_multi_non_anchor_remains_manual_review(tmp_path) -> None:
    tx = {
        "signature": "sigmulti",
        "timestamp": 1700000003,
        "tokenTransfers": [
            {"mint": "T1", "tokenAmount": "1", "tokenDecimals": 6, "fromUserAccount": "POOL", "toUserAccount": "WALLET"},
            {"mint": "T2", "tokenAmount": "1", "tokenDecimals": 6, "fromUserAccount": "WALLET", "toUserAccount": "POOL"},
        ],
    }
    evs = asyncio.run(normalize.normalize_wallet_events("WALLET", [tx], mint_cache_path=tmp_path / "mint_meta.json"))
    assert not any(e.raw.get("source") == "inferred_transfer_swap" for e in evs)
