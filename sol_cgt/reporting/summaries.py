"""Summary helpers for reporting."""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Iterable

import pandas as pd

from ..types import DisposalRecord


def summarize_by_token(disposals: Iterable[DisposalRecord]) -> pd.DataFrame:
    aggregates: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "proceeds_aud": Decimal("0"),
            "cost_base_aud": Decimal("0"),
            "fees_aud": Decimal("0"),
            "gain_loss_aud": Decimal("0"),
            "disposals": Decimal("0"),
            "token_symbol": "",
        }
    )
    for record in disposals:
        bucket = aggregates[record.token_mint]
        bucket["token_symbol"] = record.token_symbol or bucket["token_symbol"]
        bucket["proceeds_aud"] += record.proceeds_aud
        bucket["cost_base_aud"] += record.cost_base_aud
        bucket["fees_aud"] += record.fees_aud
        bucket["gain_loss_aud"] += record.gain_loss_aud
        bucket["disposals"] += Decimal("1")
    rows = []
    for mint, data in aggregates.items():
        rows.append(
            {
                "token_mint": mint,
                "token_symbol": data["token_symbol"],
                "proceeds_aud": float(data["proceeds_aud"]),
                "cost_base_aud": float(data["cost_base_aud"]),
                "fees_aud": float(data["fees_aud"]),
                "gain_loss_aud": float(data["gain_loss_aud"]),
                "disposals": int(data["disposals"]),
            }
        )
    return pd.DataFrame(rows)


def summarize_overall(disposals: Iterable[DisposalRecord]) -> pd.DataFrame:
    proceeds = Decimal("0")
    cost = Decimal("0")
    fees = Decimal("0")
    gain = Decimal("0")
    count = 0
    for record in disposals:
        proceeds += record.proceeds_aud
        cost += record.cost_base_aud
        fees += record.fees_aud
        gain += record.gain_loss_aud
        count += 1
    return pd.DataFrame(
        [
            {
                "proceeds_aud": float(proceeds),
                "cost_base_aud": float(cost),
                "fees_aud": float(fees),
                "gain_loss_aud": float(gain),
                "disposals": count,
            }
        ]
    )
