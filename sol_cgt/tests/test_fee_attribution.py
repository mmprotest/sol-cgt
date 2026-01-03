from __future__ import annotations

import asyncio
from decimal import Decimal

from sol_cgt.ingestion import normalize


def test_fee_attributed_to_fee_payer(tmp_path) -> None:
    raw_tx = {
        "signature": "sigfee",
        "timestamp": 1700000000,
        "fee": 10000,
        "feePayer": "W1",
        "tokenTransfers": [
            {
                "mint": "TOKENA",
                "tokenAmount": "1",
                "tokenDecimals": 6,
                "tokenSymbol": "TKA",
                "fromUserAccount": "W1",
                "toUserAccount": "W2",
            }
        ],
    }

    cache_path = tmp_path / "mint_meta.json"
    events_w1 = asyncio.run(normalize.normalize_wallet_events("W1", [raw_tx], mint_cache_path=cache_path))
    assert len(events_w1) == 1
    assert events_w1[0].fee_sol > Decimal("0")

    events_w2 = asyncio.run(normalize.normalize_wallet_events("W2", [raw_tx], mint_cache_path=cache_path))
    assert len(events_w2) == 1
    assert events_w2[0].fee_sol == Decimal("0")
