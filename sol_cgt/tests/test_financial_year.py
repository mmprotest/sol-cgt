from __future__ import annotations

from datetime import datetime, timezone

from sol_cgt.utils import australian_financial_year_bounds


def test_financial_year_boundary() -> None:
    fy_2024 = australian_financial_year_bounds("2023-2024")
    fy_2025 = australian_financial_year_bounds("2024-2025")

    ts_before = datetime(2024, 6, 30, 13, 59, tzinfo=timezone.utc)
    ts_after = datetime(2024, 6, 30, 14, 0, tzinfo=timezone.utc)

    assert fy_2024.start <= ts_before <= fy_2024.end
    assert fy_2025.start <= ts_after <= fy_2025.end
