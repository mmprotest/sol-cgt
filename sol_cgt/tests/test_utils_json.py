from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from sol_cgt import utils


def test_json_dumps_nested_decimal() -> None:
    payload = {"outer": [{"amount": Decimal("1.23")}]}
    output = utils.json_loads(utils.json_dumps(payload))
    assert output["outer"][0]["amount"] == "1.23"


def test_json_dumps_nested_datetime_and_date() -> None:
    payload = {"items": [{"ts": datetime(2024, 1, 1, tzinfo=timezone.utc), "d": date(2024, 1, 1)}]}
    output = utils.json_loads(utils.json_dumps(payload))
    assert output["items"][0]["ts"] == "2024-01-01T00:00:00+00:00"
    assert output["items"][0]["d"] == "2024-01-01"


def test_json_dumps_set_and_tuple() -> None:
    payload = {"values": ({1, 2}, (3, 4))}
    output = utils.json_loads(utils.json_dumps(payload))
    assert isinstance(output["values"], list)
    assert sorted(output["values"][0]) == [1, 2]
    assert output["values"][1] == [3, 4]
