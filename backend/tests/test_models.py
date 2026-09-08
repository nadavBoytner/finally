from __future__ import annotations

import re

from app.market_data.models import PriceTick, now_iso


def test_now_iso_has_z_suffix_and_millisecond_precision():
    ts = now_iso()
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", ts)


def test_direction_up_down_flat():
    up = PriceTick(ticker="AAPL", price=101.0, prev_price=100.0, change_pct=1.0, timestamp="t")
    down = PriceTick(ticker="AAPL", price=99.0, prev_price=100.0, change_pct=-1.0, timestamp="t")
    flat = PriceTick(ticker="AAPL", price=100.0, prev_price=100.0, change_pct=0.0, timestamp="t")
    assert up.direction == "up"
    assert down.direction == "down"
    assert flat.direction == "flat"


def test_to_dict_wire_format_is_camel_case_and_rounded():
    tick = PriceTick(
        ticker="aapl",
        price=190.00001,
        prev_price=189.999999,
        change_pct=0.123456,
        timestamp="2026-01-01T00:00:00.000Z",
    )
    payload = tick.to_dict()
    assert payload == {
        "ticker": "aapl",
        "price": 190.0,
        "prevPrice": 190.0,
        "changePct": 0.1235,
        "direction": "up",
        "timestamp": "2026-01-01T00:00:00.000Z",
    }


def test_price_tick_is_frozen():
    tick = PriceTick(ticker="AAPL", price=1.0, prev_price=1.0, change_pct=0.0, timestamp="t")
    try:
        tick.price = 2.0  # type: ignore[misc]
    except AttributeError:
        pass
    else:
        raise AssertionError("PriceTick must be immutable")
