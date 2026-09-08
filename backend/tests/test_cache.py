from __future__ import annotations

import pytest

from app.market_data import PriceCache

pytestmark = pytest.mark.asyncio


async def test_first_update_has_flat_direction():
    cache = PriceCache()
    tick = await cache.update("aapl", 190.0, 0.5)
    assert tick.ticker == "AAPL"          # normalized on write
    assert tick.prev_price == 190.0       # no predecessor
    assert tick.direction == "flat"


async def test_prev_price_tracks_the_previous_tick():
    cache = PriceCache()
    await cache.update("AAPL", 190.0, 0.0)
    tick = await cache.update("AAPL", 191.0, 0.5)
    assert tick.prev_price == 190.0
    assert tick.direction == "up"


async def test_get_is_case_insensitive():
    cache = PriceCache()
    await cache.update("AAPL", 190.0, 0.0)
    assert cache.get("aapl") is not None


async def test_get_returns_none_for_unknown_ticker():
    cache = PriceCache()
    assert cache.get("NOPE") is None


async def test_update_many_shares_one_timestamp():
    cache = PriceCache()
    ticks = await cache.update_many([("AAPL", 190.0, 0.1), ("MSFT", 420.0, 0.2)])
    assert len({t.timestamp for t in ticks}) == 1


async def test_update_many_normalizes_ticker_case():
    cache = PriceCache()
    ticks = await cache.update_many([("aapl", 190.0, 0.1)])
    assert ticks[0].ticker == "AAPL"


async def test_snapshot_is_an_isolated_copy():
    cache = PriceCache()
    await cache.update("AAPL", 190.0, 0.0)
    snap = cache.snapshot()
    await cache.update("AAPL", 191.0, 0.0)
    assert snap["AAPL"].price == 190.0    # frozen ticks, copied dict


async def test_snapshot_empty_cache_is_empty_dict():
    cache = PriceCache()
    assert cache.snapshot() == {}
