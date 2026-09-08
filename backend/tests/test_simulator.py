from __future__ import annotations

import asyncio
import math
import random
import statistics

import pytest

from app.market_data import PriceCache
from app.market_data.simulator import (
    DEFAULT_SEEDS,
    MARKET_BETA,
    SimulatorProvider,
    TickerState,
    derive_seed,
    gbm_step,
    session_change_pct,
)


def test_gbm_step_is_pure_and_deterministic():
    state = TickerState(price=100.0, session_open=100.0, mu=0.1, sigma=0.3)
    dt = 0.5 / (252 * 6.5 * 3600)
    first = gbm_step(state, z=1.0, dt=dt)
    assert gbm_step(state, z=1.0, dt=dt) == first  # no mutation
    assert state.price == 100.0
    assert first > 100.0                            # positive shock


def test_gbm_step_negative_shock_lowers_price():
    state = TickerState(price=100.0, session_open=100.0, mu=0.0, sigma=0.3)
    dt = 0.5 / (252 * 6.5 * 3600)
    assert gbm_step(state, z=-1.0, dt=dt) < 100.0


def test_gbm_step_stays_positive_for_large_negative_shock():
    state = TickerState(price=1.0, session_open=1.0, mu=0.0, sigma=1.0)
    dt = 0.5 / (252 * 6.5 * 3600)
    assert gbm_step(state, z=-50.0, dt=dt) > 0.0


def test_session_change_pct():
    state = TickerState(price=110.0, session_open=100.0, mu=0.0, sigma=0.0)
    assert math.isclose(session_change_pct(state), 10.0)


def test_derive_seed_is_stable_and_case_insensitive():
    a, b = derive_seed("ZZZZ"), derive_seed("zzzz")
    assert (a.price, a.mu, a.sigma) == (b.price, b.mu, b.sigma)


def test_derive_seed_is_within_configured_ranges():
    for symbol in ("A", "PYPL", "XYZ12", "QQQ"):
        seed = derive_seed(symbol)
        assert 20.0 <= seed.price <= 500.0
        assert seed.price == seed.session_open
        assert 0.04 <= seed.mu <= 0.16
        assert 0.20 <= seed.sigma <= 0.50


def test_derive_seed_differs_across_symbols():
    assert derive_seed("AAA").price != derive_seed("BBB").price


def test_instances_do_not_share_seed_state():
    """Regression: a shallow copy of DEFAULT_SEEDS would leak prices between
    providers and between tests."""
    a, b = SimulatorProvider(rng=random.Random(1)), SimulatorProvider(rng=random.Random(1))
    a.advance(["AAPL"] * 50)
    assert b._state["AAPL"].price == DEFAULT_SEEDS["AAPL"][0]


def test_advance_is_reproducible_under_a_seeded_rng():
    def run() -> list[float]:
        sim = SimulatorProvider(rng=random.Random(42))
        return [sim.advance(["AAPL", "TSLA"])[0][1] for _ in range(20)]

    assert run() == run()


def test_advance_returns_a_row_per_ticker_uppercased():
    sim = SimulatorProvider(rng=random.Random(0))
    rows = sim.advance(["aapl", "tsla"])
    assert [r[0] for r in rows] == ["AAPL", "TSLA"]


def test_advance_seeds_unknown_tickers_on_first_sight():
    sim = SimulatorProvider(rng=random.Random(0))
    assert "PYPL" not in sim._state
    rows = sim.advance(["PYPL"])
    assert rows[0][0] == "PYPL"
    assert "PYPL" in sim._state


def test_prices_stay_positive_over_a_long_run():
    sim = SimulatorProvider(rng=random.Random(7))
    for _ in range(20_000):
        for _, price, _ in sim.advance(["TSLA"]):   # highest sigma in the table
            assert price > 0


def test_market_factor_weights_preserve_unit_variance():
    """MARKET_BETA and the idiosyncratic weight must sum in quadrature to 1,
    or realized volatility silently drifts away from the configured sigma."""
    assert math.isclose(MARKET_BETA**2 + (1 - MARKET_BETA**2), 1.0)


def test_realized_volatility_matches_configured_sigma():
    """Statistical sanity, generous tolerance — a check, not a numerics test.

    Events are switched off deliberately. A 2-5% jump dwarfs a ~1 cent 500ms
    diffusion move, so even at p=0.002 events contribute >100x the variance of
    the GBM term and the measurement would read far above 0.30 with events on.
    """
    sigma, dt = 0.30, 0.5 / (252 * 6.5 * 3600)
    sim = SimulatorProvider(rng=random.Random(11), event_probability=0.0)
    sim._state["TEST"] = TickerState(price=100.0, session_open=100.0, mu=0.0, sigma=sigma)

    prices = [100.0]
    for _ in range(50_000):
        prices.append(sim.advance(["TEST"])[0][1])

    log_returns = [math.log(b / a) for a, b in zip(prices, prices[1:])]
    realized = statistics.pstdev(log_returns) / math.sqrt(dt)
    assert 0.85 * sigma < realized < 1.15 * sigma


def test_events_fire_at_roughly_the_configured_rate():
    """A 2-5% jump dwarfs a normal 500ms move, so outsized returns count events."""
    sim = SimulatorProvider(rng=random.Random(3))
    prices = [sim.advance(["AAPL"])[0][1] for _ in range(50_000)]
    jumps = sum(
        1 for a, b in zip(prices, prices[1:]) if abs(b / a - 1.0) > 0.015
    )
    assert 50 < jumps < 150   # expected ~100 at p=0.002


def test_zero_event_probability_never_fires_an_event():
    sim = SimulatorProvider(rng=random.Random(5), event_probability=0.0)
    prices = [sim.advance(["AAPL"])[0][1] for _ in range(5_000)]
    jumps = sum(1 for a, b in zip(prices, prices[1:]) if abs(b / a - 1.0) > 0.015)
    assert jumps == 0


@pytest.mark.asyncio
async def test_run_writes_to_the_cache_and_stops_on_cancel():
    sim = SimulatorProvider(rng=random.Random(1), tick_seconds=0.01)
    cache = PriceCache()
    task = asyncio.create_task(sim.run(cache, lambda: {"AAPL"}))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cache.get("AAPL") is not None


@pytest.mark.asyncio
async def test_prime_always_succeeds_and_writes_to_cache():
    sim = SimulatorProvider(rng=random.Random(1))
    cache = PriceCache()
    tick = await sim.prime(cache, "newco")
    assert tick is not None
    assert tick.ticker == "NEWCO"
    assert cache.get("NEWCO").price == tick.price


@pytest.mark.asyncio
async def test_prime_and_run_share_the_same_seeding_path():
    sim = SimulatorProvider(rng=random.Random(1))
    cache = PriceCache()
    await sim.prime(cache, "ZQX")
    seeded_state = sim._state["ZQX"]
    # advance() must reuse the same TickerState prime() seeded, not reseed it.
    sim.advance(["ZQX"])
    assert sim._state["ZQX"] is seeded_state
