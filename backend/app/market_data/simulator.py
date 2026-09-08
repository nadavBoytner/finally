from __future__ import annotations

import asyncio
import hashlib
import math
import random
from dataclasses import dataclass

from .cache import PriceCache, Row
from .interface import MarketDataProvider, Tracked
from .models import PriceTick

TICK_SECONDS = 0.5
TRADING_YEAR_SECONDS = 252 * 6.5 * 3600

MARKET_BETA = 0.55        # each ticker's loading on the shared market factor
EVENT_PROBABILITY = 0.002  # per ticker per tick
EVENT_MIN_PCT = 0.02
EVENT_MAX_PCT = 0.05

SEED_PRICE_MIN = 20.0
SEED_PRICE_MAX = 500.0


@dataclass
class TickerState:
    price: float
    session_open: float  # price at first sighting — the change_pct baseline
    mu: float            # annualized drift
    sigma: float          # annualized volatility


# (seed price, mu, sigma) — immutable tuples, not TickerState objects, so
# separate SimulatorProvider instances never share mutable state.
DEFAULT_SEEDS: dict[str, tuple[float, float, float]] = {
    "AAPL":  (190.00, 0.08, 0.25),
    "GOOGL": (175.00, 0.10, 0.28),
    "MSFT":  (420.00, 0.09, 0.24),
    "AMZN":  (185.00, 0.10, 0.30),
    "TSLA":  (250.00, 0.05, 0.55),
    "NVDA":  (120.00, 0.15, 0.50),
    "META":  (500.00, 0.10, 0.35),
    "JPM":   (210.00, 0.06, 0.20),
    "V":     (280.00, 0.07, 0.18),
    "NFLX":  (700.00, 0.09, 0.32),
}


def _state(price: float, mu: float, sigma: float) -> TickerState:
    return TickerState(price=price, session_open=price, mu=mu, sigma=sigma)


def derive_seed(ticker: str) -> TickerState:
    """Deterministic pseudo-seed for a ticker with no entry in DEFAULT_SEEDS."""
    digest = hashlib.sha256(ticker.upper().encode()).hexdigest()
    h = int(digest, 16)
    span = int((SEED_PRICE_MAX - SEED_PRICE_MIN) * 100)   # cents across $20-$500
    price = SEED_PRICE_MIN + (h % span) / 100.0
    mu = 0.04 + ((h >> 16) % 1200) / 10000.0              # ~0.04 - 0.16
    sigma = 0.20 + ((h >> 32) % 3000) / 10000.0           # ~0.20 - 0.50
    return _state(price, mu, sigma)


def gbm_step(state: TickerState, z: float, dt: float) -> float:
    """One GBM step. Pure: returns the new price, mutates nothing."""
    drift = (state.mu - 0.5 * state.sigma ** 2) * dt
    shock = state.sigma * math.sqrt(dt) * z
    return state.price * math.exp(drift + shock)


def session_change_pct(state: TickerState) -> float:
    return (state.price - state.session_open) / state.session_open * 100.0


class SimulatorProvider(MarketDataProvider):
    name = "simulator"

    def __init__(
        self,
        rng: random.Random | None = None,
        tick_seconds: float = TICK_SECONDS,
        event_probability: float = EVENT_PROBABILITY,
    ) -> None:
        self._rng = rng if rng is not None else random.Random()
        self._tick_seconds = tick_seconds
        self._event_probability = event_probability
        self._dt = tick_seconds / TRADING_YEAR_SECONDS
        # Build fresh TickerState objects; never share them with DEFAULT_SEEDS.
        self._state: dict[str, TickerState] = {
            ticker: _state(*seed) for ticker, seed in DEFAULT_SEEDS.items()
        }

    def _ensure(self, ticker: str) -> TickerState:
        """The single seeding path, used by both run() and prime()."""
        symbol = ticker.upper()
        if symbol not in self._state:
            self._state[symbol] = derive_seed(symbol)
        return self._state[symbol]

    def advance(self, tickers: list[str]) -> list[Row]:
        """Advance every ticker one tick. Synchronous and fully testable."""
        market = self._rng.gauss(0.0, 1.0)      # shared across all tickers
        idio_weight = math.sqrt(1.0 - MARKET_BETA ** 2)
        rows: list[Row] = []
        for ticker in tickers:
            state = self._ensure(ticker)
            z = MARKET_BETA * market + idio_weight * self._rng.gauss(0.0, 1.0)
            state.price = gbm_step(state, z, self._dt)
            if self._rng.random() < self._event_probability:
                sign = self._rng.choice((-1.0, 1.0))
                state.price *= 1.0 + sign * self._rng.uniform(EVENT_MIN_PCT, EVENT_MAX_PCT)
            rows.append((ticker.upper(), state.price, session_change_pct(state)))
        return rows

    async def run(self, cache: PriceCache, tracked: Tracked) -> None:
        while True:
            rows = self.advance(sorted(tracked()))
            if rows:
                await cache.update_many(rows)
            await asyncio.sleep(self._tick_seconds)

    async def prime(self, cache: PriceCache, ticker: str) -> PriceTick | None:
        """Always succeeds — a hash-derived seed exists for every symbol."""
        state = self._ensure(ticker)
        return await cache.update(ticker.upper(), state.price, session_change_pct(state))
