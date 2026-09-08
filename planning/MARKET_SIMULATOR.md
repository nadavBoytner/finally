# Market Simulator Design

Design for `backend/app/market_data/simulator.py`: the default price source (`PLAN.md` §5-6), used whenever `MASSIVE_API_KEY` is unset. Implements the `MarketDataProvider` interface from [`MARKET_INTERFACE.md`](./MARKET_INTERFACE.md).

## 1. Model: Geometric Brownian Motion

Each ticker's price evolves as discrete-time GBM:

```
S(t+dt) = S(t) * exp( (mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z )
```

- `S(t)` — current price
- `mu` — annualized drift (expected return)
- `sigma` — annualized volatility
- `dt` — time step, in years (tick interval / trading-year seconds)
- `Z` — draw from a standard normal distribution, one per ticker per tick

This is the standard log-normal stock-price model: prices stay positive, moves compound multiplicatively, and volatility scales with `sqrt(dt)` — the same math is used by both trades a course TA and any options-pricing textbook, so it is well-covered ground for a capstone.

`dt` is derived from the tick interval: at a 500ms tick, `dt = 0.5 / (252 * 6.5 * 3600)` (500ms as a fraction of a 252-trading-day, 6.5-hour year). Using a real trading-year denominator keeps `sigma` values comparable to real-world annualized volatility figures a student might look up (e.g. "AAPL's annualized vol is ~30%" maps directly to `sigma=0.30`).

## 2. Per-ticker state

```python
# backend/app/market_data/simulator.py
from dataclasses import dataclass

@dataclass
class TickerState:
    price: float
    session_open: float   # price at simulator start, for change_pct baseline
    mu: float              # annualized drift
    sigma: float           # annualized volatility
```

State lives in a plain `dict[str, TickerState]` inside the provider instance — no persistence needed; a restart just reseeds (acceptable since the simulator is explicitly not meant to be a source of historical truth — `PLAN.md` §10 already treats all chart data as ephemeral/SSE-accumulated).

## 3. Seed data for the 10 default tickers

Realistic starting prices and plausible drift/volatility, loosely calibrated to each name's real character (megacap tech lower vol, TSLA/NVDA higher vol):

| Ticker | Seed price | `mu` (annual) | `sigma` (annual) |
|---|---|---|---|
| AAPL | 190.00 | 0.08 | 0.25 |
| GOOGL | 175.00 | 0.10 | 0.28 |
| MSFT | 420.00 | 0.09 | 0.24 |
| AMZN | 185.00 | 0.10 | 0.30 |
| TSLA | 250.00 | 0.05 | 0.55 |
| NVDA | 120.00 | 0.15 | 0.50 |
| META | 500.00 | 0.10 | 0.35 |
| JPM | 210.00 | 0.06 | 0.20 |
| V | 280.00 | 0.07 | 0.18 |
| NFLX | 700.00 | 0.09 | 0.32 |

These are illustrative, not sourced live — the simulator's whole point is that it doesn't depend on real market data. Seed them as a module-level constant `DEFAULT_SEEDS: dict[str, TickerState]`.

## 4. Session-start baseline for `change_pct`

`TickerState.session_open` is set once, when a ticker first enters simulation (backend startup for the defaults; first `prime()`/`run()` sighting for anything added later), and never changes for the process lifetime. `change_pct = (price - session_open) / session_open * 100`. This is a deliberate simplification versus a real "previous close": the simulator has no concept of trading days, so "since the backend started" is the only baseline it can honestly offer. Document this in-app if it's ever surfaced to the user as "daily change" (it resets on every container restart).

## 5. New/unlisted tickers: deterministic hash seeding

Per `PLAN.md` §6, any ticker not in `DEFAULT_SEEDS` still needs a starting price, and it must be the *same* price every time that symbol is first seen (so re-adding a removed ticker, or two students on the same symbol, behave predictably):

```python
import hashlib

def derive_seed(ticker: str) -> TickerState:
    h = int(hashlib.sha256(ticker.upper().encode()).hexdigest(), 16)
    price = 20.0 + (h % 48000) / 100.0   # deterministic, spread across $20-$500
    # drift/vol also hash-derived but in a tighter, sane band
    mu = 0.04 + ((h >> 16) % 1200) / 10000.0     # ~0.04-0.16
    sigma = 0.20 + ((h >> 32) % 3000) / 10000.0  # ~0.20-0.50
    return TickerState(price=price, session_open=price, mu=mu, sigma=sigma)
```

`sha256` over the uppercased ticker (uppercasing happens once, centrally, per `MARKET_INTERFACE.md` §1 — the simulator never sees mixed case) gives a stable, well-distributed, dependency-free hash — no need for a seed table or external randomness source. Different bit-slices of the same hash drive price/drift/vol so the three don't move in lockstep across tickers.

## 6. Tick loop and correlation

```python
import asyncio
import random

TICK_SECONDS = 0.5
TRADING_YEAR_SECONDS = 252 * 6.5 * 3600
EVENT_PROBABILITY = 0.002   # ~1 event per ~1000 ticks per ticker, i.e. rare

class SimulatorProvider(MarketDataProvider):
    def __init__(self, rng: random.Random | None = None):
        self._rng = rng or random.Random()
        self._state: dict[str, TickerState] = dict(DEFAULT_SEEDS)  # copy

    def _ensure(self, ticker: str) -> TickerState:
        if ticker not in self._state:
            self._state[ticker] = derive_seed(ticker)
        return self._state[ticker]

    async def run(self, cache, tracked) -> None:
        while True:
            dt = TICK_SECONDS / TRADING_YEAR_SECONDS
            market_factor = self._rng.gauss(0, 1)  # shared across all tickers this tick
            for ticker in tracked():
                state = self._ensure(ticker)
                z = 0.6 * self._rng.gauss(0, 1) + 0.4 * market_factor
                drift = (state.mu - 0.5 * state.sigma ** 2) * dt
                shock = state.sigma * (dt ** 0.5) * z
                state.price *= 2.718281828 ** (drift + shock)
                if self._rng.random() < EVENT_PROBABILITY:
                    state.price *= 1 + self._rng.choice([-1, 1]) * self._rng.uniform(0.02, 0.05)
                change_pct = (state.price - state.session_open) / state.session_open * 100
                await cache.update(ticker, state.price, change_pct)
            await asyncio.sleep(TICK_SECONDS)

    async def prime(self, cache, ticker: str):
        state = self._ensure(ticker)
        change_pct = (state.price - state.session_open) / state.session_open * 100
        return await cache.update(ticker, state.price, change_pct)
```

Notes on the pieces:

- **Correlation** (`PLAN.md` §6: "a small shared market-wide random factor is added to every ticker's move"): one `market_factor` draw per tick, blended 40/60 into each ticker's own noise (`z`). This gives tickers a mild tendency to move together without a sector/grouping table — exactly the simplification `PLAN.md` calls for.
- **Events**: independent per ticker per tick, `EVENT_PROBABILITY = 0.002` → at 2 ticks/sec that's roughly one surprise 2-5% jump per ticker every ~8 minutes, occasional enough to be "drama" (`PLAN.md` §6) without dominating the normal drift.
- **No numpy dependency**: `random.Random.gauss` and pure-Python math are sufficient at this scale (≤ a few dozen tracked tickers, 2 Hz) — pulling in numpy for this would be the overengineering the project's style guide warns against.
- `_ensure` is the single seeding path used by both `run` (a ticker newly appearing in `tracked()`) and `prime` (an on-demand priming call from a trade) — one code path, no divergence between the two.

## 7. Testing

- **Determinism**: construct `SimulatorProvider(rng=random.Random(42))` and assert a fixed sequence of prices for a golden-file comparison — makes the GBM step itself unit-testable without timing dependencies (call the per-tick math as a small pure function extracted from `run`, e.g. `_step(state, z) -> float`, rather than only testing through the `asyncio.sleep` loop).
- **Statistical sanity**: run many ticks with a large fixed seed and assert price stays positive, and that log-returns have roughly the expected mean/stdev implied by `mu`/`sigma` (loose tolerance — this is a sanity check, not a numerics test).
- **Hash seeding**: `derive_seed("ZZZZ")` called twice returns identical values; seed price is always within `[20, 500]`.
- **Event rarity**: with a fixed seed and enough ticks, an event fires roughly `EVENT_PROBABILITY` of the time (statistical, generous tolerance).
