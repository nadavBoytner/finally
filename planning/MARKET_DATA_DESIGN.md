# Market Data Backend — Implementation Design

Implementation-ready design for FinAlly's market data layer: the unified provider interface, the GBM simulator, the Massive API provider, and everything that wires them into FastAPI.

**This document is self-contained.** An agent implementing `backend/app/market_data/` should be able to work from this file alone. The companion docs are background, not prerequisites:

- [`MASSIVE_API.md`](./MASSIVE_API.md) — vendor research: endpoints, plan tiers, response shapes, rate limits.
- [`MARKET_INTERFACE.md`](./MARKET_INTERFACE.md) — design rationale for the two-method provider interface and the cache.
- [`MARKET_SIMULATOR.md`](./MARKET_SIMULATOR.md) — rationale for the GBM model and the seed table.

Where the code here departs from a snippet in those docs, a footnote says so — see [§12](#12-corrections-to-the-earlier-design-sketches). Governing spec is [`PLAN.md`](./PLAN.md) §5–6, §8, §10.

---

## Table of contents

| § | Section |
|---|---|
| 1 | [Package layout](#1-package-layout) |
| 2 | [`models.py` — `PriceTick`](#2-modelspy--pricetick) |
| 3 | [`interface.py` — the provider contract](#3-interfacepy--the-provider-contract) |
| 4 | [`cache.py` — `PriceCache`](#4-cachepy--pricecache) |
| 5 | [`simulator.py` — the GBM simulator](#5-simulatorpy--the-gbm-simulator) |
| 6 | [`massive_provider.py` — the Massive provider](#6-massive_providerpy--the-massive-provider) |
| 7 | [`tracking.py`, `factory.py`, and the lifespan](#7-trackingpy-factorypy-and-the-lifespan) |
| 8 | [The SSE endpoint](#8-the-sse-endpoint) |
| 9 | [`prime()` on the trade path](#9-prime-on-the-trade-path) |
| 10 | [Configuration and dependencies](#10-configuration-and-dependencies) |
| 11 | [Testing](#11-testing) |
| 12 | [Corrections to the earlier design sketches](#12-corrections-to-the-earlier-design-sketches) |
| 13 | [Deliberate simplifications](#13-deliberate-simplifications) |

---

## 1. Package layout

```
backend/app/
├── market_data/
│   ├── __init__.py            public exports
│   ├── models.py              PriceTick + timestamp helper
│   ├── interface.py           MarketDataProvider ABC
│   ├── cache.py               PriceCache (the single source of price truth)
│   ├── simulator.py           SimulatorProvider — GBM, default
│   ├── massive_provider.py    MassiveProvider — REST polling, optional
│   ├── tracking.py            TrackedTickers: watchlist ∪ positions, TTL-memoized
│   └── factory.py             create_provider() — the one MASSIVE_API_KEY branch
├── api/
│   └── stream.py              GET /api/stream/prices
└── main.py                    lifespan wiring
```

The dependency graph is strictly one-directional, which is what keeps the rest of the backend provider-agnostic:

```
models.py  ←  interface.py  ←  simulator.py
    ↑             ↑         ←  massive_provider.py
 cache.py  ───────┘                    ↑
                              factory.py
                                       ↑
    tracking.py  ────────────→   main.py (lifespan)  ────→  api/stream.py
                                       ↓
                              api/portfolio.py (prime on trade)
```

Nothing outside `market_data/` imports `simulator` or `massive_provider` except `factory.py` and the one fallback line in the lifespan. Routes touch only `PriceCache` and the abstract provider.

`__init__.py` re-exports the public surface:

```python
# backend/app/market_data/__init__.py
from .cache import PriceCache
from .factory import create_provider
from .interface import MarketDataProvider, Tracked
from .models import PriceTick, now_iso
from .tracking import TrackedTickers

__all__ = [
    "MarketDataProvider",
    "PriceCache",
    "PriceTick",
    "Tracked",
    "TrackedTickers",
    "create_provider",
    "now_iso",
]
```

---

## 2. `models.py` — `PriceTick`

```python
# backend/app/market_data/models.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def now_iso() -> str:
    """UTC timestamp, ISO 8601 with a Z suffix and millisecond precision."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True, slots=True)
class PriceTick:
    ticker: str        # always uppercase
    price: float       # latest known price
    prev_price: float  # the price immediately before this update
    change_pct: float  # percent change vs. the day baseline (see below)
    timestamp: str     # ISO 8601 UTC

    @property
    def direction(self) -> str:
        """Flash direction for the frontend: 'up', 'down', or 'flat'."""
        if self.price > self.prev_price:
            return "up"
        if self.price < self.prev_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Wire format for the SSE payload (§8). camelCase for the TS client."""
        return {
            "ticker": self.ticker,
            "price": round(self.price, 4),
            "prevPrice": round(self.prev_price, 4),
            "changePct": round(self.change_pct, 4),
            "direction": self.direction,
            "timestamp": self.timestamp,
        }
```

### Why two different "previous" values

`prev_price` and `change_pct` answer different questions and cannot share a baseline:

| Field | Baseline | Consumer |
|---|---|---|
| `prev_price` | the immediately preceding tick | the price-flash animation (`PLAN.md` §2, §10) — needs to know whether *this* update went up or down |
| `change_pct` | the day baseline | the watchlist's "daily change %" column — needs a stable reference that does not reset every 500ms |

A single previous-price field cannot serve both: flashing against the day baseline would leave a ticker stuck green all session, and computing a daily change from the last tick would show ±0.01% forever.

The day baseline differs by provider, and this asymmetry is deliberate ([§13](#13-deliberate-simplifications)):

- **Simulator** — the price at process start (`TickerState.session_open`). The simulator has no concept of trading days, so "since the backend started" is the only baseline it can honestly offer.
- **Massive** — `todaysChangePerc`, which the API computes from the previous trading day's close (`MASSIVE_API.md` §4.1).

`frozen=True` matters: ticks are handed to SSE generators and held in `last_sent` maps. Immutability means a later cache write can never mutate a tick a generator is midway through serializing.

---

## 3. `interface.py` — the provider contract

```python
# backend/app/market_data/interface.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable

from .models import PriceTick

if TYPE_CHECKING:  # avoid a circular import at runtime
    from .cache import PriceCache

Tracked = Callable[[], set[str]]
"""Returns the set of tickers that should currently be priced:
watchlist ∪ open positions, uppercased. See tracking.py."""


class MarketDataProvider(ABC):
    name: str = "provider"  # for logs and /api/health

    @abstractmethod
    async def run(self, cache: "PriceCache", tracked: Tracked) -> None:
        """Run forever. Each cycle, call tracked() for the current ticker set,
        obtain prices, and write them into the cache. Must not exit on a
        transient error. Cancelled at app shutdown."""

    @abstractmethod
    async def prime(self, cache: "PriceCache", ticker: str) -> PriceTick | None:
        """Fetch one ticker's price immediately, outside the run() cycle, and
        write it into the cache. Returns None when no price is obtainable."""

    async def startup_check(self) -> None:
        """Optional fail-fast probe, called once before run() is scheduled.
        Default: no-op. Raise to signal the provider is unusable."""
        return None
```

### Why two methods

The backend has two genuinely different access patterns, and collapsing them into one would break a real user flow:

- **`run`** is the continuous loop that feeds the SSE stream. Scheduled once at startup as an `asyncio` task, it is the only thing that keeps the cache warm.
- **`prime`** is on-demand acquisition for a ticker the cache does not have yet. Without it, buying a ticker that is not on the watchlist has no price to trade at, and the user would have to add it to the watchlist and wait up to a full poll interval before trading. `prime` closes that gap synchronously — see [§9](#9-prime-on-the-trade-path).

`startup_check` is defined on the base class rather than only on `MassiveProvider` so the lifespan can call it unconditionally without an `isinstance` check.

### Why `tracked` is a callable, not a list

`tracked()` is a callback into the backend's own state, not something a provider owns or caches. Passing a callable rather than a snapshot means a provider always observes the current set — a ticker added to the watchlist at 10:00:01 is priced on the 10:00:01.5 tick, with no restart, no reconfiguration, and no subscribe/unsubscribe protocol.

It also makes the scope impossible for a provider to get wrong. `PLAN.md` §6 requires pricing the **watchlist ∪ open positions**, so a holding removed from the watchlist still prices correctly. Because that union is computed in one place ([§7](#7-trackingpy-factorypy-and-the-lifespan)), neither provider can independently drift to "just the watchlist".

---

## 4. `cache.py` — `PriceCache`

```python
# backend/app/market_data/cache.py
from __future__ import annotations

import asyncio
from collections.abc import Iterable

from .models import PriceTick, now_iso

Row = tuple[str, float, float]  # (ticker, price, change_pct)


class PriceCache:
    """In-memory latest-price store. One instance per process, on app.state."""

    def __init__(self) -> None:
        self._prices: dict[str, PriceTick] = {}
        self._lock = asyncio.Lock()

    def _apply(self, ticker: str, price: float, change_pct: float, ts: str) -> PriceTick:
        symbol = ticker.upper()
        previous = self._prices.get(symbol)
        tick = PriceTick(
            ticker=symbol,
            price=float(price),
            prev_price=previous.price if previous is not None else float(price),
            change_pct=float(change_pct),
            timestamp=ts,
        )
        self._prices[symbol] = tick
        return tick

    async def update(self, ticker: str, price: float, change_pct: float) -> PriceTick:
        async with self._lock:
            return self._apply(ticker, price, change_pct, now_iso())

    async def update_many(self, rows: Iterable[Row]) -> list[PriceTick]:
        """Apply a whole tick as one batch. Every tick in the batch shares a
        timestamp, and no reader can observe the batch half-applied."""
        ts = now_iso()
        async with self._lock:
            return [self._apply(t, p, c, ts) for t, p, c in rows]

    def get(self, ticker: str) -> PriceTick | None:
        return self._prices.get(ticker.upper())

    def snapshot(self) -> dict[str, PriceTick]:
        return dict(self._prices)
```

### On the first tick, `prev_price == price`

A ticker's first-ever update has no predecessor, so `prev_price` is set to `price` and `direction` is `"flat"`. The frontend therefore renders a newly added ticker without a spurious green or red flash on its first appearance.

### On atomicity and the lock

`get()` and `snapshot()` are deliberately synchronous and lock-free, which is safe here for a specific reason worth stating rather than assuming:

**There is no `await` inside any critical section.** `_apply` is pure CPU work, and `update_many`'s list comprehension never yields. Under asyncio's single-threaded scheduling, a coroutine can only be preempted at an `await`, so the entire batch lands between two scheduling points. A concurrent `snapshot()` therefore sees either all of a tick or none of it — never AAPL's new price beside MSFT's stale one.

The lock is not what provides that guarantee; the absence of `await` is. It is kept as an explicit, near-free marker of the invariant, so that a future edit adding an `await` inside the critical section fails loudly under contention rather than silently tearing batches. If `snapshot()` ever needs to become `async`, the lock is already there.

This is also why `update_many` exists rather than the simulator calling `update` in a loop: a 30-ticker tick becomes one lock acquisition and one timestamp instead of thirty of each, and the SSE stream sees a coherent market state.

`snapshot()` returns a shallow copy — the dict is new, but `PriceTick` is frozen, so callers cannot corrupt cache state through it.

---

## 5. `simulator.py` — the GBM simulator

The default provider, used whenever `MASSIVE_API_KEY` is unset (`PLAN.md` §5). No network, no external dependency, no failure mode.

### 5.1 The model

Each ticker follows discrete-time geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − ½σ²)·dt + σ·√dt·Z )
```

| Symbol | Meaning |
|---|---|
| `S(t)` | current price |
| `μ` | annualized drift (expected return) |
| `σ` | annualized volatility |
| `dt` | timestep in years — `TICK_SECONDS / TRADING_YEAR_SECONDS` |
| `Z` | a standard-normal draw, one per ticker per tick |

Prices stay strictly positive (the exponential never reaches zero) and moves compound multiplicatively, which is the standard log-normal stock model.

`dt` uses a real trading-year denominator — `252 × 6.5 × 3600` seconds — so `σ` is directly comparable to published annualized volatility figures. A student who reads "AAPL's annualized vol is about 30%" can set `sigma=0.30` and get behavior that matches.

### 5.2 State and seeds

```python
# backend/app/market_data/simulator.py
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
    sigma: float         # annualized volatility


# (seed price, mu, sigma) — immutable tuples, not TickerState objects. See §12.4.
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
```

Prices are illustrative, not live — the simulator's entire point is independence from real market data. Volatilities are loosely calibrated to each name's character: utilities-like megacaps (V, JPM) low, TSLA and NVDA high.

State is a plain `dict[str, TickerState]` on the provider instance. Nothing persists; a restart reseeds. That is acceptable because `PLAN.md` §10 already treats all chart data as ephemeral and SSE-accumulated — the simulator is explicitly not a source of historical truth.

### 5.3 Deterministic seeding for unlisted tickers

`PLAN.md` §6 accepts any 1–5 character alphanumeric symbol, so any ticker outside `DEFAULT_SEEDS` still needs a starting price — and it must be the *same* price every time that symbol is seen, so removing and re-adding a ticker does not teleport its price.

```python
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
```

SHA-256 over the uppercased symbol is stable across processes and Python versions (unlike `hash()`, which is salted per process), well distributed, and dependency-free. Three different bit-slices drive price, drift, and volatility so they do not move in lockstep across symbols.

Uppercasing happens centrally at the API boundary ([§9](#9-prime-on-the-trade-path)); the `.upper()` here is defence in depth so the hash can never fork on case.

### 5.4 The tick step

The GBM math is a module-level pure function, deliberately separate from the `async` loop so it can be unit-tested without timing or scheduling ([§11.2](#112-simulator)):

```python
def gbm_step(state: TickerState, z: float, dt: float) -> float:
    """One GBM step. Pure: returns the new price, mutates nothing."""
    drift = (state.mu - 0.5 * state.sigma ** 2) * dt
    shock = state.sigma * math.sqrt(dt) * z
    return state.price * math.exp(drift + shock)


def session_change_pct(state: TickerState) -> float:
    return (state.price - state.session_open) / state.session_open * 100.0
```

### 5.5 Correlation

`PLAN.md` §6 asks for "a small shared market-wide random factor added to every ticker's move each tick, so prices drift together without needing a sector/grouping table". One draw per tick, shared by every ticker:

```
z_i = MARKET_BETA · market + √(1 − MARKET_BETA²) · idio_i
```

The weights are **not** free parameters — they must sum in quadrature to 1, or `σ` stops meaning what it says. With `market` and `idio_i` both standard normal and independent:

```
Var(z_i) = MARKET_BETA² + (1 − MARKET_BETA²) = 1     ✓
Corr(z_i, z_j) = MARKET_BETA² = 0.3025               (i ≠ j)
```

So `MARKET_BETA = 0.55` gives unit-variance shocks — a ticker configured at `sigma=0.30` actually realizes 30% annualized volatility — and a mild ~0.30 pairwise correlation, which is the "drift together, but not in lockstep" behavior the plan asks for.

A naive linear blend such as `0.6·idio + 0.4·market` does *not* preserve variance: `Var = 0.36 + 0.16 = 0.52`, so every ticker realizes only `√0.52 ≈ 72%` of its configured volatility. See [§12.1](#12-corrections-to-the-earlier-design-sketches).

### 5.6 The provider

```python
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
```

Notes:

- **`advance` is where the work is; `run` is a thin timing shell.** Tests drive `advance` directly with a seeded `random.Random`, so the GBM behavior is verified with no `asyncio.sleep` anywhere.
- **`sorted(tracked())`** makes the RNG consumption order deterministic for a given ticker set, which is what makes seeded tests reproducible.
- **Events** fire independently per ticker per tick. At `p = 0.002` and 2 ticks/sec, that is roughly one 2–5% jump per ticker every ~8 minutes — enough for drama (`PLAN.md` §6) without swamping the drift. `event_probability` is a constructor argument rather than a bare constant read so tests can set it to `0.0` and measure the GBM diffusion in isolation: a 2–5% jump is enormous next to a ~1 cent 500ms move, so even at `p = 0.002` events contribute over a hundred times more variance than the diffusion term and would swamp any volatility measurement ([§11.2](#112-simulator)).
- **No numpy.** `random.Random.gauss` and `math` are ample for a few dozen tickers at 2 Hz; adding numpy for this would be pure weight.
- **`_ensure` is the only seeding path**, shared by `run` and `prime`, so a ticker first seen via a trade and one first seen via the watchlist get identical treatment.

### 5.7 Worked example

```python
>>> import random
>>> sim = SimulatorProvider(rng=random.Random(42))
>>> [(t, round(p, 5), round(c, 5)) for t, p, c in sim.advance(["AAPL", "TSLA"])]
[('AAPL', 189.99691, -0.00163), ('TSLA', 250.00597, 0.00239)]
```

Both moves are sub-cent, which is the right order of magnitude. With `dt = 0.5 / 5,896,800` years, `√dt ≈ 2.91e-4`, so a one-standard-deviation tick is `190 × 0.25 × √dt ≈ $0.014` for AAPL and `250 × 0.55 × √dt ≈ $0.040` for TSLA. The observed moves — AAPL −$0.003, TSLA +$0.006 — are a fraction of a standard deviation each.

Note the two names moved in *opposite* directions on this tick. That is expected: `MARKET_BETA` produces a ~0.30 pairwise correlation, a tendency rather than a constraint, so plenty of individual ticks diverge. Co-movement is visible over many ticks, not in any single one.

---

## 6. `massive_provider.py` — the Massive provider

Used when `MASSIVE_API_KEY` is set. This is a **stretch goal** (`PLAN.md` §5): it must never block startup or crash the app. Every failure path degrades to either the simulator (at startup) or a retry (at runtime).

### 6.1 What the API gives us

Per `MASSIVE_API.md` §4.1, one `get_snapshot_all` call returns every field a `PriceTick` needs, for any number of tickers:

```json
{
  "ticker": "AAPL",
  "todaysChangePerc": 0.82,
  "day":       { "o": 119.62, "c": 120.4229, "v": 28727868 },
  "prevDay":   { "o": 117.19, "c": 119.49 },
  "lastTrade": { "p": 120.47, "s": 236, "t": 1605195918306274000 }
}
```

| `PriceTick` field | Source |
|---|---|
| `ticker` | `ticker` |
| `price` | `lastTrade.p` — the most recent traded price; falls back to `day.c`, the running current-day bar close |
| `prev_price` | *not from the API* — `PriceCache` derives it from the preceding poll |
| `change_pct` | `todaysChangePerc`, which Massive computes against `prevDay.c` |
| `timestamp` | *not from the API* — the cache stamps ingest time |

The official client (`from massive import RESTClient`) exposes these as snake_case attributes: `snap.last_trade.price`, `snap.todays_change_percent`.

**Plan tier matters.** `MASSIVE_API.md` §3 is emphatic: the free tier has **no access to snapshot endpoints at all**. A free-tier key produces authorization errors, not slow data. Massive mode needs Starter ($29/mo) or above. §6.4 detects this at startup and falls back rather than serving a permanently blank watchlist.

### 6.2 Response parsing

```python
# backend/app/market_data/massive_provider.py
from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterator, Sequence

from .cache import PriceCache, Row
from .interface import MarketDataProvider, Tracked
from .models import PriceTick

logger = logging.getLogger(__name__)

DEFAULT_POLL_SECONDS = 15.0
MAX_TICKERS_PER_REQUEST = 100  # keeps the ?tickers= query string well under URL limits
MAX_BACKOFF_SECONDS = 120.0


class MassivePlanError(RuntimeError):
    """The key cannot reach snapshot data — bad key, or a free-tier plan."""


def _status_code(exc: BaseException) -> int | None:
    """Best-effort HTTP status extraction; the client wraps httpx errors."""
    for attr in ("status", "status_code"):
        code = getattr(exc, attr, None)
        if isinstance(code, int):
            return code
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    return code if isinstance(code, int) else None


def _retry_after(exc: BaseException) -> float | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or {}
    try:
        return float(headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None


def _chunks(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def parse_snapshot(snap: object) -> Row | None:
    """Map one snapshot entry to a cache row, or None if it carries no price."""
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    price = None
    last_trade = getattr(snap, "last_trade", None)
    if last_trade is not None:
        price = getattr(last_trade, "price", None)
    if not price:                                 # no trade yet today
        day = getattr(snap, "day", None)
        price = getattr(day, "close", None) if day is not None else None
    if not price:                                 # None or 0.0 — unusable either way
        return None

    change_pct = getattr(snap, "todays_change_percent", None) or 0.0
    return (str(ticker).upper(), float(price), float(change_pct))
```

Three parsing details that matter in practice:

- **`if not price` rather than `if price is None`.** `MASSIVE_API.md` §4.1 notes snapshot data resets at 3:30 AM ET and repopulates from ~4:00 AM; in that window fields can come back as `0.0`. A `$0.00` price would corrupt every portfolio valuation downstream, so zero is rejected exactly like a missing field.
- **`lastTrade.p` with a `day.c` fallback.** Thinly traded symbols may have no trade in the current session; the day bar close still gives a usable mark.
- **`getattr` throughout, no attribute chaining.** The client returns objects whose optional sub-objects are `None`, and a `snap.last_trade.price` chain would raise `AttributeError` inside the poll loop for one bad symbol and lose the whole batch.

**A missing ticker is not an error.** `MASSIVE_API.md` §5: an invalid or unlisted symbol simply does not appear in the response. It therefore never reaches `parse_snapshot`, keeps no stale cache entry, and shows in the UI as unavailable — exactly the behavior `PLAN.md` §6 specifies.

### 6.3 The provider

```python
class MassiveProvider(MarketDataProvider):
    name = "massive"

    def __init__(
        self,
        api_key: str,
        poll_seconds: float | None = None,
        client: object | None = None,
    ) -> None:
        if client is not None:
            self._client = client          # tests inject a fake; no network, no SDK
        else:
            from massive import RESTClient  # imported lazily: only needed in Massive mode
            self._client = RESTClient(api_key)
        self._poll_seconds = float(
            poll_seconds
            if poll_seconds is not None
            else os.environ.get("MASSIVE_POLL_SECONDS", DEFAULT_POLL_SECONDS)
        )

    async def startup_check(self) -> None:
        """One snapshot call before the app takes traffic. Distinguishes a
        usable key from a bad or free-tier one (MASSIVE_API.md §3, §5)."""
        try:
            await asyncio.to_thread(
                self._client.get_snapshot_ticker, "stocks", ticker="AAPL"
            )
        except Exception as exc:
            raise MassivePlanError(
                "MASSIVE_API_KEY is set but snapshot data is unreachable — most "
                "likely a free-tier key. Snapshots require the Starter plan or "
                "above (see MASSIVE_API.md §3)."
            ) from exc

    async def _fetch(self, tickers: Sequence[str]) -> list[Row]:
        rows: list[Row] = []
        for chunk in _chunks(tickers, MAX_TICKERS_PER_REQUEST):
            snaps = await asyncio.to_thread(
                self._client.get_snapshot_all, "stocks", tickers=list(chunk)
            )
            for snap in snaps or ():
                row = parse_snapshot(snap)
                if row is not None:
                    rows.append(row)
        return rows

    async def run(self, cache: PriceCache, tracked: Tracked) -> None:
        backoff = 0.0
        while True:
            tickers = sorted(tracked())
            if not tickers:
                await asyncio.sleep(self._poll_seconds)
                continue

            try:
                rows = await self._fetch(tickers)
            except Exception as exc:
                if _status_code(exc) == 429:
                    wait = _retry_after(exc)
                    if wait is None:
                        backoff = min(
                            max(backoff * 2.0, self._poll_seconds), MAX_BACKOFF_SECONDS
                        )
                        wait = backoff
                    logger.warning("Massive rate-limited (429); backing off %.1fs", wait)
                    await asyncio.sleep(wait)
                    continue
                logger.warning("Massive poll failed; retrying next cycle", exc_info=True)
            else:
                backoff = 0.0
                if rows:
                    await cache.update_many(rows)

            await asyncio.sleep(self._poll_seconds)

    async def prime(self, cache: PriceCache, ticker: str) -> PriceTick | None:
        """Single-ticker snapshot. Returns None when Massive has no data."""
        symbol = ticker.upper()
        try:
            snap = await asyncio.to_thread(
                self._client.get_snapshot_ticker, "stocks", ticker=symbol
            )
        except Exception:
            logger.warning("Massive prime failed for %s", symbol, exc_info=True)
            return None

        row = parse_snapshot(snap) if snap is not None else None
        if row is None:
            return None
        _, price, change_pct = row
        return await cache.update(symbol, price, change_pct)
```

Design notes:

- **`asyncio.to_thread` on every client call.** `RESTClient` is synchronous and blocking. Calling it directly on the event loop would freeze *every* connected SSE stream for the duration of the HTTP round trip — a 300ms API call would stall every browser's price feed. Offloading to a thread keeps the loop free.
- **The lazy `from massive import RESTClient`.** Keeping it inside `__init__` means `factory.py` and `main.py` can import this module unconditionally without requiring the SDK to be importable in simulator mode, and tests can construct the provider with a fake client and never touch the real package.
- **429 gets its own branch.** `MASSIVE_API.md` §5 documents `429` with an optional `Retry-After`. Honoring the header when present, and otherwise doubling from the poll interval up to a 2-minute ceiling, prevents a rate-limited client from hammering the API on every cycle. `backoff` resets to zero after any success, so one bad minute does not leave the stream permanently slow. Non-429 errors just log and retry on the normal cadence — a transient network blip should not throttle the feed.
- **A failed poll never kills the loop.** The `try` wraps only `_fetch`; the `while True` continues regardless. Massive is optional, so a broken data source must degrade to stale prices, never to a dead app.
- **Chunking at 100 tickers.** The watchlist ∪ positions set will realistically be far smaller, but `?tickers=` is a query string, and unbounded growth is the kind of thing that fails only in the demo.
- **`poll_seconds` is configuration, not auto-detection.** Defaulting to 15s per `PLAN.md` §6 and overridable via `MASSIVE_POLL_SECONDS`. Detecting the plan tier would require parsing undocumented account metadata; a documented env var the student sets once is simpler and honest. `MASSIVE_API.md` §6 notes 15s suits Starter/Developer (15-minute-delayed data does not get fresher when polled faster) while Advanced can justify 2–5s.

### 6.4 Startup fallback

`startup_check` raising must **not** stop the app. `PLAN.md` §5 makes the simulator the required data source and Massive the optional extra, so an unusable key degrades to the simulator with a loud log line. The swap happens in the lifespan ([§7.3](#73-lifespan-wiring)) — `create_provider()` cannot do it, because the check has to run after construction.

---

## 7. `tracking.py`, `factory.py`, and the lifespan

### 7.1 `tracked()` — the watchlist ∪ positions union

```python
# backend/app/market_data/tracking.py
from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable

TRACKED_SQL = """
    SELECT ticker FROM watchlist  WHERE user_id = ?
    UNION
    SELECT ticker FROM positions  WHERE user_id = ? AND quantity != 0
"""


class TrackedTickers:
    """Callable returning watchlist ∪ open positions, memoized for `ttl` seconds.

    Satisfies the Tracked protocol in interface.py. Memoized because run() calls
    it at 2 Hz and the underlying set changes only on a trade or a watchlist edit.
    """

    def __init__(
        self,
        connect: Callable[[], sqlite3.Connection],
        user_id: str = "default",
        ttl: float = 1.0,
    ) -> None:
        self._connect = connect
        self._user_id = user_id
        self._ttl = ttl
        self._cached: set[str] = set()
        self._fetched_at: float | None = None

    def __call__(self) -> set[str]:
        now = time.monotonic()
        if self._fetched_at is not None and now - self._fetched_at < self._ttl:
            return set(self._cached)
        with self._connect() as conn:
            rows = conn.execute(TRACKED_SQL, (self._user_id, self._user_id)).fetchall()
        self._cached = {str(row[0]).upper() for row in rows}
        self._fetched_at = now
        return set(self._cached)

    def invalidate(self) -> None:
        """Call after any watchlist or position change so the next tick sees it."""
        self._fetched_at = None
```

- **The union is the whole point.** `PLAN.md` §6 requires the cache to cover watchlist **∪** open positions, so a holding removed from the watchlist still prices — and stays visible in the positions table (`PLAN.md` §10) and chartable when selected. `quantity != 0` keeps fully-sold positions from being priced forever.
- **Why memoize.** At `TICK_SECONDS = 0.5` this is called twice a second for the life of the process. The query is trivially fast on a table of ~10 rows, but running it 172,800 times a day to get the same answer is noise in the logs and in the profile. A 1s TTL caps it at ~1 query/sec.
- **Why `invalidate()` rather than a shorter TTL.** Without it, a ticker added to the watchlist could take up to a full second to start streaming, which is visible as a blank row. The watchlist and trade handlers call `invalidate()` after committing, so the very next tick includes the new symbol. The TTL is then only a backstop for changes made outside those handlers (someone editing the SQLite file by hand).
- **Returning a copy** (`set(self._cached)`) prevents a provider from mutating the memo. Cheap at this size, and it removes a whole class of aliasing bug.

### 7.2 `factory.py`

```python
# backend/app/market_data/factory.py
from __future__ import annotations

import os

from .interface import MarketDataProvider
from .simulator import SimulatorProvider


def create_provider() -> MarketDataProvider:
    """The one and only place the backend branches on MASSIVE_API_KEY."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not api_key:
        return SimulatorProvider()
    from .massive_provider import MassiveProvider
    return MassiveProvider(api_key=api_key)
```

`.strip()` matters: `MASSIVE_API_KEY=` in a `.env` yields `""`, and a key pasted with a trailing space or newline would otherwise be sent to the API verbatim and rejected. Both cases resolve to "use the simulator" / "use the real key", which is what the user meant.

### 7.3 Lifespan wiring

```python
# backend/app/main.py
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import portfolio, stream, watchlist
from .db import connect, init_db
from .market_data import PriceCache, TrackedTickers, create_provider
from .market_data.massive_provider import MassivePlanError
from .market_data.simulator import SimulatorProvider

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                                    # lazy schema + seed (PLAN.md §7)

    cache = PriceCache()
    tracked = TrackedTickers(connect)
    provider = create_provider()

    # 1. Fail fast, then degrade — never refuse to start (PLAN.md §5).
    try:
        await provider.startup_check()
    except MassivePlanError as exc:
        logger.error("%s Falling back to the market simulator.", exc)
        provider = SimulatorProvider()
    except Exception:
        logger.exception("Provider startup check failed; falling back to the simulator.")
        provider = SimulatorProvider()

    # 2. Warm the cache before accepting traffic, so the first SSE frame has data.
    for ticker in sorted(tracked()):
        await provider.prime(cache, ticker)

    # 3. Start the background loop.
    task = asyncio.create_task(provider.run(cache, tracked), name="market-data")

    app.state.price_cache = cache
    app.state.provider = provider
    app.state.tracked = tracked

    logger.info("Market data provider: %s", provider.name)
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)
app.include_router(stream.router)
app.include_router(portfolio.router)
app.include_router(watchlist.router)
```

Two names here come from outside this document: `init_db()` and `connect()` belong to the database layer (`PLAN.md` §7 — lazy schema creation and seeding on first use). This design needs only that `connect()` returns a `sqlite3.Connection` and that `init_db()` has run before `tracked()` is first called; everything else about that layer is someone else's contract.

Four things this sequence gets right:

1. **The check runs after construction, so the fallback has somewhere to happen.** `create_provider()` returns before any network call, so it cannot know the key is bad. Putting `startup_check` in the lifespan is what makes `MARKET_INTERFACE.md`'s "fall back to `SimulatorProvider`" actually reachable ([§12.5](#12-corrections-to-the-earlier-design-sketches)).
2. **The bare `except Exception` alongside `MassivePlanError`.** A DNS failure or a timeout is not a plan error, but the response is the same: log it and use the simulator. Massive is optional; nothing about it should keep the app from starting.
3. **Priming before `run()` closes the cold-start gap.** The cache starts empty, and `run()` does not fill it until its first cycle completes — up to 15 seconds in Massive mode. Without this loop, every client connecting in that window gets an empty watchlist and a blank grid. Priming is cheap in simulator mode (no I/O at all) and one round trip per default ticker in Massive mode.
4. **Cancel-and-await on shutdown.** `task.cancel()` only requests cancellation; without awaiting it, uvicorn can tear the loop down mid-tick and log a "Task was destroyed but it is pending!" warning. Catching `CancelledError` makes shutdown clean and silent.

`app.state` is the handoff to the routes: `price_cache` for reads, `provider` for `prime()`, `tracked` for `invalidate()`. No route imports a concrete provider.

---

## 8. The SSE endpoint

`GET /api/stream/prices` (`PLAN.md` §8). One long-lived connection per browser tab, consumed by the native `EventSource` API.

### 8.1 Implementation

```python
# backend/app/api/stream.py
from __future__ import annotations

import asyncio
import json
import time

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

router = APIRouter()

PUSH_SECONDS = 0.5        # matches the simulator tick
HEARTBEAT_SECONDS = 15.0  # keeps idle proxies from reaping the connection
RETRY_MS = 3000           # EventSource reconnect delay hint

SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # tells nginx not to buffer the stream
}


async def price_events(request: Request):
    cache = request.app.state.price_cache
    last_sent: dict[str, str] = {}   # ticker -> last timestamp pushed to THIS client
    last_flush = time.monotonic()

    yield f"retry: {RETRY_MS}\n\n"

    while True:
        if await request.is_disconnected():
            break

        payload = []
        for tick in cache.snapshot().values():
            if last_sent.get(tick.ticker) == tick.timestamp:
                continue                       # unchanged since this client's last frame
            last_sent[tick.ticker] = tick.timestamp
            payload.append(tick.to_dict())

        now = time.monotonic()
        if payload:
            data = json.dumps(payload, separators=(",", ":"))
            yield f"event: prices\ndata: {data}\n\n"
            last_flush = now
        elif now - last_flush >= HEARTBEAT_SECONDS:
            yield ": heartbeat\n\n"
            last_flush = now

        await asyncio.sleep(PUSH_SECONDS)


@router.get("/api/stream/prices")
async def stream_prices(request: Request) -> StreamingResponse:
    return StreamingResponse(
        price_events(request),
        media_type="text/event-stream",
        headers=SSE_HEADERS,
    )
```

### 8.2 The wire format

```
retry: 3000

event: prices
data: [{"ticker":"AAPL","price":190.0074,"prevPrice":189.9912,"changePct":0.0039,"direction":"up","timestamp":"2026-09-08T14:22:01.503Z"}, ...]

event: prices
data: [{"ticker":"AAPL","price":189.9981,"prevPrice":190.0074,"changePct":-0.0010,"direction":"down","timestamp":"2026-09-08T14:22:02.004Z"}, ...]

: heartbeat

```

Every frame is terminated by a blank line — that is what SSE uses to delimit events, and omitting it means the browser buffers forever and fires no handler.

### 8.3 Design decisions

- **One frame carrying a JSON array, not one event per ticker.** A 30-ticker watchlist would otherwise mean 30 separate events every 500ms — 60 handler invocations per second, each triggering React state updates. Batching gives the frontend one array to reduce over, and the whole tick lands in a single render.
- **`event: prices` is named**, so the client uses `addEventListener("prices", …)`. That leaves the default `message` channel free for a future event type (portfolio updates, alerts) without breaking existing handlers.
- **Per-client change filtering.** Pushing the full snapshot every cycle would be pure waste in Massive mode, where prices refresh every 15s but the loop runs every 500ms — 30 identical frames for every real update. Comparing `tick.timestamp` against what this client last received sends each price exactly once. `last_sent` is per-connection state, so a client reconnecting gets the full snapshot immediately.
- **Heartbeats.** A comment line (`: heartbeat`) is valid SSE that the browser ignores entirely. Proxies and load balancers close connections that are idle for 30–60s; a 15s heartbeat during a quiet market keeps the connection alive. The `last_flush` timer resets on any real frame, so heartbeats only appear when there is genuinely nothing to send.
- **`retry: 3000`** tells `EventSource` to wait 3s before reconnecting rather than using its browser-default backoff, giving the connection-status dot (`PLAN.md` §2) predictable timing.
- **`X-Accel-Buffering: no`.** nginx buffers proxied responses by default, which for SSE means the browser receives nothing until the buffer fills — the stream appears completely dead. Harmless when FastAPI serves directly, essential the moment anything is put in front of it.
- **`request.is_disconnected()` for cleanup.** When a tab closes, the generator would otherwise keep looping forever and leak a coroutine per closed tab. Checking each cycle exits promptly. The `async for` in Starlette also throws `CancelledError` into the generator on client disconnect; both paths terminate the loop, and since there is nothing to release, no `finally` is needed.

### 8.4 Frontend consumption

```ts
// frontend/src/lib/priceStream.ts
export type PriceTick = {
  ticker: string;
  price: number;
  prevPrice: number;
  changePct: number;
  direction: "up" | "down" | "flat";
  timestamp: string;
};

export function connectPriceStream(
  onTicks: (ticks: PriceTick[]) => void,
  onStatus: (s: "connected" | "reconnecting" | "disconnected") => void,
): () => void {
  const es = new EventSource("/api/stream/prices");

  es.addEventListener("prices", (e) => onTicks(JSON.parse((e as MessageEvent).data)));
  es.onopen = () => onStatus("connected");
  es.onerror = () =>
    onStatus(es.readyState === EventSource.CONNECTING ? "reconnecting" : "disconnected");

  return () => es.close();  // call from the useEffect cleanup
}
```

`EventSource` reconnects on its own — the `onerror` handler only drives the status dot, and must not call `es.close()` or it will disable the built-in retry. The returned closer is for React unmount, not for error handling.

Sparklines and the main chart accumulate from this stream (`PLAN.md` §10): there is no historical REST endpoint, so charts start empty on page load and fill in over time. That is intentional — it keeps price data to a single source.

---

## 9. `prime()` on the trade path

A user can buy a ticker that is not on the watchlist, so its price may not be in the cache. `prime()` is what makes that trade possible without a watchlist round trip.

### 9.1 Normalization — one place, one regex

```python
# backend/app/api/_tickers.py
from __future__ import annotations

import re

from fastapi import HTTPException

TICKER_RE = re.compile(r"^[A-Z0-9]{1,5}$")


def normalize_ticker(raw: str) -> str:
    """Uppercase and validate. The single entry point for every ticker string:
    watchlist add, manual trade, and LLM-issued action alike."""
    symbol = (raw or "").strip().upper()
    if not TICKER_RE.match(symbol):
        raise HTTPException(status_code=400, detail=f"invalid ticker: {raw!r}")
    return symbol
```

`PLAN.md` §6 accepts any 1–5 character alphanumeric symbol with no validation against a real ticker list. Routing every path through this one function is what makes `aapl` and `AAPL` the same ticker in the cache, the watchlist table, the positions table, and the provider — the case-normalization requirement in `MARKET_INTERFACE.md` §1.

### 9.2 Price resolution

```python
# backend/app/api/portfolio.py  (excerpt)
from fastapi import APIRouter, HTTPException, Request

from ._tickers import normalize_ticker

router = APIRouter()


async def resolve_price(request: Request, symbol: str) -> float:
    """Latest price for an already-normalized symbol, priming on a cache miss."""
    cache = request.app.state.price_cache
    tick = cache.get(symbol)
    if tick is None:
        tick = await request.app.state.provider.prime(cache, symbol)
    if tick is None:
        raise HTTPException(
            status_code=400, detail=f"no price available for {symbol}"
        )
    return tick.price


@router.post("/api/portfolio/trade")
async def execute_trade(request: Request, body: TradeRequest) -> TradeResponse:
    symbol = normalize_ticker(body.ticker)
    if body.quantity <= 0:
        raise HTTPException(status_code=400, detail="quantity must be positive")

    price = await resolve_price(request, symbol)
    result = apply_trade(symbol, body.side, body.quantity, price)  # portfolio module

    request.app.state.tracked.invalidate()  # a new position must price next tick
    return result
```

The flow, once:

```
normalize → cache.get() → hit?  → use it
                        → miss? → await provider.prime()
                                → tick?  → use it
                                → None?  → 400 "no price available for AAPL"
```

### 9.3 Notes

- **The same path serves manual and LLM trades.** `PLAN.md` §9 requires LLM-issued trades to go through the same validation as manual ones; the chat handler calls `normalize_ticker` + `resolve_price` too, so a failure surfaces as an error the LLM can relay to the user rather than a silent no-op.
- **`invalidate()` after the trade.** A buy on a non-watchlisted ticker creates a position that must be priced from the next tick onward. Without invalidation, the TTL could leave the new holding unpriced for up to a second, and the positions table would render a blank price cell.
- **The two providers differ here, and only here.** `SimulatorProvider.prime` can never return `None` — every symbol has a hash-derived seed. `MassiveProvider.prime` returns `None` for a symbol Massive has no data on. So the 400 is reachable only in Massive mode, which is the correct behavior for a genuinely unknown symbol against real market data. It is the one user-visible difference between the two modes, and worth stating in the error message rather than hiding.
- **`resolve_price` is also the right hook for portfolio valuation**, which reads `cache.get()` for every held ticker. Those are guaranteed present because `tracked()` includes open positions — priming there is a safety net, not a normal path.

---

## 10. Configuration and dependencies

### 10.1 Environment variables

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | *(empty)* | Empty or unset → simulator. Set → `MassiveProvider`, subject to the startup check. |
| `MASSIVE_POLL_SECONDS` | `15` | Massive poll interval. 15s suits Starter/Developer; Advanced can use 2–5s (`MASSIVE_API.md` §6). Ignored in simulator mode. |

Both belong in `.env.example` (`PLAN.md` §5):

```bash
# Optional: Massive (Polygon.io) API key for real market data.
# Leave empty to use the built-in simulator (recommended).
# NOTE: requires the Starter plan or above — the free tier cannot access
# snapshot endpoints at all, and will fall back to the simulator.
MASSIVE_API_KEY=

# Optional: Massive poll interval in seconds (default 15).
MASSIVE_POLL_SECONDS=15
```

`TICK_SECONDS` (0.5) is a module constant rather than an env var — `PLAN.md` §6 fixes the simulator cadence at ~500ms, and it is a constructor argument on `SimulatorProvider` so tests can override it without touching the environment.

### 10.2 Dependencies

```toml
# backend/pyproject.toml  (excerpt)
[project]
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.32",
    "massive>=0.1",       # Massive/Polygon REST client — imported only in Massive mode
]
```

Always installed (it keeps the lockfile reproducible and the Dockerfile single-path), but imported lazily inside `MassiveProvider.__init__`, so nothing in simulator mode ever touches it.

The simulator adds **no dependency at all** — `hashlib`, `math`, `random`, `asyncio`, and `dataclasses` are all stdlib.

### 10.3 No mock flag for market data

Unlike the LLM layer, which needs `LLM_MOCK=true` for deterministic E2E runs (`PLAN.md` §5, §9), market data needs no equivalent. The simulator *is* the deterministic, dependency-free, network-free default, and tests select it simply by not setting `MASSIVE_API_KEY`. Injecting a seeded `random.Random` makes it reproducible on demand.

---

## 11. Testing

Layout follows pytest convention inside the backend project:

```
backend/tests/
├── conftest.py
├── test_cache.py
├── test_simulator.py
├── test_massive_provider.py
├── test_tracking.py
└── test_stream.py
```

### 11.1 `PriceCache`

```python
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


async def test_update_many_shares_one_timestamp():
    cache = PriceCache()
    ticks = await cache.update_many([("AAPL", 190.0, 0.1), ("MSFT", 420.0, 0.2)])
    assert len({t.timestamp for t in ticks}) == 1


async def test_snapshot_is_an_isolated_copy():
    cache = PriceCache()
    await cache.update("AAPL", 190.0, 0.0)
    snap = cache.snapshot()
    await cache.update("AAPL", 191.0, 0.0)
    assert snap["AAPL"].price == 190.0    # frozen ticks, copied dict
```

### 11.2 Simulator

```python
import math
import random
import statistics

from app.market_data.simulator import (
    DEFAULT_SEEDS,
    MARKET_BETA,
    SimulatorProvider,
    TickerState,
    derive_seed,
    gbm_step,
)


def test_gbm_step_is_pure_and_deterministic():
    state = TickerState(price=100.0, session_open=100.0, mu=0.1, sigma=0.3)
    dt = 0.5 / (252 * 6.5 * 3600)
    first = gbm_step(state, z=1.0, dt=dt)
    assert gbm_step(state, z=1.0, dt=dt) == first  # no mutation
    assert state.price == 100.0
    assert first > 100.0                            # positive shock


def test_derive_seed_is_stable_and_in_range():
    a, b = derive_seed("ZZZZ"), derive_seed("zzzz")
    assert (a.price, a.mu, a.sigma) == (b.price, b.mu, b.sigma)
    for symbol in ("A", "PYPL", "XYZ12", "QQQ"):
        seed = derive_seed(symbol)
        assert 20.0 <= seed.price <= 500.0
        assert seed.price == seed.session_open


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
    the GBM term and the measurement would read ~5.0 instead of ~0.30. This
    test is about the diffusion; test_events_fire_at_roughly_the_configured_rate
    covers the overlay.
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
```

`test_instances_do_not_share_seed_state` is the regression test for [§12.4](#12-corrections-to-the-earlier-design-sketches) — without it, that bug reappears the moment someone "simplifies" the constructor back to `dict(DEFAULT_SEEDS)`.

### 11.3 Massive provider

No test touches the network. A fake client covers every path:

```python
import asyncio
from types import SimpleNamespace

import pytest

from app.market_data import PriceCache
from app.market_data.massive_provider import (
    MassivePlanError,
    MassiveProvider,
    parse_snapshot,
)

pytestmark = pytest.mark.asyncio


def snap(ticker, price=None, day_close=None, change=1.5):
    return SimpleNamespace(
        ticker=ticker,
        last_trade=SimpleNamespace(price=price) if price is not None else None,
        day=SimpleNamespace(close=day_close) if day_close is not None else None,
        todays_change_percent=change,
    )


class FakeClient:
    def __init__(self, snapshots=None, error=None):
        self.snapshots = snapshots or []
        self.error = error
        self.calls: list[list[str]] = []

    def get_snapshot_all(self, market, tickers):
        self.calls.append(list(tickers))
        if self.error:
            raise self.error
        return self.snapshots

    def get_snapshot_ticker(self, market, ticker):
        if self.error:
            raise self.error
        return next((s for s in self.snapshots if s.ticker == ticker), None)


class RateLimited(Exception):
    status_code = 429
    response = SimpleNamespace(headers={"Retry-After": "2"})


# --- parsing -----------------------------------------------------------------

def test_parse_prefers_last_trade_then_falls_back_to_day_close():
    assert parse_snapshot(snap("AAPL", price=120.47))[1] == 120.47
    assert parse_snapshot(snap("AAPL", day_close=120.42))[1] == 120.42


def test_parse_rejects_zero_and_missing_prices():
    # Pre-market snapshots can return 0.0; a $0 mark would corrupt valuations.
    assert parse_snapshot(snap("AAPL", price=0.0)) is None
    assert parse_snapshot(snap("AAPL")) is None


def test_parse_uppercases_the_ticker():
    assert parse_snapshot(snap("aapl", price=1.0))[0] == "AAPL"


# --- run ---------------------------------------------------------------------

async def test_run_requests_exactly_the_tracked_set():
    client = FakeClient([snap("AAPL", price=190.0), snap("MSFT", price=420.0)])
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)
    cache = PriceCache()

    task = asyncio.create_task(provider.run(cache, lambda: {"AAPL", "MSFT"}))
    await asyncio.sleep(0.05)
    task.cancel()

    assert client.calls[0] == ["AAPL", "MSFT"]   # sorted, exactly tracked()
    assert cache.get("AAPL").price == 190.0


async def test_run_survives_a_poll_failure():
    client = FakeClient(error=RuntimeError("boom"))
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: {"AAPL"}))
    await asyncio.sleep(0.05)
    still_running = not task.done()
    task.cancel()

    assert still_running          # a bad poll must never kill the loop
    assert len(client.calls) > 1  # and it must keep retrying


async def test_run_honors_retry_after_on_429():
    client = FakeClient(error=RateLimited())
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: {"AAPL"}))
    await asyncio.sleep(0.05)
    task.cancel()

    assert len(client.calls) == 1  # backed off 2s, so no second attempt yet


# --- startup_check / prime ---------------------------------------------------

async def test_startup_check_raises_plan_error_on_failure():
    provider = MassiveProvider("k", client=FakeClient(error=RuntimeError("403")))
    with pytest.raises(MassivePlanError):
        await provider.startup_check()


async def test_prime_returns_none_for_an_unknown_symbol():
    provider = MassiveProvider("k", client=FakeClient([]))
    assert await provider.prime(PriceCache(), "NOPE") is None
```

### 11.4 Tracking and the SSE endpoint

```python
def test_tracked_unions_watchlist_and_positions(db):
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','AAPL')")
    db.execute(
        "INSERT INTO positions (id, user_id, ticker, quantity, avg_cost)"
        " VALUES ('2','default','TSLA',5,250.0)"
    )
    db.commit()
    assert TrackedTickers(lambda: db)() == {"AAPL", "TSLA"}


def test_tracked_excludes_zero_quantity_positions(db):
    db.execute(
        "INSERT INTO positions (id, user_id, ticker, quantity, avg_cost)"
        " VALUES ('3','default','MSFT',0,420.0)"
    )
    db.commit()
    assert "MSFT" not in TrackedTickers(lambda: db)()


def test_invalidate_forces_a_refetch(db):
    tracked = TrackedTickers(lambda: db, ttl=60.0)
    tracked()
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('4','default','NVDA')")
    db.commit()
    assert "NVDA" not in tracked()   # still memoized
    tracked.invalidate()
    assert "NVDA" in tracked()
```

The SSE endpoint is tested against a cache alone — no provider needed, because the endpoint only ever reads `app.state.price_cache`.

**Drive the generator directly, not `TestClient`.** `client.stream(...)` plus `islice(response.iter_text(), n)` looks natural and *hangs*: the stream is infinite and, once the cache stops changing, produces nothing but a heartbeat every 15 seconds, so any read past the first frame blocks until the test times out. Pulling frames from `price_events` with `__anext__` and a timeout tests the same logic and terminates:

```python
import asyncio
import json
import re
from types import SimpleNamespace

import pytest

from app.api import stream
from app.market_data import PriceCache

pytestmark = pytest.mark.asyncio


def fake_request(cache, disconnected=lambda: False):
    async def is_disconnected():
        return disconnected()
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(price_cache=cache)),
        is_disconnected=is_disconnected,
    )


async def take(gen, n, timeout=5.0):
    return [await asyncio.wait_for(gen.__anext__(), timeout) for _ in range(n)]


def parse_frame(frame):
    return json.loads(re.match(r"event: prices\ndata: (.*)\n\n", frame).group(1))


@pytest.fixture
def fast_stream(monkeypatch):
    """Shrink the cadence so a heartbeat is observable within a test."""
    monkeypatch.setattr(stream, "PUSH_SECONDS", 0.01)
    monkeypatch.setattr(stream, "HEARTBEAT_SECONDS", 0.15)


async def test_stream_emits_retry_hint_then_a_prices_event(fast_stream):
    cache = PriceCache()
    await cache.update_many([("AAPL", 190.0, 0.5), ("MSFT", 420.0, -0.2)])

    gen = stream.price_events(fake_request(cache))
    first, second = await take(gen, 2)

    assert first == "retry: 3000\n\n"
    assert second.startswith("event: prices\ndata: ")
    assert second.endswith("\n\n")                       # frame delimiter
    payload = parse_frame(second)
    assert sorted(t["ticker"] for t in payload) == ["AAPL", "MSFT"]
    assert set(payload[0]) == {
        "ticker", "price", "prevPrice", "changePct", "direction", "timestamp",
    }
    await gen.aclose()


async def test_unchanged_prices_are_not_repushed(fast_stream):
    cache = PriceCache()
    await cache.update("AAPL", 190.0, 0.5)
    gen = stream.price_events(fake_request(cache))
    await take(gen, 2)                                   # retry + first payload

    (third,) = await take(gen, 1)
    assert third == ": heartbeat\n\n"                    # nothing changed

    await cache.update("AAPL", 191.0, 0.6)
    (fourth,) = await take(gen, 1)
    payload = parse_frame(fourth)
    assert [t["ticker"] for t in payload] == ["AAPL"]    # only the changed one
    assert payload[0]["direction"] == "up"
    assert payload[0]["prevPrice"] == 190.0
    await gen.aclose()


async def test_a_fresh_client_gets_the_whole_snapshot(fast_stream):
    cache = PriceCache()
    await cache.update_many([("AAPL", 190.0, 0.5), ("MSFT", 420.0, -0.2)])
    gen = stream.price_events(fake_request(cache))
    _, frame = await take(gen, 2)
    assert sorted(t["ticker"] for t in parse_frame(frame)) == ["AAPL", "MSFT"]
    await gen.aclose()


async def test_disconnect_ends_the_generator(fast_stream):
    gen = stream.price_events(fake_request(PriceCache(), disconnected=lambda: True))
    await take(gen, 1)                                   # the retry hint
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(gen.__anext__(), 2.0)     # must not loop forever
```

Response headers are worth one separate `TestClient` check, since it only reads headers and never consumes the body:

```python
def test_stream_sets_sse_headers(client):
    with client.stream("GET", "/api/stream/prices") as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert "no-cache" in response.headers["cache-control"]
        assert response.headers["x-accel-buffering"] == "no"
```

### 11.5 E2E

Per `PLAN.md` §12, Playwright tests run with `LLM_MOCK=true` and **no** `MASSIVE_API_KEY`, so the simulator drives them. The market-data-relevant scenarios:

- prices visibly change within a few seconds of load (the stream is live);
- price cells flash green/red on change;
- the connection dot reads connected;
- killing and restoring the backend flips the dot to reconnecting and back, verifying `EventSource` retry;
- a ticker added to the watchlist begins streaming on the next tick (the `invalidate()` path);
- buying a non-watchlisted ticker succeeds and its position shows a live price (the `prime()` path).

---

## 12. Corrections to the earlier design sketches

The code above is written correct. These are the places it knowingly departs from a snippet in `MARKET_INTERFACE.md` or `MARKET_SIMULATOR.md`, recorded so a reader comparing the documents knows which to trust. Those documents remain accurate as *rationale*; the code here supersedes their *snippets*.

**12.1 — The correlation blend destroyed the configured volatility.** `MARKET_SIMULATOR.md` §6 uses `z = 0.6·idio + 0.4·market`. Since both draws are independent standard normals, `Var(z) = 0.36 + 0.16 = 0.52`, so every ticker realized `√0.52 ≈ 72%` of its stated `sigma` — a ticker configured at 30% annualized vol actually moved at ~21.6%. Measured over 400,000 samples: `stdev(z) = 0.7204`. The fix is the quadrature form in [§5.5](#55-correlation), `z = ρ·market + √(1−ρ²)·idio`, which measures `0.9990`. Choosing `ρ = 0.55` also preserves the pairwise correlation the old blend happened to produce (0.3095 measured, versus 0.3043 for the new form) — so the market-wide co-movement is unchanged and only the volatility bug is fixed.

**12.2 — `2.718281828 ** x` replaced with `math.exp(x)`.** The literal is accurate to 10 significant figures where a float carries ~16, and `exp` is both more precise and faster.

**12.3 — The pure `gbm_step` now exists.** `MARKET_SIMULATOR.md` §7 asks for "the per-tick math as a small pure function extracted from `run`", but its §6 snippet inlines the math in the `async` loop, leaving nothing to test without `asyncio.sleep`. [§5.4](#54-the-tick-step) extracts it, and [§5.6](#56-the-provider) adds the synchronous `advance()` so the whole tick — correlation, events, `change_pct` — is testable without the event loop.

**12.4 — `dict(DEFAULT_SEEDS)` shared mutable state across instances.** `MARKET_SIMULATOR.md` §6 has `self._state = dict(DEFAULT_SEEDS)  # copy`. That copies the dict but not the `TickerState` values, so `state.price *= …` mutates the module-level constant. Consequences: a second `SimulatorProvider` starts from the first one's prices, and every test leaks state into the next. Fixed in [§5.2](#52-state-and-seeds) by storing immutable `(price, mu, sigma)` tuples and constructing fresh `TickerState` objects per instance. `test_instances_do_not_share_seed_state` guards it.

**12.5 — The `startup_check` fallback was unreachable.** `MARKET_INTERFACE.md` §6 says a failed check should "fall back to `SimulatorProvider` rather than refuse to start", but `create_provider()` returns the provider before any check runs, so nothing in that design could perform the swap. [§7.3](#73-lifespan-wiring) sequences it explicitly in the lifespan, where the rebind is possible.

**12.6 — Missing imports and undefined names.** The earlier snippets reference `os` in `massive_provider.py` (never imported), `PriceTick` in `interface.py` (never imported), and `_now_iso()` in `cache.py` (never defined). All are present here.

**12.7 — Cold start left the cache empty.** Nothing in the earlier docs primes the cache before `run()` begins, so in Massive mode the first clients get a blank watchlist for up to a full poll interval. [§7.3](#73-lifespan-wiring) step 2 primes the tracked set first.

**12.8 — `429` had no distinct handling.** `MASSIVE_API.md` §5 documents `429` with an optional `Retry-After`, but `MARKET_INTERFACE.md`'s `run` catches every exception identically and sleeps the normal interval — which re-hits a rate-limited endpoint immediately. [§6.3](#63-the-provider) gives it a backoff branch honoring the header.

**12.9 — Per-ticker cache writes became one batch.** The earlier `run` awaits `cache.update()` once per ticker, so an SSE `snapshot()` between two writes observes half a tick and the ticks in one tick carry different timestamps. `update_many` ([§4](#4-cachepy--pricecache)) applies a tick atomically under one timestamp.

---

## 13. Deliberate simplifications

Accepted trade-offs, recorded so a later reader does not file them as defects.

| Simplification | Why | Consequence |
|---|---|---|
| Simulator `change_pct` baselines on process start; Massive baselines on the previous close | The simulator has no concept of trading days, so "since the backend started" is the only honest baseline it can offer | "Daily change %" resets on every container restart in simulator mode. Worth a tooltip if it is ever labeled "daily". |
| Simulator state is not persisted | `PLAN.md` §10 already treats all chart data as ephemeral; the simulator is not a source of historical truth | A restart reseeds every ticker to its table or hash-derived price. |
| No historical price endpoint | `PLAN.md` §10 — one price source, the live cache | Sparklines and the main chart start empty on page load and fill from the stream. Selecting a new ticker starts its chart empty. |
| Single in-process cache, no Redis | `PLAN.md` §3 scopes this to one user, one container | Horizontal scaling would need a shared cache. The `PriceCache` interface is small enough to swap. |
| Massive poll interval is configured, not detected | Detecting the plan tier means parsing undocumented account metadata | The student sets `MASSIVE_POLL_SECONDS` once for their plan. |
| No symbol validation against a real ticker list | `PLAN.md` §6 | Any 1–5 character alphanumeric symbol is tradeable. In simulator mode it gets a hash-derived price; in Massive mode it fails with "no price available". |
| `SimulatorProvider.prime` cannot fail | Every symbol has a hash-derived seed | The 400 in [§9](#9-prime-on-the-trade-path) is reachable only in Massive mode — the one user-visible behavioral difference between the two providers. |

---

## Implementation checklist

1. `models.py`, `interface.py`, `cache.py` — no dependencies beyond stdlib; unit-test them first.
2. `simulator.py` — `gbm_step` and `advance` before the `async` shell, so tests can drive them directly.
3. `tracking.py` + `factory.py` — needs the `watchlist` and `positions` tables from `PLAN.md` §7.
4. `main.py` lifespan — wire cache, tracked, provider, priming, and the background task.
5. `api/stream.py` — verify with `curl -N localhost:8000/api/stream/prices` before touching the frontend.
6. `_tickers.py` + `resolve_price` — route every trade path through them.
7. `massive_provider.py` — last. It is the optional stretch goal (`PLAN.md` §5); nothing else depends on it.
