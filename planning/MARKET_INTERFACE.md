# Unified Market Data Interface

Design for `backend/app/market_data/`: the shared abstraction that lets the rest of the backend (SSE stream, trade execution, portfolio valuation) work identically whether prices come from the simulator or from Massive. Builds on [`MASSIVE_API.md`](./MASSIVE_API.md); the simulator's own internals are documented separately in [`MARKET_SIMULATOR.md`](./MARKET_SIMULATOR.md).

## 1. Goals

- One interface, two implementations (`SimulatorProvider`, `MassiveProvider`), selected once at startup by `MASSIVE_API_KEY` (`PLAN.md` §5/§6).
- Everything downstream (SSE endpoint, `/api/portfolio`, trade validation) reads from a single in-memory `PriceCache` and never talks to a provider directly.
- The cache tracks the **watchlist ∪ open positions** union, not just the watchlist (`PLAN.md` §6), so a sold-off-the-watchlist holding still prices correctly.
- Ticker symbols are normalized (uppercased) at every entry point — watchlist add, trade, chat action — before touching the cache, watchlist table, or a provider. This resolves the `REVIEW.md` "case normalization" finding: `aapl` and `AAPL` are the same ticker everywhere.

## 2. Data model

```python
# backend/app/market_data/models.py
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class PriceTick:
    ticker: str          # always uppercase
    price: float         # latest known price
    prev_price: float    # price immediately before this update (for up/down flash direction)
    change_pct: float    # % change vs. prior trading day close
    timestamp: str        # ISO 8601, UTC
```

`prev_price` is tick-to-tick (this update vs. the last one), which is what the frontend's flash animation needs. `change_pct` is day-over-day, which is what the watchlist's "daily change %" column needs — these are deliberately different baselines, resolving the `REVIEW.md` finding that a single previous-tick field can't serve both. The simulator computes `change_pct` against its own session-start price (see `MARKET_SIMULATOR.md` §4); Massive supplies it directly as `todaysChangePerc`.

## 3. The provider interface

```python
# backend/app/market_data/interface.py
from abc import ABC, abstractmethod
from typing import Callable

class MarketDataProvider(ABC):
    @abstractmethod
    async def run(self, cache: "PriceCache", tracked: Callable[[], set[str]]) -> None:
        """Run forever. Each tick, read tracked() for the current ticker set
        and call cache.update(...) for each. Cancelled on app shutdown."""

    @abstractmethod
    async def prime(self, cache: "PriceCache", ticker: str) -> PriceTick | None:
        """Fetch a price for one ticker immediately, outside the poll cycle,
        and write it into the cache. Returns None if no price is obtainable
        (e.g. Massive has no data for the symbol)."""
```

Two methods, not one, because the project has two distinct access patterns:

- **`run`** — the continuous background loop backing the SSE stream. Started once at app startup as an `asyncio` task.
- **`prime`** — synchronous-feeling, on-demand priming for a ticker the cache doesn't have yet. This is what resolves the `REVIEW.md` "unlisted ticker has no price-acquisition path" finding: a trade (manual or LLM) on a ticker not currently in the cache calls `prime()` first. For the simulator this is instant (hash-derived seed, see `MARKET_SIMULATOR.md` §5). For Massive it's one `get_snapshot_ticker` call; if that returns no data, `prime` returns `None` and the trade is rejected with "no price available for {ticker}" — the same error path for both providers.

`tracked()` is a callback into the backend's own state (`SELECT ticker FROM watchlist UNION SELECT ticker FROM positions`), not something the provider owns. Passing it as a callable rather than a static list means a provider always sees the current set without the caller having to restart or reconfigure it.

## 4. Price cache

```python
# backend/app/market_data/cache.py
import asyncio

class PriceCache:
    def __init__(self) -> None:
        self._prices: dict[str, PriceTick] = {}
        self._lock = asyncio.Lock()

    async def update(self, ticker: str, price: float, change_pct: float) -> PriceTick:
        ticker = ticker.upper()
        async with self._lock:
            prev = self._prices.get(ticker)
            tick = PriceTick(
                ticker=ticker,
                price=price,
                prev_price=prev.price if prev else price,
                change_pct=change_pct,
                timestamp=_now_iso(),
            )
            self._prices[ticker] = tick
            return tick

    def get(self, ticker: str) -> PriceTick | None:
        return self._prices.get(ticker.upper())

    def snapshot(self) -> dict[str, PriceTick]:
        return dict(self._prices)
```

Single process, single cache instance, held on `app.state`. No cross-process concerns (`PLAN.md` explicitly scopes this to single-user/single-container). The lock only guards the dict swap, not I/O, so it's cheap even at a 500ms tick rate.

The SSE endpoint (`GET /api/stream/prices`) reads `cache.snapshot()` on each push cycle; it does not know or care which provider is running.

## 5. Provider selection

```python
# backend/app/market_data/factory.py
import os
from .simulator import SimulatorProvider
from .massive_provider import MassiveProvider

def create_provider() -> MarketDataProvider:
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not api_key:
        return SimulatorProvider()
    return MassiveProvider(api_key=api_key)
```

Called once at FastAPI startup. This is the only place that branches on `MASSIVE_API_KEY` — everything else in the backend is provider-agnostic.

## 6. `MassiveProvider`

```python
# backend/app/market_data/massive_provider.py
import asyncio
import logging
from massive import RESTClient
from .interface import MarketDataProvider

logger = logging.getLogger(__name__)

class MassivePlanError(RuntimeError):
    """Raised at startup when the key can't reach snapshot data (bad key or free-tier plan)."""

class MassiveProvider(MarketDataProvider):
    def __init__(self, api_key: str, poll_seconds: float | None = None):
        self._client = RESTClient(api_key)
        self._poll_seconds = poll_seconds or float(os.environ.get("MASSIVE_POLL_SECONDS", "15"))

    async def startup_check(self) -> None:
        """Fail fast: one snapshot call before the app accepts traffic. Distinguishes a
        bad/free-tier key from a transient network error, per MASSIVE_API.md §5-6."""
        try:
            await asyncio.to_thread(self._client.get_snapshot_ticker, "stocks", ticker="AAPL")
        except Exception as exc:
            raise MassivePlanError(
                "MASSIVE_API_KEY set but snapshot data is unreachable — likely a free-tier "
                "key (Starter plan or above is required for snapshots; see MASSIVE_API.md §3)."
            ) from exc

    async def run(self, cache, tracked) -> None:
        while True:
            tickers = sorted(tracked())
            if tickers:
                try:
                    snaps = await asyncio.to_thread(
                        self._client.get_snapshot_all, "stocks", tickers=tickers
                    )
                    for s in snaps:
                        await cache.update(s.ticker, s.last_trade.price, s.todays_change_percent)
                except Exception:
                    logger.warning("Massive poll failed, will retry next cycle", exc_info=True)
            await asyncio.sleep(self._poll_seconds)

    async def prime(self, cache, ticker: str) -> PriceTick | None:
        try:
            snap = await asyncio.to_thread(self._client.get_snapshot_ticker, "stocks", ticker=ticker)
        except Exception:
            return None
        if snap is None or snap.last_trade is None:
            return None
        return await cache.update(ticker, snap.last_trade.price, snap.todays_change_percent)
```

Design notes:

- `RESTClient` is synchronous (`httpx`-backed but blocking calls); every call is wrapped in `asyncio.to_thread` so it doesn't stall the event loop that's also serving SSE to other clients.
- `poll_seconds` defaults to 15s per `PLAN.md` §6, overridable via `MASSIVE_POLL_SECONDS` for a paid plan that can go faster. It is **not** auto-detected from the plan tier — that would require an extra API call and parsing undocumented plan metadata; a documented env var is simpler and the student sets it once when they know their plan.
- A single failed poll logs and retries next cycle rather than crashing the background task — matches "the simulator is the required baseline, Massive is a stretch goal" priority in `PLAN.md` §5.
- `startup_check()` is called once from the FastAPI lifespan handler right after `create_provider()`, before `run()` is scheduled. If it raises, the app should log a clear error and **fall back to `SimulatorProvider`** rather than refuse to start — consistent with Massive being optional, never blocking.

## 7. `SimulatorProvider`

Implements the same two methods (`run`, `prime`) using the GBM engine described in `MARKET_SIMULATOR.md`. Because it has no external calls, `run` ticks every ~500ms and `prime` is synchronous and instant — no network round trip, no failure mode to handle.

## 8. What this resolves from `REVIEW.md`

| Review finding | Resolution here |
|---|---|
| Massive polling excludes held-but-unwatched tickers | `tracked()` callback is always `watchlist ∪ positions`, sourced from the backend, not the provider — a provider can't get this scope wrong. |
| Unlisted ticker has no price-acquisition path for a direct trade | `prime()` — the trade handler calls it before validating price if the ticker isn't already cached. |
| Ticker case normalization undefined | Normalization happens in `PriceCache` (`ticker.upper()`) and is required at every write path into watchlist/trade/chat before it reaches the cache or a provider. |
| Daily change % undefined | `PriceTick.change_pct`, distinct from `prev_price`; sourced from `todaysChangePerc` (Massive) or session-start baseline (simulator). |

## 9. Testing

- `PriceCache` and the interface are pure/async-only — unit-testable without any provider.
- `MassiveProvider` tests mock `RESTClient` (no live network calls in CI); verify `run` calls `get_snapshot_all` with exactly the `tracked()` set, and that a raised exception doesn't kill the loop.
- `SimulatorProvider` tests are statistical/deterministic — see `MARKET_SIMULATOR.md` §7.
- No `LLM_MOCK`-style flag is needed for market data: the simulator already *is* the deterministic, dependency-free default, so tests default to it simply by not setting `MASSIVE_API_KEY`.
