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
