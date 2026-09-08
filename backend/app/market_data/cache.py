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
