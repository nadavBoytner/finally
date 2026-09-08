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


class MassivePlanError(RuntimeError):
    """The key cannot reach snapshot data — bad key, or a free-tier plan."""


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
        """Fetch every chunk, keeping rows already parsed even if a later
        chunk fails. The `massive` client's own errors (`BadResponse`,
        `AuthError`) carry no HTTP status — see MARKET_DATA_REVIEW.md
        Finding 1 — so a failed chunk is just logged and skipped rather
        than losing chunks that already succeeded."""
        rows: list[Row] = []
        for chunk in _chunks(tickers, MAX_TICKERS_PER_REQUEST):
            try:
                snaps = await asyncio.to_thread(
                    self._client.get_snapshot_all, "stocks", tickers=list(chunk)
                )
            except Exception:
                logger.warning(
                    "Massive snapshot request failed for %d ticker(s); skipping",
                    len(chunk),
                    exc_info=True,
                )
                continue
            for snap in snaps or ():
                row = parse_snapshot(snap)
                if row is not None:
                    rows.append(row)
        return rows

    async def run(self, cache: PriceCache, tracked: Tracked) -> None:
        while True:
            tickers = sorted(tracked())
            if tickers:
                rows = await self._fetch(tickers)
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
