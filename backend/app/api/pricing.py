"""On-demand price resolution for the trade path (MARKET_DATA_DESIGN.md §9.2).

A user can buy a ticker that is not on the watchlist, so its price may not be
in the cache yet. `resolve_price` primes it through the active provider rather
than making the user add it to the watchlist and wait for the next poll.

This lives here rather than in a portfolio module so that the market-data half
of the trade path is complete and tested on its own; the trade endpoint itself
(validation, cash and position math) belongs to the portfolio layer, which
calls `normalize_ticker` then `resolve_price` for both manual and LLM-issued
trades (PLAN.md §9 requires they share one validation path).
"""

from __future__ import annotations

from fastapi import HTTPException, Request

from ..market_data import PriceTick


async def resolve_tick(request: Request, symbol: str) -> PriceTick:
    """Latest tick for an already-normalized symbol, priming on a cache miss.

    Raises 400 when no price is obtainable. That is reachable only in Massive
    mode: the simulator derives a seed for any symbol, so its prime() never
    returns None. Against real market data, an unknown symbol genuinely has no
    price and the trade must be rejected.
    """
    cache = request.app.state.price_cache
    tick = cache.get(symbol)
    if tick is None:
        tick = await request.app.state.provider.prime(cache, symbol)
    if tick is None:
        raise HTTPException(
            status_code=400, detail=f"no price available for {symbol}"
        )
    return tick


async def resolve_price(request: Request, symbol: str) -> float:
    """The execution price for a market order on `symbol`."""
    return (await resolve_tick(request, symbol)).price
