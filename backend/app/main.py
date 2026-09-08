"""FastAPI application and market-data lifespan wiring (MARKET_DATA_DESIGN.md §7.3)."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import db
from .api import stream
from .market_data import PriceCache, TrackedTickers, create_provider
from .market_data.massive_provider import MassivePlanError
from .market_data.simulator import SimulatorProvider

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()  # lazy schema + seed (PLAN.md §7)

    cache = PriceCache()
    tracked = TrackedTickers(db.connect)
    provider = create_provider()

    # 1. Fail fast, then degrade — never refuse to start (PLAN.md §5). Massive
    #    is the optional stretch goal; the simulator is the required baseline.
    try:
        await provider.startup_check()
    except MassivePlanError as exc:
        logger.error("%s Falling back to the market simulator.", exc)
        provider = SimulatorProvider()
    except Exception:
        logger.exception("Provider startup check failed; falling back to the simulator.")
        provider = SimulatorProvider()

    # 2. Warm the cache before accepting traffic. run() doesn't fill it until
    #    its first cycle completes — up to a full poll interval in Massive mode
    #    — and every client connecting in that window would see a blank grid.
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
        # cancel() only requests cancellation; awaiting it is what keeps uvicorn
        # from tearing the loop down mid-tick and logging a pending-task warning.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        db.close()


app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(stream.router)


@app.get("/api/health")
async def health() -> dict:
    """Health check for Docker/deployment (PLAN.md §8)."""
    provider = getattr(app.state, "provider", None)
    cache = getattr(app.state, "price_cache", None)
    return {
        "status": "ok",
        "provider": provider.name if provider is not None else None,
        "tickers_cached": len(cache.snapshot()) if cache is not None else 0,
    }
