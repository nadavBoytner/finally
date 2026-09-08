"""SSE price stream — GET /api/stream/prices (MARKET_DATA_DESIGN.md §8).

One long-lived connection per browser tab, consumed by the native EventSource
API. The endpoint only ever reads app.state.price_cache; it neither knows nor
cares which provider is filling it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

router = APIRouter()

PUSH_SECONDS = 0.5        # matches the simulator tick
HEARTBEAT_SECONDS = 15.0  # keeps idle proxies from reaping the connection
RETRY_MS = 3000           # EventSource reconnect delay hint

SSE_HEADERS = {
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    # nginx buffers proxied responses by default, which for SSE means the
    # browser receives nothing until the buffer fills.
    "X-Accel-Buffering": "no",
}


async def price_events(request: Request) -> AsyncIterator[str]:
    """Yield SSE frames until the client disconnects.

    Each frame carries a JSON array of ticks rather than one event per ticker:
    a 30-ticker watchlist would otherwise fire 60 handler invocations a second
    on the client. Only ticks whose timestamp this connection has not already
    seen are sent, so a 15s Massive poll doesn't produce 30 identical frames.
    """
    cache = request.app.state.price_cache
    last_sent: dict[str, str] = {}  # ticker -> last timestamp pushed to THIS client
    last_flush = time.monotonic()

    yield f"retry: {RETRY_MS}\n\n"

    while True:
        if await request.is_disconnected():
            break

        payload = []
        for tick in cache.snapshot().values():
            if last_sent.get(tick.ticker) == tick.timestamp:
                continue
            last_sent[tick.ticker] = tick.timestamp
            payload.append(tick.to_dict())

        now = time.monotonic()
        if payload:
            data = json.dumps(payload, separators=(",", ":"))
            yield f"event: prices\ndata: {data}\n\n"
            last_flush = now
        elif now - last_flush >= HEARTBEAT_SECONDS:
            # A comment line is valid SSE that the browser ignores; it exists
            # only to keep proxies from closing an idle connection.
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
