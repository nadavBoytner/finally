"""SSE endpoint tests (MARKET_DATA_DESIGN.md §11.4).

These drive the `price_events` generator directly rather than going through
TestClient. `client.stream(...)` plus `islice(response.iter_text(), n)` looks
natural and hangs: the stream is infinite and, once the cache stops changing,
produces nothing but a heartbeat every 15 seconds, so any read past the first
frame blocks until the test times out.
"""

from __future__ import annotations

import asyncio
import json
import re
from types import SimpleNamespace

import pytest

from app.api import stream
from app.market_data import PriceCache


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


@pytest.mark.asyncio
async def test_emits_retry_hint_then_a_prices_event(fast_stream):
    cache = PriceCache()
    await cache.update_many([("AAPL", 190.0, 0.5), ("MSFT", 420.0, -0.2)])

    gen = stream.price_events(fake_request(cache))
    first, second = await take(gen, 2)

    assert first == f"retry: {stream.RETRY_MS}\n\n"
    assert second.startswith("event: prices\ndata: ")
    assert second.endswith("\n\n")  # SSE frame delimiter
    payload = parse_frame(second)
    assert sorted(t["ticker"] for t in payload) == ["AAPL", "MSFT"]
    assert set(payload[0]) == {
        "ticker", "price", "prevPrice", "changePct", "direction", "timestamp",
    }
    await gen.aclose()


@pytest.mark.asyncio
async def test_unchanged_prices_are_not_repushed(fast_stream):
    cache = PriceCache()
    await cache.update("AAPL", 190.0, 0.5)
    gen = stream.price_events(fake_request(cache))
    await take(gen, 2)  # retry hint + first payload

    (third,) = await take(gen, 1)
    assert third == ": heartbeat\n\n"  # nothing changed

    await cache.update("AAPL", 191.0, 0.6)
    (fourth,) = await take(gen, 1)
    payload = parse_frame(fourth)
    assert [t["ticker"] for t in payload] == ["AAPL"]  # only the changed one
    assert payload[0]["direction"] == "up"
    assert payload[0]["prevPrice"] == 190.0
    await gen.aclose()


@pytest.mark.asyncio
async def test_a_fresh_client_gets_the_whole_snapshot(fast_stream):
    cache = PriceCache()
    await cache.update_many([("AAPL", 190.0, 0.5), ("MSFT", 420.0, -0.2)])

    first_gen = stream.price_events(fake_request(cache))
    await take(first_gen, 2)
    await first_gen.aclose()

    # last_sent is per-connection: a second client still gets everything.
    gen = stream.price_events(fake_request(cache))
    _, frame = await take(gen, 2)
    assert sorted(t["ticker"] for t in parse_frame(frame)) == ["AAPL", "MSFT"]
    await gen.aclose()


@pytest.mark.asyncio
async def test_disconnect_ends_the_generator(fast_stream):
    gen = stream.price_events(fake_request(PriceCache(), disconnected=lambda: True))
    await take(gen, 1)  # the retry hint
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(gen.__anext__(), 2.0)  # must not loop forever


@pytest.mark.asyncio
async def test_an_empty_cache_yields_a_heartbeat_not_an_empty_frame(fast_stream):
    gen = stream.price_events(fake_request(PriceCache()))
    await take(gen, 1)
    (frame,) = await take(gen, 1)
    assert frame == ": heartbeat\n\n"
    await gen.aclose()
