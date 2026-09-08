"""End-to-end wiring: lifespan, headers, health, normalization, price resolution.

Covers MARKET_DATA_DESIGN.md §7.3, §8 (headers), and §9 against the real
FastAPI app with the simulator provider.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app import db
from app.api._tickers import normalize_ticker
from app.api.pricing import resolve_price, resolve_tick
from app.market_data import PriceCache


@pytest.fixture
def app_client(tmp_path, monkeypatch):
    """The real app on a throwaway database, with the simulator provider."""
    monkeypatch.setenv("FINALLY_DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    db.close()
    from app.main import app  # imported here so the env vars above apply

    with TestClient(app) as client:
        yield client
    db.close()


# --- ticker normalization (§9.1) ---------------------------------------------

def test_normalize_uppercases_and_strips():
    assert normalize_ticker("  aapl ") == "AAPL"
    assert normalize_ticker("xyz12") == "XYZ12"


@pytest.mark.parametrize("bad", ["", "   ", "TOOLONG", "AA-PL", "AA PL", None])
def test_normalize_rejects_invalid_symbols(bad):
    with pytest.raises(HTTPException) as exc:
        normalize_ticker(bad)
    assert exc.value.status_code == 400


# --- lifespan (§7.3) ---------------------------------------------------------

def test_lifespan_primes_the_cache_before_serving(app_client):
    """Every seeded watchlist ticker must have a price before the first request,
    so a client connecting immediately doesn't see a blank grid."""
    body = app_client.get("/api/health").json()
    assert body["status"] == "ok"
    assert body["provider"] == "simulator"
    assert body["tickers_cached"] == len(db.DEFAULT_WATCHLIST)


def test_lifespan_exposes_state_for_the_routes(app_client):
    state = app_client.app.state
    assert isinstance(state.price_cache, PriceCache)
    assert state.provider.name == "simulator"
    assert state.tracked() == set(db.DEFAULT_WATCHLIST)


def test_background_task_updates_prices(app_client):
    """The provider loop is actually running, not merely constructed."""
    cache = app_client.app.state.price_cache
    before = cache.get("AAPL")
    for _ in range(60):  # up to ~3s at the 0.5s tick
        if cache.get("AAPL").timestamp != before.timestamp:
            break
        app_client.get("/api/health")  # yields to the event loop
    assert cache.get("AAPL").timestamp != before.timestamp


# --- SSE response headers (§8) -----------------------------------------------

def test_stream_sets_sse_headers(app_client):
    with app_client.stream("GET", "/api/stream/prices") as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert "no-cache" in response.headers["cache-control"]
        assert response.headers["x-accel-buffering"] == "no"


# --- price resolution on the trade path (§9.2) -------------------------------

@pytest.mark.asyncio
async def test_resolve_price_primes_an_uncached_ticker(app_client):
    """Buying a non-watchlisted ticker must work without a watchlist round trip."""
    request = _request_for(app_client)
    assert request.app.state.price_cache.get("PYPL") is None
    price = await resolve_price(request, "PYPL")
    assert price > 0
    assert request.app.state.price_cache.get("PYPL") is not None


@pytest.mark.asyncio
async def test_resolve_tick_uses_the_cache_on_a_hit(app_client):
    request = _request_for(app_client)
    cached = request.app.state.price_cache.get("AAPL")
    tick = await resolve_tick(request, "AAPL")
    assert tick.timestamp == cached.timestamp  # no reprime


@pytest.mark.asyncio
async def test_resolve_price_400s_when_no_price_is_obtainable(app_client):
    """Reachable only in Massive mode; simulated here with a provider whose
    prime() returns None, which is what MassiveProvider does for an unknown
    symbol."""
    request = _request_for(app_client)

    class NoPriceProvider:
        name = "none"

        async def prime(self, cache, ticker):
            return None

    request.app.state.provider = NoPriceProvider()
    with pytest.raises(HTTPException) as exc:
        await resolve_price(request, "NOPE")
    assert exc.value.status_code == 400
    assert "no price available for NOPE" in exc.value.detail


def _request_for(client) -> object:
    """A minimal stand-in for a Request: resolve_* only touches request.app."""
    from types import SimpleNamespace

    return SimpleNamespace(app=client.app)
