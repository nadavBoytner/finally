from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.market_data import PriceCache
from app.market_data.massive_provider import (
    MassivePlanError,
    MassiveProvider,
    parse_snapshot,
)


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


# --- construction --------------------------------------------------------


def test_injected_client_skips_the_massive_sdk_import():
    # Constructing with a client= must never try `from massive import RESTClient`.
    provider = MassiveProvider("k", client=FakeClient())
    assert provider._client.__class__ is FakeClient


def test_poll_seconds_defaults_when_not_given(monkeypatch):
    monkeypatch.delenv("MASSIVE_POLL_SECONDS", raising=False)
    provider = MassiveProvider("k", client=FakeClient())
    assert provider._poll_seconds == 15.0


def test_poll_seconds_reads_env_var(monkeypatch):
    monkeypatch.setenv("MASSIVE_POLL_SECONDS", "5")
    provider = MassiveProvider("k", client=FakeClient())
    assert provider._poll_seconds == 5.0


def test_explicit_poll_seconds_overrides_env(monkeypatch):
    monkeypatch.setenv("MASSIVE_POLL_SECONDS", "5")
    provider = MassiveProvider("k", poll_seconds=1.0, client=FakeClient())
    assert provider._poll_seconds == 1.0


# --- parsing ---------------------------------------------------------------


def test_parse_prefers_last_trade_then_falls_back_to_day_close():
    assert parse_snapshot(snap("AAPL", price=120.47))[1] == 120.47
    assert parse_snapshot(snap("AAPL", day_close=120.42))[1] == 120.42


def test_parse_rejects_zero_and_missing_prices():
    # Pre-market snapshots can return 0.0; a $0 mark would corrupt valuations.
    assert parse_snapshot(snap("AAPL", price=0.0)) is None
    assert parse_snapshot(snap("AAPL")) is None


def test_parse_uppercases_the_ticker():
    assert parse_snapshot(snap("aapl", price=1.0))[0] == "AAPL"


def test_parse_returns_none_without_a_ticker():
    assert parse_snapshot(SimpleNamespace(ticker=None)) is None


def test_parse_defaults_missing_change_pct_to_zero():
    s = snap("AAPL", price=1.0)
    s.todays_change_percent = None
    assert parse_snapshot(s)[2] == 0.0


def test_parse_never_raises_on_missing_sub_objects():
    bare = SimpleNamespace(ticker="AAPL")
    assert parse_snapshot(bare) is None


# --- _fetch / run ------------------------------------------------------------
#
# The real `massive` SDK's own exceptions (`BadResponse`, `AuthError`) carry no
# HTTP status or headers — see MARKET_DATA_REVIEW.md Finding 1 — so there is no
# 429-specific path to test here. A poll failure is just logged and retried at
# the normal cadence, which is what these tests verify.


@pytest.mark.asyncio
async def test_run_requests_exactly_the_tracked_set():
    client = FakeClient([snap("AAPL", price=190.0), snap("MSFT", price=420.0)])
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)
    cache = PriceCache()

    task = asyncio.create_task(provider.run(cache, lambda: {"AAPL", "MSFT"}))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.calls[0] == ["AAPL", "MSFT"]   # sorted, exactly tracked()
    assert cache.get("AAPL").price == 190.0


@pytest.mark.asyncio
async def test_run_skips_the_fetch_when_nothing_is_tracked():
    client = FakeClient([])
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: set()))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.calls == []


@pytest.mark.asyncio
async def test_run_survives_a_poll_failure():
    client = FakeClient(error=RuntimeError("boom"))
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: {"AAPL"}))
    await asyncio.sleep(0.05)
    still_running = not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert still_running          # a bad poll must never kill the loop
    assert len(client.calls) > 1  # and it must keep retrying


@pytest.mark.asyncio
async def test_fetch_logs_and_returns_no_rows_on_failure_without_raising():
    client = FakeClient(error=RuntimeError("boom"))
    provider = MassiveProvider("k", client=client)
    assert await provider._fetch(["AAPL"]) == []


class SecondChunkFails(FakeClient):
    """Serves the first chunk normally but raises on any later chunk — used
    to verify a failed chunk doesn't discard rows already parsed from an
    earlier, successful one."""

    def get_snapshot_all(self, market, tickers):
        self.calls.append(list(tickers))
        if tickers == ["MSFT"]:
            raise RuntimeError("boom")
        return [s for s in self.snapshots if s.ticker in tickers]


@pytest.mark.asyncio
async def test_fetch_keeps_earlier_chunks_when_a_later_chunk_fails(monkeypatch):
    # Force one ticker per chunk so AAPL and MSFT land in separate requests.
    monkeypatch.setattr(
        "app.market_data.massive_provider.MAX_TICKERS_PER_REQUEST", 1
    )
    client = SecondChunkFails([snap("AAPL", price=190.0)])
    provider = MassiveProvider("k", client=client)

    rows = await provider._fetch(["AAPL", "MSFT"])

    assert rows == [("AAPL", 190.0, 1.5)]   # AAPL's chunk survives MSFT's failure
    assert len(client.calls) == 2           # both chunks were attempted


# --- startup_check / prime ---------------------------------------------------


@pytest.mark.asyncio
async def test_startup_check_raises_plan_error_on_failure():
    provider = MassiveProvider("k", client=FakeClient(error=RuntimeError("403")))
    with pytest.raises(MassivePlanError):
        await provider.startup_check()


@pytest.mark.asyncio
async def test_startup_check_succeeds_with_a_working_client():
    provider = MassiveProvider("k", client=FakeClient([snap("AAPL", price=1.0)]))
    await provider.startup_check()  # must not raise


@pytest.mark.asyncio
async def test_prime_returns_none_for_an_unknown_symbol():
    provider = MassiveProvider("k", client=FakeClient([]))
    assert await provider.prime(PriceCache(), "NOPE") is None


@pytest.mark.asyncio
async def test_prime_writes_the_resolved_price_into_the_cache():
    provider = MassiveProvider("k", client=FakeClient([snap("AAPL", price=190.0)]))
    cache = PriceCache()
    tick = await provider.prime(cache, "aapl")
    assert tick is not None
    assert tick.ticker == "AAPL"
    assert tick.price == 190.0
    assert cache.get("AAPL").price == 190.0


@pytest.mark.asyncio
async def test_prime_returns_none_on_client_error():
    provider = MassiveProvider("k", client=FakeClient(error=RuntimeError("network")))
    assert await provider.prime(PriceCache(), "AAPL") is None
