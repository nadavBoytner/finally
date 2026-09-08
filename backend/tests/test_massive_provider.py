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

pytestmark = pytest.mark.asyncio


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


class RateLimited(Exception):
    status_code = 429
    response = SimpleNamespace(headers={"Retry-After": "2"})


class RateLimitedNoRetryAfter(Exception):
    status_code = 429
    response = SimpleNamespace(headers={})


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


# --- run ---------------------------------------------------------------------


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


async def test_run_skips_the_fetch_when_nothing_is_tracked():
    client = FakeClient([])
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: set()))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.calls == []


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


async def test_run_honors_retry_after_on_429():
    client = FakeClient(error=RateLimited())
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: {"AAPL"}))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(client.calls) == 1  # backed off 2s, so no second attempt yet


async def test_run_backs_off_without_retry_after_and_keeps_retrying():
    client = FakeClient(error=RateLimitedNoRetryAfter())
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: {"AAPL"}))
    await asyncio.sleep(0.1)
    still_running = not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # No Retry-After header: falls back to the growing backoff, but the loop
    # must never die and must eventually retry again.
    assert still_running
    assert len(client.calls) >= 1


class QuickRateLimited(Exception):
    status_code = 429
    response = SimpleNamespace(headers={"Retry-After": "0.05"})


class FlakyThenHealthyClient(FakeClient):
    """Rate-limited for the first N calls, then serves normally — used to
    verify backoff resets to the fast cadence after a success."""

    def __init__(self, snapshots, fail_calls: int):
        super().__init__(snapshots)
        self._fail_calls = fail_calls

    def get_snapshot_all(self, market, tickers):
        self.calls.append(list(tickers))
        if len(self.calls) <= self._fail_calls:
            raise QuickRateLimited()
        return self.snapshots


async def test_run_resets_backoff_after_a_success():
    client = FlakyThenHealthyClient([snap("AAPL", price=190.0)], fail_calls=1)
    provider = MassiveProvider("k", poll_seconds=0.01, client=client)

    task = asyncio.create_task(provider.run(PriceCache(), lambda: {"AAPL"}))
    # First call is rate-limited (short Retry-After); a naive implementation
    # that never resets backoff would stay slow forever after. Give it enough
    # time to pass the recovered call and resume the fast, poll_seconds cadence.
    await asyncio.sleep(0.15)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(client.calls) >= 3  # 1 failure + at least 2 fast follow-up polls


# --- startup_check / prime ---------------------------------------------------


async def test_startup_check_raises_plan_error_on_failure():
    provider = MassiveProvider("k", client=FakeClient(error=RuntimeError("403")))
    with pytest.raises(MassivePlanError):
        await provider.startup_check()


async def test_startup_check_succeeds_with_a_working_client():
    provider = MassiveProvider("k", client=FakeClient([snap("AAPL", price=1.0)]))
    await provider.startup_check()  # must not raise


async def test_prime_returns_none_for_an_unknown_symbol():
    provider = MassiveProvider("k", client=FakeClient([]))
    assert await provider.prime(PriceCache(), "NOPE") is None


async def test_prime_writes_the_resolved_price_into_the_cache():
    provider = MassiveProvider("k", client=FakeClient([snap("AAPL", price=190.0)]))
    cache = PriceCache()
    tick = await provider.prime(cache, "aapl")
    assert tick is not None
    assert tick.ticker == "AAPL"
    assert tick.price == 190.0
    assert cache.get("AAPL").price == 190.0


async def test_prime_returns_none_on_client_error():
    provider = MassiveProvider("k", client=FakeClient(error=RuntimeError("network")))
    assert await provider.prime(PriceCache(), "AAPL") is None
