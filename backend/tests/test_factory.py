from __future__ import annotations

import sys
import types

from app.market_data.factory import create_provider
from app.market_data.simulator import SimulatorProvider


def test_no_api_key_returns_simulator(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    assert isinstance(create_provider(), SimulatorProvider)


def test_blank_api_key_returns_simulator(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "   ")
    assert isinstance(create_provider(), SimulatorProvider)


def test_api_key_returns_massive_provider(monkeypatch):
    """Stub the `massive` SDK so this exercises factory.py's branch without
    requiring the real (optional, network-capable) package to be installed."""
    monkeypatch.setenv("MASSIVE_API_KEY", " sk-live-123 ")

    class FakeRESTClient:
        def __init__(self, api_key):
            self.api_key = api_key

    fake_module = types.ModuleType("massive")
    fake_module.RESTClient = FakeRESTClient
    monkeypatch.setitem(sys.modules, "massive", fake_module)

    from app.market_data.massive_provider import MassiveProvider

    provider = create_provider()
    assert isinstance(provider, MassiveProvider)
    assert provider._client.api_key == "sk-live-123"  # trimmed before use
