from __future__ import annotations

import os

from .interface import MarketDataProvider
from .simulator import SimulatorProvider


def create_provider() -> MarketDataProvider:
    """The one and only place the backend branches on MASSIVE_API_KEY."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if not api_key:
        return SimulatorProvider()
    from .massive_provider import MassiveProvider
    return MassiveProvider(api_key=api_key)
