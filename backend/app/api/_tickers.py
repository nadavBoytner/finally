"""Ticker normalization — the single entry point for every ticker string.

MARKET_DATA_DESIGN.md §9.1. Routing watchlist adds, manual trades, and
LLM-issued actions all through `normalize_ticker` is what makes `aapl` and
`AAPL` the same ticker in the price cache, the watchlist table, the positions
table, and the provider.
"""

from __future__ import annotations

import re

from fastapi import HTTPException

# PLAN.md §6: any 1-5 character alphanumeric symbol is accepted; there is no
# validation against a real ticker list.
TICKER_RE = re.compile(r"^[A-Z0-9]{1,5}$")


def normalize_ticker(raw: str | None) -> str:
    """Uppercase, strip, and validate a user- or LLM-supplied ticker."""
    symbol = (raw or "").strip().upper()
    if not TICKER_RE.match(symbol):
        raise HTTPException(status_code=400, detail=f"invalid ticker: {raw!r}")
    return symbol
