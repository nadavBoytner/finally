from __future__ import annotations

import sqlite3

import pytest


@pytest.fixture
def db() -> sqlite3.Connection:
    """An in-memory sqlite connection with just the watchlist/positions tables
    that TrackedTickers queries — enough to unit-test tracking.py without the
    full application schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE watchlist (id TEXT PRIMARY KEY, user_id TEXT, ticker TEXT)"
    )
    conn.execute(
        "CREATE TABLE positions ("
        "id TEXT PRIMARY KEY, user_id TEXT, ticker TEXT, "
        "quantity INTEGER, avg_cost REAL"
        ")"
    )
    conn.commit()
    yield conn
    conn.close()
