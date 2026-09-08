"""SQLite access and lazy schema initialization (PLAN.md §7).

The backend owns its database entirely: on first use it creates the schema and
seeds default data if the file doesn't exist or is empty, so a fresh bind mount
starts clean and seeded with no migration step.

`connect()` returns a single shared connection for the process. That is the
contract `market_data.TrackedTickers` relies on: it calls `connect()` once and
holds the result for the process lifetime, because `sqlite3.Connection.__exit__`
commits but never closes, so a per-call factory would leak a connection roughly
once a second at the 2 Hz tick rate.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_USER_ID = "default"
DEFAULT_CASH_BALANCE = 10_000.0

# PLAN.md §7 — the ten default watchlist tickers.
DEFAULT_WATCHLIST = (
    "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
    "NVDA", "META", "JPM", "V", "NFLX",
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS users_profile (
    id           TEXT PRIMARY KEY,
    cash_balance REAL NOT NULL DEFAULT 10000.0,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist (
    id       TEXT PRIMARY KEY,
    user_id  TEXT NOT NULL DEFAULT 'default',
    ticker   TEXT NOT NULL,
    added_at TEXT NOT NULL,
    UNIQUE (user_id, ticker)
);

CREATE TABLE IF NOT EXISTS positions (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL DEFAULT 'default',
    ticker     TEXT NOT NULL,
    quantity   INTEGER NOT NULL,
    avg_cost   REAL NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (user_id, ticker)
);

CREATE TABLE IF NOT EXISTS trades (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL DEFAULT 'default',
    ticker      TEXT NOT NULL,
    side        TEXT NOT NULL,
    quantity    INTEGER NOT NULL,
    price       REAL NOT NULL,
    executed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL DEFAULT 'default',
    total_value REAL NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL DEFAULT 'default',
    role       TEXT NOT NULL,
    content    TEXT NOT NULL,
    actions    TEXT,
    created_at TEXT NOT NULL
);
"""

_connection: sqlite3.Connection | None = None


def database_path() -> Path:
    """Where the SQLite file lives.

    Defaults to the repo's top-level `db/finally.db`, which is the directory
    bind-mounted to /app/db in the container (PLAN.md §11). Override with
    FINALLY_DB_PATH; ":memory:" is honored for tests.
    """
    override = os.environ.get("FINALLY_DB_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "db" / "finally.db"


def connect() -> sqlite3.Connection:
    """The process-wide shared connection, opened on first use.

    check_same_thread=False because FastAPI runs sync route handlers in a
    threadpool while the market-data loop touches the same connection from the
    event loop thread.
    """
    global _connection
    if _connection is None:
        path = database_path()
        if str(path) != ":memory:":
            path.parent.mkdir(parents=True, exist_ok=True)
        _connection = sqlite3.connect(str(path), check_same_thread=False)
        _connection.row_factory = sqlite3.Row
        _connection.execute("PRAGMA foreign_keys = ON")
    return _connection


def close() -> None:
    """Close and forget the shared connection. For tests and shutdown."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def init_db() -> sqlite3.Connection:
    """Create the schema if missing and seed defaults if empty. Idempotent.

    Safe to call on every startup: CREATE TABLE IF NOT EXISTS never drops
    anything, and seeding is skipped once a profile row exists, so a
    bind-mounted database keeps its cash balance and positions across restarts.
    """
    conn = connect()
    conn.executescript(SCHEMA)
    _seed(conn)
    conn.commit()
    return conn


def _seed(conn: sqlite3.Connection) -> None:
    profile_exists = conn.execute(
        "SELECT 1 FROM users_profile WHERE id = ?", (DEFAULT_USER_ID,)
    ).fetchone()
    if profile_exists is None:
        conn.execute(
            "INSERT INTO users_profile (id, cash_balance, created_at) VALUES (?, ?, ?)",
            (DEFAULT_USER_ID, DEFAULT_CASH_BALANCE, _now_iso()),
        )

    # Seed the watchlist only when it is completely empty, so a user who has
    # removed every default ticker doesn't get them all back on restart.
    watchlist_exists = conn.execute(
        "SELECT 1 FROM watchlist WHERE user_id = ? LIMIT 1", (DEFAULT_USER_ID,)
    ).fetchone()
    if watchlist_exists is None:
        now = _now_iso()
        conn.executemany(
            "INSERT INTO watchlist (id, user_id, ticker, added_at) VALUES (?, ?, ?, ?)",
            [(str(uuid.uuid4()), DEFAULT_USER_ID, ticker, now)
             for ticker in DEFAULT_WATCHLIST],
        )
