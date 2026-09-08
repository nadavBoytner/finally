from __future__ import annotations

import sqlite3

import pytest

from app import db


@pytest.fixture
def temp_db(tmp_path, monkeypatch):
    """Point the db module at a throwaway file and reset its shared connection."""
    monkeypatch.setenv("FINALLY_DB_PATH", str(tmp_path / "test.db"))
    db.close()
    yield
    db.close()


def test_init_db_creates_every_table(temp_db):
    conn = db.init_db()
    names = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert names >= {
        "users_profile",
        "watchlist",
        "positions",
        "trades",
        "portfolio_snapshots",
        "chat_messages",
    }


def test_init_db_seeds_the_default_profile_and_watchlist(temp_db):
    conn = db.init_db()
    cash = conn.execute(
        "SELECT cash_balance FROM users_profile WHERE id = 'default'"
    ).fetchone()[0]
    assert cash == 10_000.0

    tickers = {
        row[0] for row in conn.execute("SELECT ticker FROM watchlist")
    }
    assert tickers == set(db.DEFAULT_WATCHLIST)
    assert len(db.DEFAULT_WATCHLIST) == 10


def test_init_db_is_idempotent(temp_db):
    db.init_db()
    conn = db.init_db()  # second call must not duplicate or reset anything
    assert conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0] == 10
    assert conn.execute("SELECT COUNT(*) FROM users_profile").fetchone()[0] == 1


def test_reinit_preserves_user_state(temp_db):
    """A restart against a bind-mounted database must not reset the portfolio."""
    conn = db.init_db()
    conn.execute("UPDATE users_profile SET cash_balance = 4200.0 WHERE id = 'default'")
    conn.execute("DELETE FROM watchlist WHERE ticker = 'AAPL'")
    conn.commit()

    conn = db.init_db()
    assert conn.execute(
        "SELECT cash_balance FROM users_profile WHERE id = 'default'"
    ).fetchone()[0] == 4200.0
    # A removed ticker stays removed: seeding only fires on a wholly empty list.
    tickers = {row[0] for row in conn.execute("SELECT ticker FROM watchlist")}
    assert "AAPL" not in tickers and len(tickers) == 9


def test_connect_returns_one_shared_connection(temp_db):
    """TrackedTickers holds the result for the process lifetime, so connect()
    must not hand out a fresh connection per call."""
    assert db.connect() is db.connect()


def test_close_lets_a_new_connection_open(temp_db):
    first = db.connect()
    db.close()
    assert db.connect() is not first


def test_watchlist_ticker_is_unique_per_user(temp_db):
    conn = db.init_db()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO watchlist (id, user_id, ticker, added_at)"
            " VALUES ('x', 'default', 'AAPL', 'now')"
        )
