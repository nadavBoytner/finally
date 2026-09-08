from __future__ import annotations

import time

from app.market_data.tracking import TrackedTickers


def test_tracked_unions_watchlist_and_positions(db):
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','AAPL')")
    db.execute(
        "INSERT INTO positions (id, user_id, ticker, quantity, avg_cost)"
        " VALUES ('2','default','TSLA',5,250.0)"
    )
    db.commit()
    assert TrackedTickers(lambda: db)() == {"AAPL", "TSLA"}


def test_tracked_excludes_zero_quantity_positions(db):
    db.execute(
        "INSERT INTO positions (id, user_id, ticker, quantity, avg_cost)"
        " VALUES ('3','default','MSFT',0,420.0)"
    )
    db.commit()
    assert "MSFT" not in TrackedTickers(lambda: db)()


def test_tracked_deduplicates_a_ticker_on_both_watchlist_and_positions(db):
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','AAPL')")
    db.execute(
        "INSERT INTO positions (id, user_id, ticker, quantity, avg_cost)"
        " VALUES ('2','default','AAPL',5,190.0)"
    )
    db.commit()
    assert TrackedTickers(lambda: db)() == {"AAPL"}


def test_tracked_scopes_to_the_given_user_id(db):
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','other','AAPL')")
    db.commit()
    assert TrackedTickers(lambda: db, user_id="default")() == set()


def test_tracked_uppercases_tickers(db):
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','aapl')")
    db.commit()
    assert TrackedTickers(lambda: db)() == {"AAPL"}


def test_tracked_result_is_memoized_within_the_ttl(db):
    tracked = TrackedTickers(lambda: db, ttl=60.0)
    assert tracked() == set()
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','NVDA')")
    db.commit()
    assert tracked() == set()  # still within the TTL window


def test_invalidate_forces_a_refetch(db):
    tracked = TrackedTickers(lambda: db, ttl=60.0)
    tracked()
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('4','default','NVDA')")
    db.commit()
    assert "NVDA" not in tracked()   # still memoized
    tracked.invalidate()
    assert "NVDA" in tracked()


def test_ttl_expiry_forces_a_refetch_without_invalidate(db):
    tracked = TrackedTickers(lambda: db, ttl=0.01)
    tracked()
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','NVDA')")
    db.commit()
    time.sleep(0.02)
    assert "NVDA" in tracked()


def test_returned_set_is_a_copy_and_cannot_mutate_the_memo(db):
    db.execute("INSERT INTO watchlist (id, user_id, ticker) VALUES ('1','default','AAPL')")
    db.commit()
    tracked = TrackedTickers(lambda: db, ttl=60.0)
    result = tracked()
    result.add("BOGUS")
    assert "BOGUS" not in tracked()
