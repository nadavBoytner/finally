from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable

TRACKED_SQL = """
    SELECT ticker FROM watchlist  WHERE user_id = ?
    UNION
    SELECT ticker FROM positions  WHERE user_id = ? AND quantity != 0
"""


class TrackedTickers:
    """Callable returning watchlist ∪ open positions, memoized for `ttl` seconds.

    Satisfies the Tracked protocol in interface.py. Memoized because run() calls
    it at 2 Hz and the underlying set changes only on a trade or a watchlist edit.
    """

    def __init__(
        self,
        connect: Callable[[], sqlite3.Connection],
        user_id: str = "default",
        ttl: float = 1.0,
    ) -> None:
        self._connect = connect
        self._user_id = user_id
        self._ttl = ttl
        self._cached: set[str] = set()
        self._fetched_at: float | None = None

    def __call__(self) -> set[str]:
        now = time.monotonic()
        if self._fetched_at is not None and now - self._fetched_at < self._ttl:
            return set(self._cached)
        with self._connect() as conn:
            rows = conn.execute(TRACKED_SQL, (self._user_id, self._user_id)).fetchall()
        self._cached = {str(row[0]).upper() for row in rows}
        self._fetched_at = now
        return set(self._cached)

    def invalidate(self) -> None:
        """Call after any watchlist or position change so the next tick sees it."""
        self._fetched_at = None
