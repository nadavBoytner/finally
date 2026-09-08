from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def now_iso() -> str:
    """UTC timestamp, ISO 8601 with a Z suffix and millisecond precision."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True, slots=True)
class PriceTick:
    ticker: str        # always uppercase
    price: float       # latest known price
    prev_price: float  # the price immediately before this update
    change_pct: float  # percent change vs. the day baseline (see module docs)
    timestamp: str      # ISO 8601 UTC

    @property
    def direction(self) -> str:
        """Flash direction for the frontend: 'up', 'down', or 'flat'."""
        if self.price > self.prev_price:
            return "up"
        if self.price < self.prev_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Wire format for the SSE payload. camelCase for the TS client."""
        return {
            "ticker": self.ticker,
            "price": round(self.price, 4),
            "prevPrice": round(self.prev_price, 4),
            "changePct": round(self.change_pct, 4),
            "direction": self.direction,
            "timestamp": self.timestamp,
        }
