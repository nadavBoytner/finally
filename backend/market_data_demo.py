"""Live terminal demo of the market data module.

Renders the 10 default tickers as a live-refreshing table with:
  - a colored up/down/flat arrow for the latest tick
  - a Unicode sparkline built from in-process price history
  - a scrolling event log of outsized single-tick moves

Uses `SimulatorProvider` + `PriceCache` directly — no database, no HTTP
server, no .env required. Run it from the `backend/` directory:

    uv run python market_data_demo.py
    uv run python market_data_demo.py --tickers AAPL,TSLA,NVDA --interval 0.25
    uv run python market_data_demo.py --iterations 20 --no-color   # non-interactive

Press Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import deque
from datetime import datetime

from app.market_data import PriceCache, PriceTick
from app.market_data.simulator import DEFAULT_SEEDS, SimulatorProvider

DEFAULT_TICKERS = list(DEFAULT_SEEDS.keys())
DEFAULT_INTERVAL_SECONDS = 0.5
DEFAULT_HISTORY_LEN = 40
DEFAULT_EVENT_THRESHOLD_PCT = 1.5
EVENT_LOG_LEN = 8

SPARK_LEVELS = " ▁▂▃▄▅▆▇█"

CLEAR_SCREEN = "\033[2J\033[H"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"


class Palette:
    """ANSI colors, or empty strings when color is disabled."""

    def __init__(self, enabled: bool) -> None:
        self.green = "\033[32m" if enabled else ""
        self.red = "\033[31m" if enabled else ""
        self.yellow = "\033[33m" if enabled else ""
        self.gray = "\033[90m" if enabled else ""
        self.bold = "\033[1m" if enabled else ""
        self.reset = "\033[0m" if enabled else ""


def sparkline(history: deque[float]) -> str:
    if len(history) < 2:
        return SPARK_LEVELS[0] * len(history)
    lo, hi = min(history), max(history)
    if hi == lo:
        mid = SPARK_LEVELS[len(SPARK_LEVELS) // 2]
        return mid * len(history)
    span = hi - lo
    scale = len(SPARK_LEVELS) - 1
    return "".join(
        SPARK_LEVELS[round((value - lo) / span * scale)] for value in history
    )


def arrow_and_color(tick: PriceTick, palette: Palette) -> tuple[str, str]:
    if tick.direction == "up":
        return "▲", palette.green
    if tick.direction == "down":
        return "▼", palette.red
    return "→", palette.gray


def tick_move_pct(tick: PriceTick) -> float:
    if tick.prev_price == 0:
        return 0.0
    return (tick.price - tick.prev_price) / tick.prev_price * 100.0


def format_row(ticker: str, tick: PriceTick | None, history: deque[float], palette: Palette) -> str:
    if tick is None:
        return f"  {ticker:<6} {palette.gray}-- waiting for data --{palette.reset}"
    arrow, color = arrow_and_color(tick, palette)
    spark = sparkline(history)
    return (
        f"  {ticker:<6} "
        f"{color}{tick.price:>10.2f}{palette.reset}  "
        f"{color}{arrow} {tick.change_pct:+6.2f}%{palette.reset}  "
        f"{palette.bold}{spark}{palette.reset}"
    )


def format_event(ticker: str, tick: PriceTick, move_pct: float, palette: Palette) -> str:
    arrow, color = arrow_and_color(tick, palette)
    hhmmss = tick.timestamp.split("T")[-1].rstrip("Z")
    return (
        f"  {palette.gray}{hhmmss}{palette.reset}  "
        f"{color}{ticker:<6} {arrow} {move_pct:+.2f}%{palette.reset}  "
        f"jump to {tick.price:.2f}"
    )


def render(
    tickers: list[str],
    ticks: dict[str, PriceTick],
    history: dict[str, deque[float]],
    events: deque[str],
    palette: Palette,
    clear: bool,
) -> None:
    lines: list[str] = []
    if clear:
        lines.append(CLEAR_SCREEN)
    now = datetime.now().strftime("%H:%M:%S")
    lines.append(f"{palette.bold}FinAlly — Market Data Demo (simulator){palette.reset}   {now}")
    lines.append("")
    lines.append(f"  {'TICKER':<6} {'PRICE':>10}  {'CHG':>9}   SPARKLINE")
    lines.append("  " + "-" * 60)
    for ticker in tickers:
        lines.append(format_row(ticker, ticks.get(ticker), history[ticker], palette))
    lines.append("")
    lines.append(f"{palette.bold}Event Log{palette.reset} {palette.gray}(moves ≥ threshold, newest first){palette.reset}")
    if events:
        lines.extend(events)
    else:
        lines.append(f"  {palette.gray}-- no notable moves yet --{palette.reset}")
    sys.stdout.write("\n".join(lines) + "\n")
    sys.stdout.flush()


async def run_demo(
    tickers: list[str],
    interval: float,
    history_len: int,
    event_threshold_pct: float,
    iterations: int | None,
    palette: Palette,
    clear: bool,
) -> None:
    cache = PriceCache()
    provider = SimulatorProvider(tick_seconds=interval)
    history: dict[str, deque[float]] = {t: deque(maxlen=history_len) for t in tickers}
    events: deque[str] = deque(maxlen=EVENT_LOG_LEN)

    for ticker in tickers:
        await provider.prime(cache, ticker)

    count = 0
    while iterations is None or count < iterations:
        rows = provider.advance(tickers)
        ticks = {tick.ticker: tick for tick in await cache.update_many(rows)}
        for ticker, tick in ticks.items():
            history[ticker].append(tick.price)
            move_pct = tick_move_pct(tick)
            if abs(move_pct) >= event_threshold_pct:
                events.appendleft(format_event(ticker, tick, move_pct, palette))

        render(tickers, ticks, history, events, palette, clear)
        count += 1
        if iterations is None or count < iterations:
            await asyncio.sleep(interval)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers to watch (default: the 10 FinAlly default tickers)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Seconds between ticks (default: {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--history",
        type=int,
        default=DEFAULT_HISTORY_LEN,
        help=f"Sparkline history length in ticks (default: {DEFAULT_HISTORY_LEN})",
    )
    parser.add_argument(
        "--event-threshold",
        type=float,
        default=DEFAULT_EVENT_THRESHOLD_PCT,
        help=f"Minimum abs single-tick %% move logged as an event (default: {DEFAULT_EVENT_THRESHOLD_PCT})",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Run this many ticks then exit, instead of running forever (useful for CI/non-interactive runs)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors (also auto-disabled when stdout is not a TTY)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    tickers = (
        [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if args.tickers
        else DEFAULT_TICKERS
    )
    color_enabled = sys.stdout.isatty() and not args.no_color
    palette = Palette(enabled=color_enabled)
    clear = sys.stdout.isatty()

    if clear:
        sys.stdout.write(HIDE_CURSOR)
        sys.stdout.flush()
    try:
        asyncio.run(
            run_demo(
                tickers=tickers,
                interval=args.interval,
                history_len=args.history,
                event_threshold_pct=args.event_threshold,
                iterations=args.iterations,
                palette=palette,
                clear=clear,
            )
        )
    except KeyboardInterrupt:
        pass
    finally:
        if clear:
            sys.stdout.write(SHOW_CURSOR)
            sys.stdout.flush()


if __name__ == "__main__":
    main()
