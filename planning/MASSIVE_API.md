# Massive API Research

Research notes on [Massive](https://massive.com) (the rebrand of Polygon.io, effective October 30, 2025) for retrieving real-time and end-of-day stock prices for multiple tickers. This document is the factual reference; [`MARKET_INTERFACE.md`](./MARKET_INTERFACE.md) uses it to design this project's unified market-data API.

## 1. What "Massive" is

Massive.com is Polygon.io under a new name. Per Massive's own migration notes: existing API keys, accounts, and the `polygon-io/client-python` SDK all continue to work unchanged. The only practical differences:

- New base URL `api.massive.com` (old `api.polygon.io` still works "for an extended period")
- New official Python package `massive` (old `polygon-api-client` still installable)
- Docs now live at `massive.com/docs` instead of `polygon.io/docs`

Everything below uses the new Massive naming, but is identical in behavior to Polygon.io if any pre-rebrand material is encountered elsewhere.

## 2. Authentication

Two equivalent methods, both accepted on every REST endpoint:

```
GET https://api.massive.com/v2/aggs/ticker/AAPL/prev?apiKey=YOUR_API_KEY
```

```
GET https://api.massive.com/v2/aggs/ticker/AAPL/prev HTTP/1.1
Host: api.massive.com
Authorization: Bearer YOUR_API_KEY
```

The official Python client (`pip install massive`, `from massive import RESTClient`) picks up the key automatically from the **`MASSIVE_API_KEY`** environment variable if `api_key` isn't passed explicitly to the constructor — this matches the env var name already specified in `PLAN.md` §5, so no translation layer is needed.

## 3. Plan tiers — the critical constraint

This is the single most important finding for this project: **the free tier cannot do real-time or delayed streaming; it is end-of-day only, and it has no access to the snapshot endpoints at all.**

| Plan | Price | Rate limit | Data recency | Snapshot endpoints |
|---|---|---|---|---|
| **Basic (free)** | $0/mo | 5 calls/min | **End of day only** | ❌ Not available |
| **Starter** | $29/mo | Unlimited | 15-minute delayed | ✅ Available |
| **Developer** | $79/mo | Unlimited | 15-minute delayed | ✅ Available, + trades data |
| **Advanced** | $199/mo | Unlimited | **Real-time** | ✅ Available |

Aggregate/bar endpoints (previous close, custom bars, grouped daily, open/close) are included on **every** tier, including free — but on the free tier they only ever return data as of the last completed trading day. The snapshot endpoints (the ones that return a live last-trade/last-quote/current-day bar) require Starter or above.

**Implication for FinAlly:** `PLAN.md` §6 currently states "Free tier (5 calls/min): poll every 15 seconds," implying the free tier can be polled for near-live prices. It cannot — a free-tier key can only ever serve stale, previous-close data through this project's live-streaming SSE architecture. If a student runs with a free-tier `MASSIVE_API_KEY`, they will get authorization/plan errors from the snapshot endpoint, not slow-but-working data. [`MARKET_INTERFACE.md`](./MARKET_INTERFACE.md) documents the fallback behavior. Practically: Massive mode needs at minimum a **Starter ($29/mo)** key; anything less should fall back to the simulator.

## 4. Endpoints relevant to this project

### 4.1 Full Market Snapshot (multi-ticker) — the primary endpoint for this project

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,TSLA,GOOGL&apiKey=YOUR_API_KEY
```

| Param | Type | Notes |
|---|---|---|
| `tickers` | string | Comma-separated, case-insensitive. Omit to get all 10,000+ tickers (don't — always pass the watched/held set). |
| `include_otc` | bool | Default `false`. |

Response — one entry per requested ticker under `tickers[]`:

```json
{
  "status": "OK",
  "count": 2,
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": 0.98,
      "todaysChangePerc": 0.82,
      "updated": 1605195918306274000,
      "day":     { "o": 119.62, "h": 120.53, "l": 118.81, "c": 120.4229, "v": 28727868, "vw": 119.725 },
      "prevDay": { "o": 117.19, "h": 119.63, "l": 116.44, "c": 119.49,   "v": 110597265, "vw": 118.4998 },
      "min":     { "o": 120.435, "h": 120.468, "l": 120.37, "c": 120.4201, "v": 270796, "t": 1684428720000 },
      "lastTrade": { "p": 120.47, "s": 236, "t": 1605195918306274000 },
      "lastQuote": { "p": 120.46, "P": 120.47, "s": 8, "S": 4, "t": 1605195918507251700 }
    }
  ]
}
```

For a live price to display, `lastTrade.p` is the most recent traded price; `day.c` is the running current-day bar close (also usable, updates less granularly). `todaysChangePerc` is ready-made for a "daily change %" field, sourced from `prevDay.c` — directly useful for the open question flagged in `PLAN.md` §13/`REVIEW.md` about how daily change % should be computed.

Snapshot data resets daily at 3:30 AM ET and repopulates from ~4:00 AM ET onward, so pre-market snapshot calls can return yesterday's frozen values.

**Python (official client):**

```python
from massive import RESTClient

client = RESTClient()  # reads MASSIVE_API_KEY from env

snapshot = client.get_snapshot_all("stocks", tickers=["AAPL", "TSLA", "GOOGL"])
for t in snapshot:
    print(t.ticker, t.last_trade.price, t.todays_change_percent)
```

### 4.2 Single Ticker Snapshot

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers/AAPL?apiKey=YOUR_API_KEY
```

Same shape as one entry from §4.1, nested under `"ticker"`. Useful for priming a single newly-added or newly-traded symbol without re-fetching the whole watchlist.

```python
snap = client.get_snapshot_ticker("stocks", ticker="AAPL")
print(snap.last_trade.price)
```

### 4.3 Previous Day Bar (EOD) — works on every plan including free

```
GET https://api.massive.com/v2/aggs/ticker/AAPL/prev?adjusted=true&apiKey=YOUR_API_KEY
```

```json
{
  "ticker": "AAPL",
  "resultsCount": 1,
  "results": [
    { "T": "AAPL", "o": 115.55, "h": 117.59, "l": 114.13, "c": 115.97, "v": 131704427, "vw": 116.3058, "t": 1605042000000 }
  ]
}
```

```python
prev = client.get_previous_close_agg(ticker="AAPL")
```

### 4.4 Custom Bars (historical aggregates)

```
GET https://api.massive.com/v2/aggs/ticker/AAPL/range/1/day/2026-08-01/2026-09-01?adjusted=true&sort=asc&limit=5000&apiKey=YOUR_API_KEY
```

Path: `{ticker}/range/{multiplier}/{timespan}/{from}/{to}`, where `timespan` is one of `minute|hour|day|week|month|quarter|year`. Max 50,000 bars per request; paginates via `next_url`.

```python
for bar in client.list_aggs(ticker="AAPL", multiplier=1, timespan="day",
                             from_="2026-08-01", to="2026-09-01", limit=5000):
    print(bar.timestamp, bar.close)
```

Not used by this project (per `PLAN.md` §10, there is deliberately no historical-price REST path — charts are SSE-accumulated only), but documented here since it's the standard way to backfill EOD history if that scope ever changes.

### 4.5 Daily Market Summary (grouped, all tickers for one date)

```
GET https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-09-05?adjusted=true&apiKey=YOUR_API_KEY
```

Returns every ticker's OHLC for a single trading day in one call (`results[]`, keyed by `T`). Efficient for a free-tier EOD-only use case since it's one call regardless of how many tickers are watched — but not useful for streaming since it only returns a full day's data.

## 5. Rate limits & errors

- **Free tier**: hard 5 requests/minute; a 6th request in the same minute is rejected.
- **Paid tiers**: no fixed cap, but Massive asks clients to stay under ~100 req/sec.
- On over-limit: standard HTTP `429 Too Many Requests`, optionally with a `Retry-After` header — treat this like any standard REST 429 (backoff and retry).
- On a bad/missing API key: an authorization error is returned (4xx) rather than empty data — this should surface as a startup failure, not a silent empty watchlist.
- On a plan-tier violation (e.g., calling the snapshot endpoint on a free key): also a 4xx authorization-style error. Design implication used in `MARKET_INTERFACE.md`: a startup probe call is required to detect this case early, since it's indistinguishable from "bad key" without inspecting the response.
- An invalid/unlisted ticker symbol is **not** an error — it simply doesn't appear in the response (or `resultsCount: 0`), matching the "unavailable" behavior `PLAN.md` §6 specifies for unlisted symbols in Massive mode.

## 6. Recommendation for this project

1. Use `get_snapshot_all("stocks", tickers=[...])` on a poll interval as the sole ongoing data path — it returns everything needed (current price, previous price via `prevDay.c`, timestamp, daily change %) in one call per tick, for any number of tickers.
2. Poll interval should be plan-aware, not the flat 15s the current plan text implies: 15s only makes sense for Starter/Developer (15-min-delayed data doesn't get fresher by polling faster); Advanced can poll every 2-5s for genuinely live movement. Free tier should not be polled for streaming at all (see §3).
3. Do one snapshot call at startup to fail fast with a clear error if the key is invalid or the plan doesn't support snapshots, rather than surfacing that failure silently mid-stream.
4. `get_snapshot_ticker` (single-ticker) is the natural on-demand call for priming a brand-new symbol (e.g. a trade on a not-yet-watched ticker) without waiting for the next full poll.

Sources: [massive.com/docs](https://massive.com/docs), [Stocks REST overview](https://massive.com/docs/rest/stocks/overview), [Full Market Snapshot](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot), [Single Ticker Snapshot](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot), [Previous Day Bar](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar), [Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars), [Daily Market Summary](https://massive.com/docs/rest/stocks/aggregates/daily-market-summary), [Pricing](https://massive.com/pricing), [Rate limit KB article](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis), [Polygon.io is now Massive](https://massive.com/blog/polygon-is-now-massive), [massive-com/client-python](https://github.com/massive-com/client-python).
