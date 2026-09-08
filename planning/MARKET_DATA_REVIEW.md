# Market Data Backend — Code Review

Review of `backend/app/market_data/` against its governing design docs
(`MARKET_DATA_DESIGN.md`, `MARKET_INTERFACE.md`, `MARKET_SIMULATOR.md`, `MASSIVE_API.md`)
and `PLAN.md` §5–6. Scope: `models.py`, `interface.py`, `cache.py`, `simulator.py`,
`massive_provider.py`, `tracking.py`, `factory.py`, `__init__.py`, and all six test files.

**Not yet implemented** (and therefore not reviewed, because it doesn't exist yet):
`main.py` lifespan wiring, `api/stream.py` (SSE endpoint), and `api/portfolio.py` /
`_tickers.py` (trade-path `prime()` integration) — §7–9 of `MARKET_DATA_DESIGN.md`.
`pyproject.toml` has no `fastapi`/`uvicorn` dependency yet, consistent with that. This
review covers only the `market_data` package itself, which is a complete, self-contained
unit per the design doc's own layering.

## Test run

```
uv run pytest -v
64 passed, 10 warnings in 0.99s
```

All 64 tests pass. No skips, no xfails. The 10 warnings are all the same issue (see
Finding 3 below) — nothing failed.

## Verified correct

- Every "correction" documented in `MARKET_DATA_DESIGN.md` §12 is actually present in the
  code, not just described: the quadrature correlation blend (`MARKET_BETA * market +
  sqrt(1-MARKET_BETA**2) * idio`), `math.exp` instead of a literal, the pure `gbm_step`/
  `advance` split, per-instance immutable `(price, mu, sigma)` seed tuples instead of a
  shared `dict(DEFAULT_SEEDS)`, and `update_many` for atomic per-tick batching. Each has a
  dedicated regression test (`test_market_factor_weights_preserve_unit_variance`,
  `test_instances_do_not_share_seed_state`, `test_update_many_shares_one_timestamp`, etc.).
- `PriceCache`'s "no `await` inside the critical section" invariant, which is what makes
  `get()`/`snapshot()` safely lock-free, actually holds — `_apply` is pure, and
  `update_many`'s comprehension never yields.
- I checked `parse_snapshot()` in `massive_provider.py` against the **actually installed**
  `massive` 2.8.0 SDK (not just `MASSIVE_API.md`'s description of it): `TickerSnapshot`
  really does expose `.ticker`, `.last_trade.price`, `.day.close`, and
  `.todays_change_percent` as snake_case attributes, and `get_snapshot_all` /
  `get_snapshot_ticker` really do take `(market_type, tickers=...)` /
  `(market_type, ticker=...)`. This is good — it would have been an easy place for the
  design doc's assumptions to have silently drifted from reality, and they didn't.
- Test design is genuinely careful: `SimulatorProvider` tests use a seeded `random.Random`
  for determinism, extract pure functions (`gbm_step`, `advance`) so nothing needs
  `asyncio.sleep`, and the statistical tests document *why* their tolerances are what they
  are (e.g. why events must be disabled to measure diffusion volatility). The Massive tests
  use a hand-rolled fake client rather than mocking — no network calls in CI, and the fake
  is simple enough to trust.

## Findings

### 1. (High) The Massive-provider 429 backoff path is dead code against the real SDK

`massive_provider.py`'s `_status_code()` and `_retry_after()` extract a status code and
`Retry-After` header from a caught exception via `getattr(exc, "status", None)`,
`getattr(exc, "status_code", None)`, and `getattr(exc, "response", None)`. This is designed
to detect a `429` and back off intelligently — the exact behavior `MARKET_DATA_DESIGN.md`
§12.8 calls out as a fix over the original sketch.

I traced the actual exception the installed `massive` 2.8.0 client raises
(`.venv/Lib/site-packages/massive/rest/base.py:134-135`):

```python
if resp.status != 200:
    raise BadResponse(resp.data.decode("utf-8"))
```

`BadResponse` (`massive/exceptions.py`) is a bare `Exception` subclass with no extra
attributes — just the decoded response body as its message string. It carries no
`.status`, `.status_code`, or `.response`. So for **every** real HTTP error from Massive,
including a genuine `429`, `_status_code(exc)` returns `None`, the `if _status_code(exc) ==
429:` branch in `run()` never taken, and the code falls into the generic
`except Exception` path — logs `"Massive poll failed; retrying next cycle"` and sleeps the
normal `poll_seconds` interval. The custom backoff (honor `Retry-After`, else double up to
`MAX_BACKOFF_SECONDS`) never runs.

Two mitigating facts, so this isn't a crash risk:
- The SDK's own `urllib3.Retry` strategy already retries `429`/`5xx` up to 3 times
  internally (`backoff_factor=0.1`, so ~0.1s/0.2s/0.4s) before `BadResponse` is ever raised
  — so there is *some* backoff, just much shorter and not visible to/tunable by this code.
- The generic fallback (log + retry at normal cadence) is safe — it doesn't hammer the API
  faster than `poll_seconds`, so this isn't going to cause a demo to break.

But the tests that exercise this path (`test_run_honors_retry_after_on_429`,
`test_run_backs_off_without_retry_after_and_keeps_retrying`,
`test_run_resets_backoff_after_a_success`) construct synthetic exceptions
(`RateLimited`, `RateLimitedNoRetryAfter`, `QuickRateLimited`) with a `status_code` and
`response.headers` that the real SDK never produces. All three pass, but they're testing a
contract the real dependency doesn't implement — green tests, wrong behavior against the
live API. If Massive-mode rate-limit resilience matters for the demo, either:
- parse the status code out of `BadResponse`'s message text (fragile — it's the raw
  response body, not a structured error), or
- wrap the `get_snapshot_all`/`get_snapshot_ticker` calls to catch `BadResponse` and inspect
  it, accepting that today's `BadResponse` gives you nothing to distinguish a 429 from a
  400 or a 500 without a body-content heuristic, or
- accept the generic-retry fallback as the real behavior and delete/simplify the 429-specific
  branch and its tests so they don't imply a guarantee that isn't kept.

I'd lean toward the third option — the generic fallback is already safe, and the 429-specific
code adds real complexity (backoff state, `_status_code`, `_retry_after`, three tests) for a
path that can't currently be reached.

### 2. (Medium, forward-looking) `TrackedTickers` never closes the connection it opens

`tracking.py`:

```python
with self._connect() as conn:
    rows = conn.execute(TRACKED_SQL, (self._user_id, self._user_id)).fetchall()
```

`sqlite3.Connection.__exit__` commits or rolls back the transaction — it does **not** close
the connection (this is a common sqlite3 gotcha; `with conn:` ≠ `with contextlib.closing(conn):`).
That's harmless with the test fixture, which hands `TrackedTickers` the *same* long-lived
connection every call. But `connect: Callable[[], sqlite3.Connection]` is typed as a
zero-arg factory, and `MARKET_DATA_DESIGN.md` §7.3's lifespan wiring passes the bare
`connect` function from the (not-yet-written) db layer — which, per every convention shown
in `PLAN.md`, opens a **new** connection per call. Given `TrackedTickers.__call__` runs on a
1-second TTL for the life of the process, that's a new, never-explicitly-closed
`sqlite3.Connection` roughly once a second, for as long as the container runs. CPython's
refcounting GC will eventually collect and close abandoned connections, but relying on GC
timing for OS file-descriptor cleanup in a long-running server process is exactly the kind
of thing that's fine in a two-hour demo and a slow leak in anything longer.

This isn't a bug in the code that exists today — `market_data` has no control over how
`connect()` will be implemented — but it's a contract worth flagging now, before the db
layer is built: either `connect()` should return a shared/pooled connection, or
`TrackedTickers` should own the close (`try: ... finally: conn.close()`, or
`contextlib.closing`).

### 3. (Low) Ten `PytestWarning`s from a misapplied module-level marker

`test_massive_provider.py` sets `pytestmark = pytest.mark.asyncio` once at module scope,
but roughly a third of the tests in that file (`test_poll_seconds_defaults_when_not_given`,
all the `test_parse_*` tests, etc.) are plain synchronous functions. Under
`asyncio_mode = "strict"` this produces a `PytestWarning` per sync test — 10 in the current
run — though nothing fails. Cleanest fix: drop the module-level `pytestmark` and mark only
the `async def` tests individually (or split the file into sync-parsing vs. async-run
sections, each with its own marker scope).

### 4. (Low) A partial chunk failure in `MassiveProvider._fetch` discards already-fetched rows

When the tracked set exceeds `MAX_TICKERS_PER_REQUEST` (100), `_fetch` loops over chunks and
awaits each `get_snapshot_all` in turn. If chunk 2 of 2 raises, the rows already parsed from
chunk 1 are thrown away with the exception — `run()` writes nothing to the cache that cycle,
even though it had good data for the first 100 tickers. Given the project's 10-default-ticker
scale this is very unlikely to matter in practice (chunking only engages past 100 tracked
symbols), but if it's worth fixing: accumulate rows across chunks and write what succeeded
via `cache.update_many` even if a later chunk fails, instead of losing the whole cycle.

### 5. (Info, not a defect) No lint or type-check config

There's no `ruff`/`mypy` config in `pyproject.toml` yet. Not a correctness issue — the code
is consistently typed and styled by hand — just noting it's absent if the project wants CI
gating on it later.

## Verdict

The `market_data` package is a faithful, careful implementation of its design doc — the
kind of design doc that documents its own prior mistakes (§12) and has every one of those
fixes actually landed and regression-tested in the code. All 64 tests pass, the simulator's
math checks out (verified independently, not just "tests pass"), and the Massive
integration was checked against the real installed SDK rather than taken on faith.

The one real bug (**Finding 1**) is that the Massive-provider's rate-limit-specific backoff
never activates against the real `massive` client, because that client's exceptions don't
carry the `status_code`/`response` shape the code (and its tests, via synthetic exception
classes) assume. It degrades safely rather than crashing, but it's dead code that
its own test suite doesn't actually catch, because the tests mock a contract the dependency
doesn't implement. Worth a decision — fix the detection, or drop the special-cased path —
before treating Massive-mode resilience as covered.

**Finding 2** is a heads-up for whoever writes `backend/app/db.py` next, not a bug in this
package.
