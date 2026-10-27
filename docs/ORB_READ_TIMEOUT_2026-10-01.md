# ORB read timeout — October 1, 2026

## Observed failure

At 13:05 CT the live `/v1/orb` four-symbol collection returned HTTP 503 /
`RUNTIME_READ_TIMEOUT` after 1.015 seconds. Immediately afterward the single NDX
route returned 200 in 0.625 seconds. Live health returned 200 in 0.016 seconds,
with advancing feed messages. This reproduces the screenshot's backend error;
it is not simply an old Streamlit error message.

ORB has one dedicated worker and constructs its requested symbols serially.
Its one-second deadline included the entire collection, while the dashboard's
HTTP timeout was 1.5 seconds. The earlier database optimization is already loaded
(`backend/database.py` startup hash equals the current file).

A separate process using the canonical `.venv`, a SQLite URI `mode=ro` connection,
and `PRAGMA query_only=ON` projected the same retained four-symbol data in 0.452
seconds cold and 0.296 seconds warm. This supports a live-load/budget problem;
it does not distinguish Python scheduling pressure from shared-lock waits.
The runtime-context getter also takes the prediction-publication lock, which can
be held through persistence. Later live health requests intermittently returned
503, so the ORB allowance is not a claim to resolve every shared runtime timeout.

## Scoped change

- `backend/api/routers/orb.py`: raise the dedicated ORB read allowance from one
  second to four seconds. Keep one worker, bounded admission, request coalescing,
  and no reuse of completed snapshots. Health and prediction budgets are unchanged.
- `app/services/orb_view.py`: allow five seconds for the HTTP response, leaving
  time for the bounded backend collection and response overhead. Explicit caller
  timeout overrides still work.
- Preserve the existing projection, partial-capture status, runtime identity
  checks, freshness rules, and 503 reporting. No ranges are inferred or backfilled.

This is a bounded timeout mitigation for demonstrated collection work. It does
not remove all underlying contention or guarantee reads complete under any load.

## Verification

**79 tests passed** across `test_health_async_isolation.py`, `test_orb_view.py`,
and `test_market_structure_orb.py`, using the canonical repository `.venv`.

The added regression has four individually sub-second projections whose total
exceeds the old deadline. Two simultaneous requests complete with the same
payload and only one set of four projections; an unrelated endpoint remains
responsive. Existing tests still prove genuine lock timeouts fail closed and
cancelled requests retain capacity until their worker exits. The dashboard test
checks that its timeout exceeds the server budget while preserving overrides.

An isolated ASGI request against today's read-only retained records returned
HTTP 200 in **0.846 seconds**. All four response payloads exactly matched the
pre-change projection at the same as-of time and runtime identity. No server,
provider connection, or application lifespan was started for that check.

`git diff --check` passed. The staged diff remains unchanged. Existing local
changes were preserved. No subscription, formula, production record, service,
or process was changed by this repair. Evidence is retained under
`output/orb_timeout_20261001/`: `baseline.json`, `orb_all.json`, `orb_ndx.json`,
`before-benchmark.json`, `before-profile.txt`, `tests.xml`,
`asgi-verification.json`, and `postconditions.json`.

## Activation still required

The running backend and dashboard have not loaded these source edits. The live
error can therefore continue until both components are activated. Preserve the
live session; use verified ownership and the canonical guarded component workflow
in the next maintenance window. On activation, verify the new source identity,
repeated four-symbol ORB responses and latency, unchanged evidence eligibility,
and fresh current-generation live health/persistence separately.

If ORB still exceeds four seconds, retain its error and investigate shared-lock
and persistence duration. Do not keep increasing deadlines or present an older
snapshot as current to suppress the message.
