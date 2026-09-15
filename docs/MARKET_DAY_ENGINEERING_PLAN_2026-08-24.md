# MarketPinPredictor Market-Day Engineering Plan

Date: 2026-08-24
Operating timezone: America/Chicago
Primary runtime: the canonical local MarketPinPredictor checkout

## Mission

Operational update (2026-09-14): before each future session, review the
[failure and correction register](FAILURE_AND_CORRECTION_REGISTER.md) and complete
the [premarket/opening evidence checklist](PREMARKET_EVIDENCE_CHECKLIST.md).
These newer incident-specific checks supplement the historical plan below;
documented corrections are not considered deployed or verified without evidence.

Keep the live SPX/NDX prediction system observable, auditable, and recoverable throughout the market session. Prefer trustworthy incomplete output over plausible-looking fabricated output. Never silently replace invalid regular-hours Databento data with historical or proxy values.

The system must collect enough raw and derived evidence to answer four questions later:

1. What data did the model receive?
2. What exact formulas and parameters produced the pin and close target?
3. Was a wrong-looking value stored incorrectly, or was the UI stale/misrendered?
4. What change would have improved the prediction without hindsight leakage?

## Recovered cloud checkpoint

The shared ChatGPT link contains this transfer task and early retrieval messages, not the missing deep planning discussion. The durable work was recovered from the Visual Studio cloud-agent Git checkpoints instead:

- `53528d6ddee9fc79c635db73518fc7f10f2772a6` — first recovered VS Code cloud-agent checkpoint.
- `6a066b56a8c1047af72280321c44a88bf1b3fc22` — current cloud-agent checkpoint branch base.
- Active branch at recovery: `copilot/vscode-mt7dg09i-b1zg`.

The recovery preserved runtime source, tests, watchdog scripts, the same-day full OPRA universe cache, and the live SQLite database/WAL. No database restore or destructive Git reset was used.

## Current live architecture

```text
Databento OPRA cbbo-1s
        |
        v
latency-sensitive record consumer
        |-- O(1) per-root handoff tracking
        |-- quote cache + generation tag
        |
        +--> dedicated single compute worker
                  |-- same-expiration put/call parity spot
                  |-- IV / gamma / GEX calculations
                  |-- invariant validation
                  |-- audit JSON + NDJSON
                  |-- SQLite WAL persistence
                  +-- API / dashboard cache

FastAPI :8000  ---> Streamlit :8501 (SSE live status)
```

The live subscription profile is `near-term-shadow`:

- Full cached universe: 14,234 OI-positive contracts.
- Selected live universe: 2,534 contracts (82.2% reduction).
- Primary: 976 contracts — SPX 0DTE 405, NDX 0DTE 494, VIX nearest 77.
- Shadow: 1,558 contracts — SPX/NDX 1–3 DTE and 4–45 DTE representatives plus the next VIX expiry.
- The corrected near-expiration blend remains shadow-only. `DATABENTO_USE_MULTI_EXPIRATION=0` prevents promotion without evidence.

## Reliability corrections applied today

1. Restored the same-day universe cache before restart so discovery did not block market-open data.
2. Disabled replay on normal restart to prevent historical backlog from starving the live feed.
3. Bounded the live universe to complete selected expirations rather than truncating arbitrary strikes or option legs.
4. Interleaved selected markets so SPX, NDX, and VIX mapping/quotes are not ordered as one giant root block.
5. Replaced the per-record full quote-cache rescan with generation-local root tracking.
6. Moved gamma calculation, callbacks, file output, and database persistence out of the Databento iterator thread.
7. Added a subscription-generation guard so a reconnect cannot publish a calculation assembled across generations.
8. Kept launcher restarts component-specific and made the full launcher refuse to kill an unrelated owner of ports 8000 or 8501.
9. Preserved invalid diagnostics while preventing invalid zero rows from overwriting valid gamma buckets.
10. Persisted market snapshots and full gamma inputs for replay/backtest evidence.

## Market-hours acceptance gates

Do not call the session healthy based on open ports alone. All of the following must hold:

- FastAPI `/health` responds on port 8000.
- Streamlit `/_stcore/health` responds on port 8501.
- `websocket=active`, `handoff_status=active`, and `stream_progressing=true`.
- `messages_received` advances across samples.
- Fresh quote counts advance for SPX and NDX; VIX is monitored separately because sparse paired quotes may keep it invalid.
- `data_age_seconds` remains within the configured freshness envelope.
- No `skipped_records_after_slow_reading`, reconnect loop, traceback, or recurring queue growth is present in the current backend log.
- Latest SPX and NDX audit snapshots are valid, nonzero, and newer than the previous minute.
- GEX invariants hold:
  - `gross_gex >= abs(net_gex)`
  - `gross_gex = call_gex_total + put_gex_total`
  - `net_gex = call_gex_total - put_gex_total`
- SQLite is in WAL mode and `PRAGMA quick_check` returns `ok`.
- Persistence rows and timestamps advance for:
  - `gamma_calculation_runs`
  - `gamma_calculation_input_blobs`
  - `gamma_audit_snapshots`
  - `gamma_pin_snapshots`
  - `prediction_snapshots`
  - `market_snapshots`

## UI-versus-backend reconciliation

When the dashboard looks wrong, capture these three timestamps/values before changing code:

1. The UI-displayed `_source_file` and generated timestamp.
2. The latest audit JSON for the same symbol.
3. The latest SQLite row timestamp and values.

Diagnosis rules:

- UI source timestamp older than backend/audit timestamp: **stale UI/session**.
- Audit JSON and SQLite agree but UI differs at the same timestamp: **display/cache bug**.
- Audit JSON and SQLite disagree: **persistence serialization or transaction bug**.
- Audit and SQLite both contain zeros/invalid reasons: **backend/feed/formula gate**, not a UI bug.

Today’s proof case: Chrome showed an old NDX audit (`logs\audit\NDX\20260824-151247.json`) as invalid while the backend had already written a newer valid NDX audit. One necessary reload aligned the UI with the backend. No formula fallback was used.

## Five-minute anomaly review

For each new five-minute window, record concise evidence for:

- gamma-pin movement and abrupt spot/pin jumps;
- quote/data age and missing root progression;
- sign, finite-value, and GEX invariant failures;
- paired-quote/IV/gamma rejection counts;
- thin-chain and concentration gates;
- missing full-input captures;
- duplicate timestamps or persistence lag;
- suspicious zero/null rates;
- same-day target versus shadow multi-expiration target;
- queue-full, skipped-record, disconnect, or reconnect events.

Safe market-hours corrective actions are limited to restarting a verified failed component, restoring a known same-day cache, reducing a proven hot path, or isolating an invalid symbol. Avoid broad model changes while live.

## Screenshot and evidence retention

- Rendered dashboard PNGs: `exports\dashboard_screenshots\2026-08-24\`
- Per-symbol audit JSON: `logs\audit\{SPX,NDX,VIX}\`
- Runtime logs: `logs\runtime\`
- Full calculation inputs and outputs: SQLite plus audit/NDJSON exports.
- Do not force a page reload solely to take a screenshot; reload once only when evidence proves the session is stale.

## Final 30 minutes and post-close

During the final 30 minutes, report the most defensible analytical estimates for SPX and NDX with:

- current spot;
- primary gamma pin;
- max pain;
- same-day target;
- shadow multi-expiration target;
- model confidence and explicit data-quality caveats.

State clearly that the output is an analytical estimate, not guaranteed financial advice.

After the official close is available, produce a compact recap containing prediction error, anomaly log, pin-movement summary, persistence totals, screenshot count, and prioritized follow-ups.

## Prioritized engineering backlog

### P0 — session integrity

- Continue the live watch through close and alert on skipped records, reconnects, stale SPX/NDX quotes, invariant failures, or persistence stalls.
- Keep VIX invalid when its chain is too thin; do not let it silently poison SPX/NDX.
- Add a formal startup-burst/staged-subscription test and measured queue-depth telemetry.

### P1 — backtesting and auditability

- Build deterministic replay from `gamma_calculation_input_blobs` using the stored universe hash, parameters, rejection counts, and formula versions.
- Add reconciliation tests across valid/invalid audit JSON, SQLite rows, and dashboard payloads.
- Add duplicate-timestamp, null-rate, and write-lag checks to the watchdog.
- Record official closes and calculate point/percentage error for each prediction mode.

### P2 — formula and model improvement

- Compare same-day versus near-expiration shadow targets over enough sessions to justify or reject promotion.
- Evaluate pin concentration, distance buckets, dispersion, volatility regime, and chain-thickness effects without hindsight leakage.
- Separate per-symbol formula-health ratios so a known-sparse VIX chain does not obscure healthy SPX/NDX calculations.
- Add calibrated confidence based on data quality and historical out-of-sample error rather than presentation heuristics.

### P3 — operator experience

- Add an explicit UI badge showing audit timestamp, backend timestamp, and session freshness.
- Add a one-click evidence bundle containing screenshot, health JSON, latest audits, and persistence counters.
- Surface the exact invalid reason and recovery action without showing zero GEX as a valid invariant success.

## Verified recovery snapshot

At 10:44:47 America/Chicago after the final compute-worker restart:

- Backend PID 57368; dashboard PID 45792.
- 376,450 live messages received and advancing.
- Fresh quotes: SPX 1,005; NDX 759; VIX 71.
- Valid: SPX, NDX. Invalid: VIX (thin/insufficient paired quotes).
- Data age: 1.18 seconds.
- Reconnects: 0; last error: none.
- No skipped-record event after the restart; only one bounded startup mapping backlog warning.
- SQLite: WAL mode, `quick_check=ok`.

This snapshot is evidence of readiness at that time, not a guarantee of future market data or prediction accuracy.
