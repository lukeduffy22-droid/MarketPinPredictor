# October 26 failures and October 27 opening preparation

Verified October 27, 2026 at 00:24 CT. Status: warming/off-hours. Operational fixes are installed and loaded; exact opening capture and market-data quality remain unverified until the open.

## Project and preservation

Canonical project: `C:\Cprojectsgpu_app\MarketPinPredictor`, branch `feature/2026-09-legacy-score-backlog`. The uncommitted October source was retained. The repairs changed two existing source files and added two regression files; no branch merge, commit, push, model promotion, historical backfill, or subscription purchase was performed. Pre-repair source: `source-before.zip` and `source-before-sha256.json` in this evidence directory. `repair-only.diff` isolates the repair from earlier uncommitted changes.

## Corrections

1. RUT canary fresh-quote coverage compared active primary quotes with the full selected universe, including deferred contracts. It now compares against the active subscription and expires cached freshness before checking. Existing coverage and lag limits remain unchanged.
2. The opening sampler polled once per second. During the first five-second opening bucket it now retries every 250 ms, giving eligible late-arriving evidence an opportunity to be captured before the actual deadline. It cannot backfill missing observations or admit unsynchronized clocks.
3. The manual full-stack launcher stopped venv wrapper processes before actual socket owners. An October 27 activation attempt then encountered the still-listening dashboard (PID 14232). The launcher now verifies both port owners before changing processes, stops listeners before wrappers, and rechecks creation-time identities before each stop. Tests cover unrelated owners and PID reuse. The original attempt is retained in `activation-first-attempt.log`.

## Observed activation

The canonical `start_databento_app.ps1 -NoBrowser -EnableRutCanary -Caller October26OpeningRepair` was run with administrator access. A fresh startup receipt identifies backend listener 67044 and dashboard listener 51080 with ownership verified. Both endpoints respond; all 74 startup source hashes match the current canonical files. SPX, NDX, VIX and RUT are requested. Research PID 28720 remains running.

The manual launcher initially obtained a timeout on its final diagnostic-access probe, so the external activation wrapper recorded exit 1 even though the services and startup receipt were already established. A subsequent authenticated probe returned HTTP 200; `diagnostic-access-after.json` and `final-verification.json` establish the postcondition. That first probe did not prove a backend startup failure.

The backend is in `preopen_wait` with generation zero and an uninitialized live universe. This is expected overnight. The launch receipt used a labelled October 26 fallback to prepare October 27; current-day contract discovery is still required at the scheduled pre-stage. Do not present that fallback as current market observations.

## Validation

- 375 Python cases passed across streamer, staged subscriptions, canary, clock isolation, first-bucket capture, ORB integration, recorder start gates, inference, legacy-artifact auditing and provider guards. JUnit files: `streamer-tests.xml` (227) and `readiness-tests.xml` (148).
- All three new Python regressions fail when run against the saved pre-repair methods and pass with the correction.
- 211 PowerShell cases passed, including the three new listener-stop regressions; `powershell-tests.json`.
- Canonical source/schema preflight passed with no database issues or schema bootstrap. Startup repeated it against the final repaired source.
- Main SQLite database: WAL, full read-only `PRAGMA quick_check` = `ok`; `database-integrity-full.json`. A prior 120-second bounded attempt was interrupted and is retained separately; the full check completed in 201 seconds.
- Canonical CUDA attestation passed; `cuda-attestation.json`.
- Authenticated passport access returned HTTP 200, without an AI request.

## Automatic October 27 schedule

Both enabled SYSTEM tasks point at the canonical folder and its `start_market_day.ps1 -EnableRutCanary`:

- 07:00 and 07:15 CT: clock/universe pre-stage.
- 07:45 CT: launch; 08:15 CT: another startup invocation.
- 07:50 CT onward: watchdog every five minutes during its configured window.

Read-only schedule checks classified October 27 as a regular session and returned `would_clock_and_universe_prestage` and `would_launch`. No schedule change was needed. Actual pre-stage success, current-day universe provenance, fresh per-symbol quotes, first-bucket receipts and persistence advancement must be observed in that session. Keep the PC powered and connected.

## Remaining limitations and acceptance

- October 26 first-bucket evidence showed zero provider timing samples at the initial attempt. That bucket cannot be reconstructed as an observed open. Faster retries address a scheduling opportunity; they cannot guarantee that sufficient provider data arrives within five seconds.
- October 26 had genuine opening processing lag and a queue-full warning. The canary correction removes a false coverage denominator; it intentionally retains rollback for measured core lag, invalidity or real overload. Live recurrence has not yet been ruled out.
- Late-session SPX and NDX sometimes failed minimum-pair and primary-coverage gates; VIX was thin. Those retained invalid results remain invalid. Their data-quality causes are not resolved by a startup repair.
- SPX's legacy model is incompatible and NDX's model is absent. CUDA hardware works, but trained-model inference remains disabled. The cached readiness audit reported 15/60 eligible model-evidence sessions and 0/20 prospective paper sessions; this is a cached count, not a newly verified promotion result. No validated candidate can be manufactured by relaxing these gates.
- Optional EQUS.MINI requests received an entitlement denial. OPRA remains the core feed; no entitlement or paid subscription was changed.

Before calling the October 27 session healthy, require advancing traffic and persistence, fresh valid current-generation SPX/NDX (and separately report RUT/VIX), exact opening receipt/provenance, no recurring queue loss, and a current-day universe. Forecast authority remains RESEARCH_ONLY until the model lifecycle gates are met.
