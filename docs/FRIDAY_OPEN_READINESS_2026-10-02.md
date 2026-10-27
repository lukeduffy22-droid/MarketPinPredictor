# Friday October 2 opening preparation

Target: October 2, 2026 cash open, **8:30 AM America/Chicago**. Evidence below
was collected before that open. Preparation is not proof of future live capture
or forecast accuracy. Prior receipts, option tapes and forecasts were preserved.

## Changes completed

- **OBSERVED:** the independent research service and dashboard loaded the
  indicator-capture change at 12:47 AM Central. The research process loaded the
  expected source hash; options-backend PID 27484 was unchanged by activation.
  Receipt: `output/indicator_capture_20261002/20261002T054515233Z.activation.json`.
- **OBSERVED:** Thursday's delayed official closing prices were retrieved,
  validated against exact-date official artifacts, appended to the close ledger,
  and scored against the already-frozen prediction selection. The selection was
  not recreated after publication.
- **OBSERVED configuration:** the existing Windows settlement task now checks
  at **6:30 AM**, 5:05 PM, 5:20 PM and 5:45 PM Central on weekdays. Its bounded
  catch-up mode considers the last seven calendar days, prefers the newest
  pending selection, and settles at most one date per invocation. It never
  creates a hindsight selection. Valid completed receipts must match both the
  date and the frozen selection; invalid receipts block the attempt. Existing
  task credentials/settings and its IgnoreNew overlap policy were preserved.
- **OBSERVED tests:** 140 indicator tests passed before activation; 12 scoring,
  catch-up and chronological-evaluation tests passed together; the final focused
  catch-up suite passed 6 tests, including two additional receipt-binding cases.
  These counts overlap and should not be added. Three PowerShell launcher tests
  also passed. No prediction model, coefficient, threshold or OPRA subscription
  was changed by this work.

## October 1 official close comparisons

All values below are index points. These are **diagnostic research scores**,
not a promoted performance claim or a training-eligibility decision.

| Symbol | Selected forecast time, CDT | Forecast | Official close | Absolute error |
|---|---|---:|---:|---:|
| SPX | 2:50:39.138499 PM | 7,676.088875529683 | 7,666.45 | 9.638875529683 |
| NDX | 2:54:44.096091 PM | 30,507.818508860746 | 30,501.56 | 6.258508860745 |

**OBSERVED:** the earlier evening attempts failed because the SPX source still
ended at September 30. The later retry received October 1. The old source artifact
was retained rather than rewritten or accepted as current. SPX close observation
398 and NDX observation 400 match the selected predictions 14367 and 14371.

Selection SHA256:
`159c7de82d2a7344a3dfbef2098a76120308ecdb42decc061db40dbcdb942f9a`.

Evidence: `output/session_readiness_20261002/settlement-retry.txt` and
`scored-close-readback.json`; official artifacts are retained under
`data/verified_close_sources/2026-10-01/`. RUT, VIX and SPY closes were also saved.
No score was invented for symbols without a frozen eligible selection.

## Backtest finding

**OBSERVED:** a stricter chronological comparison of the frozen experimental
five-minute pin formula did not beat the last-observation baseline:

| Symbol | Test sessions | Nonoverlapping pairs | Baseline MAE | Experimental MAE |
|---|---:|---:|---:|---:|
| SPX | 8 | 195 | 3.061419 | 4.779638 |
| NDX | 11 | 263 | 17.164108 | 22.167893 |

The evaluator requires five earlier scored sessions, validates all feature and
outcome timestamps, rejects duplicate/mismatched pairs and outcomes more than
60 seconds late, and separates test intervals with a five-minute embargo. It
does not refit a model. The target is retained OPRA parity, not the official
cash-index close. The 60-second evaluation filter does not alter production
admission. Formula definitions, input hash, selected record IDs, folds and
exclusion counts are in `purged-walk-forward.json`.

**INFERRED decision:** keep this candidate in shadow. These results do not support
promoting it or promising more accurate Friday forecasts. The separate saved
EOD evaluation has only 3–7 scored cases per method; that is insufficient to
establish stable forecast performance. After close settlement, the research
reader sees 74 verified daily observations per supported symbol through October 1.

## What is and is not ready

- **OBSERVED:** Windows reported synchronized time at 12:44:47 AM Central, with
  its last successful sync about 24 seconds earlier. This host-only diagnostic
  does not certify absolute accuracy or the provider-clock gate at the open.
- **OBSERVED configuration:** autostart is enabled for 7:00, 7:15, 7:45 and
  8:15 AM; the watchdog begins at 7:50 AM. Successful future execution still
  depends on the machine, Windows session, network and provider availability.
- **OBSERVED:** the dashboard activation guard completed strict opening
  preflight in 73.6 seconds and passed its subsequent expected-fingerprint check.
  The standalone `opening-fingerprint.json` earlier reported a mismatch because
  that invocation omitted the required expected fingerprint. It does not prove
  a deployed-source defect; the guarded checks are the applicable activation evidence.
- **OBSERVED:** the market database passed its integrity check. Before the
  Friday open, the options backend correctly reported off-hours; current Friday
  forecasts were withheld rather than filled from Thursday's quotes.
- **OBSERVED blocker:** a direct live EQUS.MINI check returned
  `ENTITLEMENT_UNAVAILABLE`. Historical retrieval succeeded, but that does not
  prove current-session access. Appropriate Databento live U.S. equities access
  is required for current ETF indicators. No license was purchased.
- **NOT YET VERIFIED:** current-session indicator collection and browser display.
  The collector is enabled, but no Friday bars had arrived outside cash hours.
  Browser-control timed out, so activation was checked through service identity
  and endpoints rather than a new rendered screenshot. Indicators need at least
  35 completed, continuous, verified current-session bars, so they cannot all be
  available at the first opening instant.
- **NOT YET VERIFIED:** Friday quote/persistence advancement, fresh paired
  SPX/NDX coverage, runtime generation, full clock admission and the exact first
  ORB bucket. These require observations during Friday's session. A missed opening
  bucket remains missing; later indicator-bar recovery cannot repair that receipt.

## Evidence and ownership

Readiness receipts are in `output/session_readiness_20261002/`; indicator release
receipts are in `output/indicator_capture_20261002/`. Final receipt hashes are in
the timestamped readiness manifest. Hashes and read-only attributes make changes
detectable but are not an external WORM-storage guarantee.

Readiness source additions: `tools/evaluate_shadow_walk_forward.py`,
`tests/test_shadow_walk_forward.py`, `tests/test_postclose_catchup.py`.
Catch-up edits: `tools/run_verified_close_postclose.py`, its PowerShell launcher,
and `tools/install_verified_close_postclose_tasks.ps1`.

Separate concurrent repair work is investigating other runtime/UI failures.
This report covers the changes and evidence above; it does not certify that all
previously reported application problems have been resolved.

## Subsequent service interruption

**OBSERVED at 1:06 AM Central:** all three endpoints refused connections and the
previous listener processes were absent. This supersedes the activation receipt
as a statement about service availability at that time. The other repair chat
also observed the interruption at 1:01 AM and reported no stop/restart action.
Windows had not rebooted. The narrow event inspection found a git.exe application
hang at 12:59:54 AM, but it does not establish why the app processes ended.
Termination cause remains **NOT YET VERIFIED**. See `final-service-check.json`
and `shutdown-inspection.json`.

Coordinated recovery uses the canonical launcher after verifying the other
repair chat's final tested source hashes. A later timestamped `*.recovery.json`
receipt, if successful, supplies the restoration result; this paragraph alone
does not claim recovery has occurred.

### Restoration verified at 1:19 AM Central

**OBSERVED:** `20261002T061457610Z.recovery.json` records a successful canonical
full-stack startup at 1:16:59 AM. Subsequent `post-recovery-readback.json` confirms
all four health reads (backend, live status, research and dashboard) returned 200
at 1:19:01 AM, with off-hours admission retained. Backend listener 68596,
dashboard listener 31796 and research listener 56568 have startup ownership
receipts. All 19 combined source hashes still match disk; every source covered
by the backend's startup fingerprint also matches. This is source and endpoint
proof, not visual proof of every dashboard feature. The recorder remains under
its normal scheduled capture; it was not started off-hours.

The concurrent repair's release passed 366 regression tests with one skip,
plus focused checks. It repairs bounded retry after a database-busy publication,
mixed-type GEX table serialization, recorder callback pause/drain handling and
model-status wording. A damaged installed package was restored from a verified
cache without changing its version. See `docs/SESSION_ERROR_REPAIRS_2026-10-02.md`
and `output/session_repair_20261002/release-manifest.json` for exact scope.

The first recovery helper failed before any service change because Windows
PowerShell could not load Get-FileHash. Retrying the unchanged helper under the
installed PowerShell 7 retained its guards and succeeded. The initial three-second
diagnostic-access probe timed out during startup; one post-warmup recheck passed
authenticated HTTP 200. Both failed observations remain in the evidence.

**OBSERVED source/configuration, NOT YET VERIFIED at the open:** overnight startup
uses an explicitly labeled October 1 fallback contract scaffold. The 7:00 and
7:15 AM tasks prepare Friday's cache; the first allowed preopen connection calls
`_build_universe`, which prefers the validated current-day cache even when an
in-memory fallback already exists. Current-day promotion, provider readiness and
complete opening capture must still be observed that morning. None is inferred
from the successful overnight startup.
