# Friday live repair: tested release, activation pending

Recorded October 2, 2026, approximately 1:43 PM America/Chicago.

## Implemented and tested

1. Removed the redundant cash-calendar/stall check immediately after an accepted
   quote advances the reader's progress clock. Control records and the independent
   watchdog retain session-aware stall detection. Runtime Mapping checks now use
   collections.abc instead of the typing wrapper. No quote acceptance policy changed.
2. A same-day preopen `warming` handoff alone no longer triggers the supervisor's
   readiness restart. Invalid identity, failed endpoints, dead sampler and degraded
   handoff still fail their checks. At cash open the active-handoff requirement
   remains. The separate prior-day source refresh remains mandatory. This does not
   establish that every restart in the morning report was erroneous.
3. Structural estimates retain a separately bound, already-journaled committed
   calculation across intervening transient quote revisions. The original timestamp,
   calculation ID, model inputs/lean, content hash and journal receipt are unchanged.
   Reads independently expire it at the existing 30-second freshness limit and
   withhold it on changed runtime, stale/future time, altered content or invalid
   parent prediction. A transient quote never receives an earlier calculation ID.
   The UI explains the distinct saved-calculation cadence.

Models, coefficients, freshness/coverage thresholds, subscription configuration and
historical records were not changed. The saved-input cadence was not increased.
Availability between captures improves only while the committed estimate remains
fresh; a number is still withheld afterward.

## Observed validation

- 176 reader, timing and publication test executions passed.
- 171 structural, workstation and publication test executions passed, including
  independent expiry while the parent is fresh, runtime changes, invalid inputs,
  content alteration and no duplicate journal observation.
- 134 provider, health, UI and staged-subscription test executions passed.
- 167 PowerShell supervisor checks passed.

Some test files overlap between groups; these are executions, not unique-test counts.
Tests used an absolute temporary database, not the production database.

The profiled single-instrument 100,000-record experiment measured 8.589 seconds
before and 4.192 after. A second, unprofiled 1,902-instrument fixture ran three
alternating before/after comparisons: before 1.559–2.940 seconds, after 1.422–1.528
seconds; medians 2.899 and 1.492 seconds. Each run accepted all 100,000 synthetic
records with one generation and no reconnect. The first experiment's temporary
directory cleanup failed because its current directory was still open; its timing
receipt was retained. The second experiment corrected cleanup and isolated SDK
teardown from the consumer measurement.

These are OBSERVED isolated measurements. They do not identify every cause of the
morning outage, demonstrate real SDK queue capacity, or include competing live
calculation/database work. No fixture contacted a provider or entered live evidence.

## Live evidence before activation

OBSERVED, 1:35:00–1:36:15 PM CT: generation remained 53, messages advanced
4,525,128 → 4,554,994, reconnects remained 165, skipped-record warnings remained
174, queue-full warnings remained 75. All 12 sampled SPX/NDX prediction responses
returned 200. The queue counter had increased from 74 earlier in the afternoon;
the session was not continuously loss/pressure-free.

Committed calculations advanced independently:

| Symbol | Calculation row before → after | Saved prediction row before → after |
|---|---|---|
| SPX | 24010 → 24013 | 14684 → 14686 |
| NDX | 24011 → 24014 | 14685 → 14687 |

OBSERVED at 1:41:23 PM CT: same generation 53, 4,675,780 messages, unchanged
reconnect/loss counters. SPX/NDX were valid, current and usable, with source ages
12.765/13.812 seconds. The existing runtime produced these research-only estimates:

| Symbol | Forecast timestamp CT | EOD estimate | Forecast ID |
|---|---|---:|---|
| SPX | 13:41:19.998715 | 7703.424633343399 | 7c351367c92e58edca62ceab7b09a3f7843116fe837057b59a40e7007249ffa0 |
| NDX | 13:41:20.018640 | 30661.397503471355 | bcd85ecc58ca1ca0c3ff0f39c491375b349f15cc39c2ab95e3c9f2557c9f7a6d |

These estimates preceded activation and are not improvements attributable to this
release. Accuracy remains NOT YET VERIFIED against today's official closes.

## Activation status and next step

NOT ACTIVATED. Backend PID 53028 runs under SYSTEM; the calling session cannot
read its executable/command line. The Windows elevation request was canceled.
No guarded restart was executed. Loaded-source hashes at 1:41 PM still match the
old streamer/lifecycle/workstation code. The supervisor's PowerShell source is
loaded on each subsequent invocation; its preopen change has no intraday effect.
Visual dashboard verification could not run because browser tooling failed to
initialize; API verification is separate from visual verification.

From Administrator PowerShell, run:

```powershell
& 'C:\Cprojectsgpu_app\MarketPinPredictor\output\live_repair_20261002\activate-reviewed-release.ps1'
```

The wrapper checks the release manifest, then calls the existing guarded launcher
with expected backend PID 53028. It refuses changed source/ownership. If the PID
changes, capture a new ownership baseline and reviewed activation receipt; do not
silently replace this expected PID or reuse an obsolete proof.

After activation, verify the new exact-owned PID and source hashes; collect at
least five minutes of bounded live samples with fresh/current SPX and NDX, stable
generation and loss counters, advancing saved calculations/predictions, and exact
structural IDs joined to calculation rows and journal entries. Observe expiry
between captures. A launcher exit code or HTTP 200 alone is not acceptance.

The explicit restart is backend-only. Observe the next normal supervisor run for
recorder/staged-subscription effects; zero counters in a new process do not erase
the retained session loss. Do not claim today's tape or ORBs complete.

## Remaining parts of the repair plan

- The first opening bucket was missed and remains missing. Provider clock had
  zero of 50 required samples at the opening attempt. Independent host-clock
  bootstrap authority still requires the documented calibration/persistence work.
- Recorder recovery after historical loss still requires a separately labeled,
  clean segment and proven capacity/transport ownership; broad intraday replay is
  not part of this release.
- Current-session observed equity bars remain blocked by provider entitlement.
  Option-parity values are not a substitute for observed index OHLCV.
- Opening-load soak/concurrency proof and post-deployment live stability remain
  NOT YET VERIFIED. Do not promote models or promise prediction accuracy.

## Receipts

`output/live_repair_20261002/` contains source-before copies, focused diffs,
`release-manifest.json`, before/after synthetic profiles, `reader-comparison.json`,
read-only `before_activation/` and `activation_pending/` HTTP/SQLite receipts with
SHA-256 manifests, and `activation-attempt-1.json`. No prior receipts were replaced.
The repeatable bounded read-only collector is `tools/capture_live_repair_evidence.py`.
