# Observed indicator capture and independent display

Prepared late October 1 Central / October 2 UTC. This change supplies price-bar
context; it does not change forecast models or establish prediction readiness.

## Findings and implementation

- **OBSERVED:** `EquityContextTracker` existed but was not started by the running
  research-service launcher. The indicator endpoint therefore had no retained
  `equity_context.db` to read.
- **IMPLEMENTED AND TESTED:** an explicitly enabled, separate display collector
  now belongs to the research-service lifespan. It requests SPY, QQQ, IWM and DIA
  completed one-minute EQUS.MINI bars, once per minute during the cash session.
  An OS lease prevents concurrent writers; dashboard GETs only read retained data.
- **IMPLEMENTED AND TESTED:** recovery starts at the earliest missing current-day
  minute, including after a denied request, write failure, restart or date rollover.
  Successfully retrieved bars retain their actual retrieval time. Records are
  committed before being published in memory, so a failed write remains retryable.
  The store is append-only and separate from OPRA observations and forecast history.
- **IMPLEMENTED AND TESTED:** the independent **Observed price indicators** panel
  can be loaded even when forecasts abstain. It shows RSI, MACD and its signal,
  SMA, momentum, KAMA and a typical-price volume-weighted approximation. SPY is
  clearly labeled ETF context for SPX; QQQ for NDX, IWM for RUT and DIA for DJI.
  No ETF number is relabeled as an observed cash-index price.
- **IMPLEMENTED AND TESTED:** existing freshness, full-session continuity,
  provenance and 35-completed-bar requirements remain enforced. A provider
  license denial is shown explicitly, with a 30-minute retry backoff.

These minute-bar recovery requests do not reconstruct or overwrite opening-range
receipts, prior predictions, accuracy records, option tapes or daily closes.

## Provider checks

- **OBSERVED:** a bounded historical request returned two October 1 SPY/QQQ
  bars for 19:59–20:00 UTC. The estimated cost was USD 0.000001251698, checked
  before retrieval against a USD 0.01 cap. No returned bars were inserted into
  the application's market databases by this probe.
- **OBSERVED:** a separate bounded live EQUS.MINI connection was denied with
  `ENTITLEMENT_UNAVAILABLE` (`BentoError`), returning no bars. Historical
  retrieval success does not establish permission for live data.
- **NOT YET VERIFIED:** successful current-session collection and live displayed
  indicator numbers. The check occurred after cash-market hours, and the live
  entitlement is denied. The account must have appropriate live U.S. equities
  access before this can be verified. No subscription or license was purchased.

Databento's own equities guide describes the live subscription requirement:
https://databento.com/docs/examples/equities/equities-introduction

## Evidence

Directory: `output/indicator_capture_20261002/`

- `baseline-sha256.json` and `baseline/`: preserved source baseline.
- `tests-final.xml`: **140 tests passed**, covering capture recovery, persistence,
  single-writer ownership, research lifespan/API isolation, display and exports.
- `20261002T044351578466Z.access-check.json`: historical access result.
- `20261002T044515096273Z.live-access.json`: live entitlement denial.
- `release-manifest.json`: exact tested file hashes and expected research hash.
- `*.activation.json` and corresponding progress logs: guarded activation
  outcome. Only an `ACTIVATED_OBSERVED` receipt proves successful activation.

Activation is limited to the independent research service and Streamlit
dashboard. The helper checks exact process ownership, source hashes and the
unchanged options-backend identity. It does not restart the options backend.

At 11:52 PM Central, activation remained **NOT YET VERIFIED**: Windows was
waiting for administrator consent, the elevated helper had produced no receipt,
and the last pre-activation research readback still showed PID 10992 and the
previous loaded source hash. The new source and opt-in configuration are saved;
they are not evidence that the running collector has loaded them. Inspect the
timestamped activation receipt after consent before claiming the change is live.

### October 2 activation update

**OBSERVED at 12:47:02 AM Central:** the guarded activation completed successfully.
Receipt `output/indicator_capture_20261002/20261002T054515233Z.activation.json`
records `ACTIVATED_OBSERVED`. The research service changed from PID 10992 to
46424 and loaded the expected source hash
`5fc9ae9891aab1ad9e0f92cbc7c085c2e5fe17120525eb1d256081b15c1ab1f4`.
The dashboard changed from PID 41628 to 65556. Options-backend PID 27484 stayed
unchanged. The separate display collector reported enabled/running; it had no
current-session bars outside cash hours. This supersedes the earlier pending
activation state, but does not establish live equities entitlement or current
indicator values. Browser visual verification remained unavailable because the
browser-control connection timed out; process and endpoint checks succeeded.

At 1:01–1:06 AM the app processes were later found absent for an unverified reason.
The coordinated full-stack recovery completed at 1:16:59 AM, and a 1:19 AM
readback confirmed the independent collector still enabled/running with the
expected source hash in research PID 56568. See
`output/session_readiness_20261002/20261002T061457610Z.recovery.json` and
`post-recovery-readback.json`. Current-session bars remain unverified.

## Remaining acceptance

1. Enable the required U.S. equities access in the Databento account if wanted.
2. During a cash session, confirm advancing captured rows and current SPY/QQQ
   bars. Following an entitlement denial, retry occurs after the 30-minute
   backoff; restarting is not a prerequisite for the account change to take effect.
3. Load **Observed price indicators** and verify correctly labeled current values
   after sufficient completed bars have been received. Missing data continues
   to display the specific cause; stale or synthetic numbers are not substituted.

The indicators are analytical context. Their availability is not proof that
forecast accuracy improved, that all forecast horizons qualify, or that every
other previously reported application issue has been resolved.
