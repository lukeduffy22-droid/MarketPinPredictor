# October 5 live readiness verification

Verified between 08:19 and 08:41 America/Chicago. Overall result: **DEGRADED**.
Current SPX/NDX live research estimates and ongoing persistence work. Complete
opening capture did not pass; this is not an all-clear for every subsystem.

## Observed results

- Today’s verified option universe is dated 2026-10-05, with no fallback. The
  backend is generation 1 in epoch
  `bacbb95c42f733a6e9965cf81cd9d204295a6b00ff842fca793955634608a56d`.
  The Friday backend repairs matched the loaded startup fingerprints.
- Twenty-eight retained samples span 08:28:54–08:35:39 CT. Accepted quote
  messages advanced from 0 to 245,587. At 08:39:25 they reached 394,783.
  Handoff was active; reconnects and reported skipped records were both zero.
  Four queue-full warnings had accumulated by 08:39, so this was not a clean
  transport run with no warnings. Absence of reported loss is not losslessness.
- SPX and NDX prediction requests returned HTTP 200 in all 19 samples from
  08:31:09 through 08:35:39. Results explicitly retain RESEARCH_ONLY authority,
  decision_grade=false, and uncalibrated confidence. Current-generation source
  validation passed. This verifies availability, not forecast accuracy.
- At 08:39:25 SPX age was 5.93 seconds and NDX age 18.44 seconds, within the
  unchanged 30-second limit. NDX carried a freshness warning. RUT and VIX failed
  their independent chain-thickness checks (25 and 1 strikes versus 30).
- Gamma calculation row IDs advanced 24276 → 24292 and prediction snapshot IDs
  14833 → 14843 during the 28 samples. Gamma audits, stored inputs, pin snapshots
  and market snapshots also advanced into October 5. The preopen SQLite quick
  check passed and WAL mode was observed.
- The active CBBO ORB tables are `orb_cbbo_reference_samples` and
  `orb_cbbo_reference_sample_decisions`. The older `orb_reference_samples`
  table is not this deployment’s capture lane. At 08:39:25 SPX, NDX and RUT each
  had 110 stored samples starting at 08:30:20; VIX had 63 with gaps.
- The five-minute ORBs for SPX, NDX and RUT contain **56/60** eligible buckets
  (93.33%). All missed the exact first bucket and remain partial. VIX contained
  21/60 at the five-minute check. No missing observations were reconstructed.
- The first-bucket failure receipt is
  `logs/opening_capture_checks/2026-10-05/c2622923161d139e643ff4e493d68f555c3745f44b56be45769e2cdc875b3639.json`.
  Failed attempts identify insufficient live provider clock samples. Host
  diagnostics were candidate-ready, but have diagnostic authority only.
  Primary quote/handoff readiness also arrived later. Windows synchronization
  alone would not prove that the missing buckets could have passed.
- The provider clock subsequently passed with 2,048 retained timing samples.
  At 08:38 the frontend still displayed its 08:17 preopen snapshot. Refreshing
  live status showed OPEN, the passed clock check and prediction pipeline ready.
- An EQUS.MINI metadata-only check at 08:35:25 returned HTTP 422,
  `data_start_after_available_end`: requested start 13:34 UTC, available end
  13:30 UTC. Today’s observed indicator error is publication delay; this check
  did not establish an entitlement denial. No indicator bars were accepted.
- The broad closing-tape collector is deliberately deferred until 09:35 CT.
  Its start, uninterrupted day capture, and eventual close settlement remain
  NOT YET VERIFIED. SPY is not configured for the current live ORB lane.

## Narrow supervisor repair

Windows listener enumeration intermittently returned PID 0 or omitted an owner
while the corresponding local service was responding. PID 0 reproduced a
`ParameterBindingValidationException` in the ownership helper.

`market_app_supervisor.psm1` now treats PID 0 as unverified, never as a process
that can be stopped or attributed to a launch. If enumeration finds no listener,
a bounded 250 ms loopback connection checks port occupancy before allowing a
new launch. That connection grants no ownership or readiness authority.

Seven new regressions plus the existing supervisor suite passed: **174 passed,
0 failed**. The original two PID-zero regression cases failed before the fix.
The 08:40 scheduled invocation completed with exit result 0, preserved backend
and dashboard, and deferred the closing tape as configured.

The intermediate 08:35 invocation, before the occupancy fallback was added,
attempted a dashboard start after enumeration omitted its listener. Ownership
verification failed; its cleanup was blocked because the source fingerprint had
changed during the invocation. No claim is made that the attempted launch was
cleaned up or that current OS process ownership is fully verified. The final
08:40 run preserved responding ports without another launch. Backend generation
and quote progression remained unchanged. No manual backend restart was issued.

## Verification limits and remaining work

- **OBSERVED:** fresh core estimates, advancing persisted observations, partial
  ORBs, successful final scheduled supervisor execution, and original gaps.
- **INFERRED:** PID-zero ownership observations explain the matching parameter
  validation failures; the older log records did not retain exception stacks.
- **NOT YET VERIFIED:** complete future opening capture, all-symbol forecast
  readiness, fresh indicator bars, exact OS listener ownership, closing-tape
  launch and day completion, and predictive accuracy.

The permanent opening repair still needs a separately versioned, tested clock
admission design and timely eligible quote/handoff evidence before the first
bucket expires. The current host-clock observer is not that implementation.
Do not relabel its diagnostic result as admission authority or lower the current
50-sample requirement as a shortcut. A missed first bucket cannot be repaired by
later live samples or retroactively labeled observed.

Current SPX/NDX audit aggregates passed gross >= abs(net), gross = call + put,
and net = call - put. The legacy `tools/recompute_gamma.py` did not provide a
full replay proof: it sums only the five retained top-strike audit rows against
the full-universe totals, then its Windows console output raised an encoding
error. The independent aggregate checks are recorded separately; full input
replay has not been claimed.

All task receipts, the pre-edit supervisor copy, exact patch, test result and
release hashes are retained under `output/readiness_20261005`. Existing models,
thresholds, subscriptions and historical market records were left unchanged.
