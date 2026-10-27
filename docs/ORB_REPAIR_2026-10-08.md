# ORB capture repair — October 8, 2026

Prepared for the October 7 incident and the next opening session. All local times below are America/Chicago (CDT).

**Status: repaired source deployed and verified at 01:26 CT on October 8; runtime is off-hours/preopen.** The October 7 opening range remains partial. A complete future opening range requires fresh live evidence; these repairs do not establish that it has been captured.

## What the October 7 evidence establishes

The sampler started on time: its first opening attempt was **08:30:00.130472**, inside the exact 08:30:00–08:30:05 first bucket. It rejected that attempt because provider-processing clock evidence was not yet sufficient. Clock samples accumulated slowly, from zero to 17 through 08:30:12, while recorded receive-to-process lag reached approximately **11.28 seconds**. At 08:30:13–15, the rejection changed to inactive handoff with NDX still missing from the required fresh-family observations.

The first retained eligible reference bucket for SPX, NDX, VIX and RUT was **08:30:15**. The first SPX/NDX samples were persisted around 08:30:16. Their contributing provider CBBO intervals were also after the first bucket. Older optional last-sale timestamps do not reconstruct an opening quote observation.

This establishes delayed opening readiness, not a late sampler launch. The retained data does **not** identify the exact lock holder or prove that publication contention was the sole cause of October 7's delay. An offline reproduction did confirm a warming-reader defect that can produce this type of obstruction.

A separate failure followed the opening hour: at **09:30:09**, the additive shadow subscription was followed by the existing RUT canary rollback. The selected symbol list fell from 2,576 to 2,048 contracts, but its metadata retained the old selected-universe hash. Reconnection then failed with `FULL_SELECTED_UNIVERSE_HASH_MISMATCH` beginning at **09:30:12**. This later failure explains the degraded runtime shown in the retained afternoon ORB payload; it did not cause the earlier missed opening bucket.

Evidence: [opening attempts](../output/orb_repair_20261007/opening-minute.json), [stored range summary](../output/orb_repair_20261007/stored-orb-summary.json), [provider timing summary](../output/orb_repair_20261007/first-provider-timing-summary.json), [cold reader reproduction](../output/orb_repair_20261007/reader-cold-open-profile.json), and [rollback sequence and verification](../output/orb_repair_20261007/staged-hash-repair-tests.json).

## Repairs

| Component | Confirmed defect and repair |
| --- | --- |
| Warming reader in `backend/databento_streamer.py` | Recording a fresh family waited on the forecast-publication lock, preventing the live reader from consuming the next quote and clock sample. The reader now retains bounded pending observations under the short quote lock and attempts publication without waiting. The existing watchdog can drain those observations after contention ends, even without another quote. Activation rechecks each required family's generation, subscription cutoff and freshness for both ordinary warming and timeout recovery. |
| Existing optional-family rollback in `backend/databento_streamer.py` | RUT removal changed the selected contracts without updating their declared identity. Rollback now verifies the prior hash and ordered frame/list agreement, prepares retained metadata and indexes before mutation, and publishes the matching frame, symbol list, indexes and hash under the existing locks with handoff inactive. It preserves original source provenance and does not reselect remaining contracts or spend freed capacity. |
| ORB display in `app/services/orb_view.py` | The panel treated every displayed row as a live configured market. An unconfigured RUT canary with no current bound sample could trigger a global process-identity warning. Notices now name the affected symbols and distinguish unconfigured capture, inactive handoff, actual process/generation mismatch and incomplete opening capture. Existing price, freshness and breakout eligibility gates remain enforced. |

Independent review caught and resolved two rollback atomicity defects: invalid retained numeric data and a disagreement between the frame's family label and cached symbol index could previously cause partial mutation. Candidate index construction and ordered frame/list checks now reject those cases before changing shared state. The review also caught the old warming rule's reliance on historical “seen” flags; activation now requires all required observations to remain fresh at the decision time.

The repair preserves existing dirty work, captured tapes, database/WAL, opening gaps and immutable receipts. It does not change formulas, configured freshness/coverage thresholds, required opening buckets, subscription selection policy, replay or model promotion. No provider connection was opened for offline verification. Missing October 7 observations have not been backfilled or relabeled complete.

## Verification and release state

All tests used the repository's canonical `.venv\Scripts\python.exe`, offline fixtures/fake provider clients, and absolute isolated SQLite database paths.

- Warming, ingestion, clock and opening suites: **199 passed** in 12.52 seconds.
- Rollback, staged subscription and streamer suites: **197 passed** in 15.21 seconds; the independent reviewer also ran the 10 dedicated rollback tests successfully.
- ORB display and end-to-end rehearsal: **20 passed** in 22.13 seconds.
- Before/after evaluation of the retained real ORB payload preserved every prior data/eligibility field except the misleading UI-added warning on the unconfigured row; the new status explanation was added separately.
- **Final consolidated test result: 297 passed in 46.36 seconds across 14 suites, exit code 0.** Source and test hashes remained unchanged throughout the run. The earlier focused runs overlap and must not be added together as distinct test counts. See [test receipt](../output/orb_repair_20261007/final-combined-tests-result.json) and [output](../output/orb_repair_20261007/final-combined-tests.txt).
- **Backend activation verified at 01:21:44 CT:** canonical guarded recovery replaced owned PID 49180 with PID 52192. The new epoch is `5f06de505a62751ab62a5b7867e13969fbcabed1711350569c9f5d01e31909a8`; its loaded streamer hash matches the tested `c5d5b39d322e212ac7221153bb87b2b912d22c93245e0f3d84e166790a2a9d69`. The canonical source/database preflight passed before stopping the old process. Recorder and research services were outside the recovery scope.
- **Dashboard activation verified at 01:24:44 CT:** hot reload is intentionally disabled, so separate canonical guarded recovery replaced owned PID 47020 with PID 17156. The backend identity stayed unchanged. Browser inspection showed the new per-symbol explanation and status-detail column, a fresh October 8 page timestamp, market closed, and opening capture not started.
- At **01:26:30 CT**, `/health`, `/health/live`, `/v1/orb` and Streamlit health all returned HTTP 200 in 16–47 ms. All **72** captured startup source hashes matched the sealed release; on-disk source also matched. The ORB sampler thread was alive, no opening attempts had failed, and backend `last_error` was null. These are deployment/off-hours checks, not proof of live quote progression. See [post-activation evidence](../output/orb_repair_20261007/postactivation-runtime.json).
- A read-only comparison confirmed all October 7 ORB row counts and first/latest timestamps unchanged. The staged Git diff SHA256 also remained `02932c0c5d65cdb70191751a066ddbf51f5734d1645a1d5e879a738e19bf0f83`.

Beforecopies, incremental patches, test receipts and independent-review reproductions are retained under [the repair evidence directory](../output/orb_repair_20261007/), including `identity-baseline`, `staged-baseline` and `warming-baseline`. The [backend activation manifest](../output/orb_repair_20261007/release-manifest.backend-activation.json) is preserved separately from the later [dashboard release manifest](../output/orb_repair_20261007/release-manifest.json).

An initial activation helper stalled while serializing decorated PowerShell output into its evidence log, before invoking the canonical restart. Only that exact owned helper was stopped; the backend was preserved. The helper now logs scalar summaries and retained file paths; two offline logging checks passed before retry. Both subsequent guarded activations completed successfully.

The new component logs contain no traceback, recurring runtime-read timeout, universe-hash rejection or Arrow conversion error at this check. A separate startup warning remains: the SPX inference checkpoint lacks its ordered feature contract. This ORB repair did not change model artifacts or relax that gate.

## Remaining live acceptance

No further source activation is required for this release. The one-off activation helpers expire before 07:30 CT and should not be reused as general launchers. Normal future startup remains with the existing canonical launcher/watchdog.

Overnight cache preparation used the explicitly labeled October 7 fallback for October 8; the backend universe remains uninitialized until its normal connection window. Current-day contract provenance still needs verification during morning preparation. No active cash-session feed or complete opening capture is claimed overnight.

For the next cash open, verify current-day universe provenance and clock/handoff readiness before 08:30, then retain evidence of an eligible sample committed in the **exact 08:30:00–08:30:05 bucket** for each required configured market. Observe subsequent 5-, 15-, 30- and 60-minute coverage, current-generation freshness and normal-interval persistence advancement. Confirm that any later existing optional-family rollback keeps frame/list/hash metadata consistent and permits guarded reconnection without repeated hash rejection.

The first bucket and later coverage remain separate acceptance requirements. If live evidence is missing or rejected, the dashboard must continue to report the range as partial and withhold the affected breakout signal.
