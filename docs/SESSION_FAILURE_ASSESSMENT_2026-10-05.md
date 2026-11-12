# MarketPin session failure assessment — Monday, October 5, 2026

Prepared October 6, approximately 1:15 AM America/Chicago. All operator times below are Central Daylight Time. October 6's cash session has not opened; this assessment covers the most recent completed session, October 5.

**Overall assessment: DEGRADED.** The app collected substantial usable data and produced hundreds of research forecasts, but failed complete live opening capture, lost core forecast availability near the close, and did not settle Monday's official closes. Several problems are data-delivery or application-integration failures; others are correct withholding because required evidence does not exist. Passing source tests did not resolve all runtime failures.

This was a read-only assessment. No services were restarted, provider downloads initiated, models or subscriptions changed, or historical records rewritten. Evidence was read from SQLite in query-only mode, local health endpoints, retained artifacts, scheduler state and logs. New assessment files were written separately.

**Classification:** OBSERVED means directly present in retained records or a current read; INFERRED means a supported explanation that still needs a causal test; NOT YET VERIFIED means evidence is insufficient. Counts describe retained records, not continuous uptime, unique trading opportunities, or independent training sessions.

## Priority findings

| Priority | Failure | Evidence and impact | Assessment |
|---|---|---|---|
| 1 | Exact opening bucket missed | All four configured ORB symbols first stored a sample at 8:30:20 AM. No 8:30:00 bucket. All original live ORBs remain partial. | OBSERVED; unresolved for future opens. |
| 1 | Core forecast availability deteriorated near the close | Last valid periodic SPX forecast: 2:46:20 PM; NDX: 2:54:54 PM. Invalid calculations followed. | OBSERVED; quote-pair/freshness investigation remains necessary. |
| 1 | Forecast identity writes failed under database contention | 20 passport issuance failures from 2:41:18–2:41:44 PM, with SQLite `database is locked` at `BEGIN IMMEDIATE`; nullable forecast IDs published. | OBSERVED; lock owner NOT YET VERIFIED. |
| 1 | Monday official close capture/scoring failed | 5:05, 5:20 and 5:45 PM attempts all rejected the SPX artifact because it ended October 2. Zero October 5 close observations in the ledger. | OBSERVED; selected forecasts remain unscored. |
| 2 | Future-expiration quote capture never advanced | Midday evidence shows 1,528 active of 2,616 selected contracts, 1,088 deferred; stage frozen by queue pressure. | OBSERVED; future gamma cannot be computed from unsubscribed quotes. |
| 2 | Structural forecasts still lacked committed calculation identity | SPX and NDX request receipts at 12:49 and 1:53 PM return `COMMITTED_CALCULATION_ID_MISSING`, despite research forecasts elsewhere. | OBSERVED; separate from later passport lock failures. |
| 2 | Startup and recovery remained unreliable | Nine failed supervisor invocations, one 150-second preflight timeout, PID-zero ownership problems, and a source-fingerprint mismatch. | OBSERVED; narrow PID fix helped but did not establish full recovery authority. |
| 2 | Verified technical-indicator bars unavailable | 38 EQUS underlying-validation license-denial log entries; indicator service reports zero accepted bars and `ENTITLEMENT_UNAVAILABLE`. | OBSERVED; provider access is a blocker, not merely UI formatting. |
| 2 | RUT/VIX calculations generally ineligible | RUT: 4 valid / 341 invalid stored calculations. VIX: 0 valid / 68 invalid. Thin chains and insufficient paired quotes are documented. | OBSERVED; retain symbol-specific withholding. |
| 2 | Trained-model artifact failed to load | At 8:05:27 AM, SPX checkpoint rejected for missing ordered feature contract; startup reported no inference artifacts loaded. | OBSERVED; research heuristics still ran, so this was not a total prediction outage. |
| 2 | Promoted-model evidence still insufficient | Retained readiness: 9/10 clean captures, 7/60 joint model-evidence sessions, 0/20 paper-forecast sessions. | OBSERVED evidence shortfall; gates are behaving correctly. |
| 3 | UI/deployment states diverged | Main UI showed an 8:17 preopen snapshot at 8:38. Updated research UI ran on 8502 while original research service remained on its earlier loaded source. | OBSERVED; users can see different capabilities depending on page/service. |
| 3 | Operational monitoring has blind spots | 101 dashboard recovery deferrals due to unverified listener enumeration; two backend recovery deferrals near the close; logon heartbeat task last result 1. | OBSERVED; not proof of 101 dashboard outages. |

## 1. Opening capture: what actually failed

The exact first-bucket diagnostic shows:

- The Windows host reported a recent successful synchronization and candidate-ready clock evidence.
- The live provider clock had **0 of the required 50 timing samples** at the first attempt. Opening retries reported `PROCESSING_CLOCK_NOT_SYNCHRONIZED` through 8:30:18.
- Provider timing passed afterward, but handoff still reported missing fresh NDX evidence around 8:30:19–20.
- The first stored sample bucket was 8:30:20 for SPX, NDX, RUT and VIX. First-bucket checks also reported unverified/changing runtime context.

**INFERRED:** bootstrap timing and quote/handoff readiness jointly prevented exact opening admission. Calling this simply “Windows lost clock sync again” does not fit the recorded host-clock evidence. Synchronizing Windows alone cannot create eligible quote pairs in the missing bucket.

Final accepted five-second bucket counts, calculated from the active CBBO sample table joined to its admission decisions:

| Symbol | 5-minute | 15-minute | 30-minute | 60-minute | Exact first bucket |
|---|---:|---:|---:|---:|---|
| SPX | 56/60 | 176/180 | 356/360 | 708/720 | Missing |
| NDX | 56/60 | 176/180 | 356/360 | 708/720 | Missing |
| RUT | 56/60 | 176/180 | 356/360 | 708/720 | Missing |
| VIX | 21/60 | 107/180 | 232/360 | 464/720 | Missing |

Higher later coverage cannot replace the first bucket. SPY was not configured for this live ORB capture path.

A separately retained, previously authorized historical reconstruction recovered SPX's first 5/15/30-minute sampled ranges. NDX/RUT/VIX remained partial in that reconstruction. Those inferred ranges were not inserted into the live tables and do not make the original live capture complete. The reconstruction's 60-minute result was still forming at its 9:22 AM assessment.

Evidence: `logs/opening_reference_attempts/2026-10-05.ndjson`; `logs/opening_capture_checks/2026-10-05/`; assessment `database-summary.json`; `output/orb_recovery_20261005/reconstruction-verified/REPORT.md`.

## 2. Quote quality, future dates and forecast availability

The session did generate usable research results:

| Symbol | Valid calculation rows | Invalid calculation rows | Saved periodic predictions | Last saved valid periodic prediction |
|---|---:|---:|---:|---|
| SPX | 361 | 14 | 361 | 2:46:20 PM |
| NDX | 369 | 8 | 369 | 2:54:54 PM |
| RUT | 4 | 341 | 4 | 9:08:55 AM |
| VIX | 0 | 68 | 0 | None |

These are stored calculation/prediction counts, not a forecast-availability percentage. Request-time checks can reject a previously valid calculation, and journals do not record every unsuccessful request.

At 2:53:50 PM, SPX's paired-primary coverage was 6.67% against the unchanged 10% minimum. NDX also became intermittently unavailable; its first stored invalid calculation was at 2:52:04 PM. Current-generation alignment and a connected stream did not establish sufficient current inputs.

The core CBBO stream logged **nine queue-full warnings**, including a queue of 525 records at 10:56:15 AM. This is observed backpressure; it does not itself prove records were dropped. The separate TCBBO recorder has its own counters and passed its retained finalization audit.

At 12:49 PM, supplemental subscriptions were still frozen: 1,088 selected future-expiration contracts were not active. `QUEUE_FULL_DELTA_FREEZES_PROMOTION` was recorded, no additive subscription request had been sent, and the last eligibility evaluation remained at 8:30:17 AM. Current source confirms queue growth makes this stage terminal rather than automatically resuming after a clean interval. The old warmup reasons in that frozen object must not be interpreted as fresh midday clock failures.

Future-expiration open-interest max-pain context **was** present in the SPX payload, including October 6 and 7. Fresh gamma profiles contained only the primary expiration. OI context, live gamma, and next-session price forecasts are different products with different evidence requirements.

Friday's missing official closes blocked positive-horizon research for much of Monday. The historical-close recovery at approximately 2:42 PM restored that prerequisite: the original journal saved six SPX and twelve NDX next-session forecasts. The later loss of quote eligibility still withheld new forecasts. Today’s newly missing October 5 close can repeat the history-currentness block at Tuesday’s open unless settled in time.

Evidence: `database-summary.json`; `research-journal-counts-corrected.json`; `output/data_utilization_20261005/20261005T174932244507Z-health.json`; `output/forecast_reliability_20261005/20261005T195350632527Z-runtime.json`.

## 3. Calculation identity and database writes

There were **two distinct identity failures**:

1. **Structural calculation ID missing.** Both core symbols' retained 12:49 and 1:53 PM API results lack a calculation ID in their pin payload and withhold structural estimates with `COMMITTED_CALCULATION_ID_MISSING`. This happened before the database-lock incident. Durable periodic records do contain calculation IDs, so a missing propagation/binding step is a plausible explanation, but the exact loaded request-path defect has not been reproduced in this assessment.
2. **Passport writes blocked.** From 2:41:18–2:41:44 PM, logs show 8 SPX prediction, 9 NDX prediction, 1 NDX snapshot and 2 VIX prediction passport failures. All corresponding traces fail at SQLite `BEGIN IMMEDIATE`. The application explicitly published nullable forecast IDs afterward.

The latter interval overlaps the October 2 official-close recovery, which finished at 2:41:51 PM. **INFERRED:** a concurrent write transaction is a strong suspect. **NOT YET VERIFIED:** which writer held the lock, its transaction duration, and whether it explains any separate quote-coverage deterioration. This timing is a reason to investigate the recovery's interaction with live writes, not to declare the recovery harmless or causally guilty without tracing.

Evidence: `logs/runtime/backend.stderr.log`, lines 601–1333; retained prediction API receipts; assessment `log-and-tape-summary.json`. No missing IDs were synthesized after the fact.

## 4. Official close settlement and scoring

The 3:05 PM selection task succeeded and froze these diagnostic forecasts:

| Symbol | Selected estimate | Forecast time | October 5 official close / absolute error |
|---|---:|---|---|
| SPX | 7,773.29268292683 | 2:46:20.914121 PM | NOT YET VERIFIED / unavailable |
| NDX | 31,005.661944047606 | 2:54:54.018161 PM | NOT YET VERIFIED / unavailable |
| RUT | 2,841.191410071195 | 9:08:55.974281 AM | NOT YET VERIFIED / unavailable |

The RUT selection is almost six hours before close. It is the last eligible stored selection under the existing policy, not a claim of a fresh late-day forecast. Comparisons must disclose this forecast age.

All three evening settlement attempts failed with `OfficialArtifactStaleError`. The captured SPX CSV ends on October 2, at 7,722.72; that value is not Monday's close. There is no October 5 close observation or completed settlement receipt in the inspected ledger.

The bundle fetch loops through symbols and raises on the first stale artifact. SPX's failure therefore prevents collection/settlement of the remaining families in that attempt. Their absence does not establish that their official publishers were also late. A permanent improvement should retain separate per-symbol acquisition results while preserving the existing full-bundle promotion and provenance requirements.

The new attempt receipts worked: they retained all three exact failure times and reasons. Morning retries at 6:30, 7:45, and 8:15 AM plus sign-in are configured. At assessment time, those October 6 runs had not happened. No source was downloaded or close ingestion retried for this read-only review.

Evidence: `data/prediction_score_selections/2026-10-05/e7fe4a34a663ac14d6b4e917ed6ee5eeea52777b555bae599f05681171d3307a.json`; `data/postclose_attempts/2026-10-05/`; `logs/verified_close_postclose/2026-10-05-catchup.log`; `data/verified_close_sources/2026-10-05/SPX/`.

## 5. Indicators, models and accuracy evidence

- **Observed bars:** the early metadata check found publication lag, while actual underlying requests separately returned HTTP 403 `license_not_found_unauthorized`. Both happened. The indicator collector reports zero accepted SPY/QQQ/IWM/DIA bars. Technical indicators requiring observed OHLCV therefore cannot be enabled truthfully from these inputs.
- **Model artifact:** the SPX trained checkpoint lacks its ordered feature contract; the loader correctly rejected it. Reconstruct the exact training feature order and artifact manifest, then prove deterministic inference before enabling it. Do not guess the order or label a research fallback as the trained model.
- **Structural estimate:** remains blocked by calculation identity in the inspected request receipts, independent of indicator availability.
- **Promoted tape model:** nine clean capture sessions is progress, but not ten. Seven sessions with the required joint model evidence are not sixty. Seventy-five verified daily close dates do not equal seventy-five complete tape/feature/label sessions. Paper evidence is zero of twenty sessions.
- **Adaptive EOD challenger:** remains withheld pending sufficient chronological training/test evidence and provable pre-close issuance. The original research process still lacks the new receipt-table implementation; the separately launched preview saves post-commit receipts. Deployment into the primary service remains incomplete.
- **Measured research weakness:** Monday's retained five-minute shadow walk-forward review showed SPX candidate MAE 4.407618 versus last-price baseline 2.909796 over 234 comparisons; NDX 21.971277 versus 16.399724 over 302. That candidate underperformed. These are five-minute parity-outcome comparisons across prior sessions, not Monday EOD close errors and not evidence that every candidate is worse.

**NOT YET VERIFIED:** Monday's official-close accuracy, calibrated probabilities/intervals, or improved EOD accuracy from the new comparison/adaptive work. CUDA would not supply missing contracts, permissions, labels or model evidence.

## 6. Startup, supervision and display failures

Nine supervisor invocations failed between 7:46 and 8:36 AM: five parameter-validation exceptions and four runtime exceptions. One source preflight exhausted its 150-second deadline at 8:16 AM. Another preflight took approximately 79 seconds before its invocation failed. An 8:36 attempted dashboard launch lacked verified ownership, and its cleanup encountered a source-fingerprint mismatch while source was changing.

The daytime PID-zero/port-occupancy repair passed its recorded 174 tests and later invocations returned successfully. However, 101 dashboard recovery decisions still deferred because listener enumeration did not establish exactly one owner. Two backend recovery decisions at 2:50 and 2:55 PM also deferred while core calculation readiness failed. Preserving an unverified process was correct; treating that as fully functional automatic recovery would be incorrect.

Windows Task Scheduler currently records AutoStart result 1, the auxiliary Codex logon heartbeat result 1, selection result 0, settlement result 1 and the final watchdog result 0. A task exit code alone cannot establish data readiness. The logon heartbeat's specific root cause remains unverified.

The stale preopen main-page snapshot was corrected manually during the session. Automatic refresh and Central-time rendering were verified later in the companion page on port 8502; that did not update every module in the older elevated primary processes. Deprecated Streamlit width arguments also generated warnings, but the inspected dashboard log did not contain a Python traceback. Those warnings are maintenance work, not the reason core predictions were withheld.

## What succeeded and must be preserved

- Current-day universe provenance was October 5, with no fallback in the inspected opening samples. This was not another observed prior-day-universe capture incident.
- The main store has 734 valid periodic research predictions across SPX/NDX/RUT and 14,856 raw stored CBBO reference samples. Accepted samples remain distinct from excluded admission decisions and failed attempts.
- The database's overnight query-only check returned `quick_check=ok`, with WAL mode. That does not disprove the intraday write-lock failures.
- The separate options tape launched at 9:35:21 AM with replay from the configured session start, completed at 3:20 PM, and has a passing stored `tcbbo-observed-v7` finalization audit at 3:25:33 PM: **3,229,954 trade records**, 3,201,645 valid-NBBO TCBBO records, three of three subscription acknowledgments and completed replays, no reported slow-reader warnings, no provider errors, and no unmapped trades. Retained readiness marks this session capture-eligible, with all five families present.
- Tape completion was confirmed from existing catalog/status/finalization evidence; the finalized DBN was not reopened or rescanned. It is not a claim that every trade had a valid NBBO, that true aggressor positions were observed, or that capture alone authorizes predictions.
- Ten checked content-addressed opening/selection/attempt receipts matched their filename hashes. Original failures and selections remain intact.
- Overnight disconnection, absent fresh quotes and the 11 PM trading-date rollover are expected off-hours states; they were not counted as another Monday regular-session outage.

## Recommended repair order and acceptance criteria

1. **Opening readiness and transport:** reproduce the first 25 seconds with the timestamp contract; establish valid independent clock admission and per-symbol eligible quote/handoff evidence before the bucket deadline. Acceptance is a saved exact first bucket plus subsequent coverage, or an explicit irrecoverable missing-input result. A unit test or later reconstruction is insufficient live proof.
2. **Publication identity and write isolation:** trace each request to its already committed calculation, then instrument writer ownership and transaction duration. Keep heavy close-history/scoring work from starving passport commits. Acceptance: no null IDs from contention under concurrent capture/settlement tests, bounded latency, unchanged forecast-to-input bindings, and observed live persistence afterward.
3. **Closing labels:** retain each publisher's exact-date outcome independently, make publication delay actionable, and exercise the scheduled catch-up path. Acceptance: Monday's selected forecasts gain append-only official outcomes and exact errors, with any mismatch retained. Do not change the immutable selections.
4. **Future-date capture:** first resolve measured queue pressure; then design an explicit bounded recovery transition from a frozen supplemental stage. Acceptance requires active contract acknowledgments, advancing fresh future-date pairs, labeled segment identity, and no deterioration in core capture. Do not remove the overload gate just to show values.
5. **Deployment and observed indicators:** verify process ownership/source fingerprints before a controlled release; resolve EQUS permission through the authorized provider plan or an approved source. Acceptance requires actual current bars and coherent app/service versions, not a green socket check. Any new paid entitlement needs a separate decision.
6. **Accuracy:** repair the feature-contract artifact, accumulate eligible feature/label sessions and test against the last-price baseline with chronological holdouts. Promote only demonstrated improvement; keep the underperforming five-minute candidate out of production promotion.

The most immediate repeat risk for Tuesday is the combination of unchanged opening bootstrap behavior and missing Monday close labels. The most serious newly confirmed intraday engineering defect is forecast-identity loss under write contention. These should be addressed before adding model complexity.

## Evidence package and limits

Retained assessment evidence is under `output/session_failure_assessment_20261005/`. It includes database aggregates, log/tape summaries, corrected research-journal extraction, scheduler state, receipt-integrity checks, and the overnight baseline. `research-summary.json` initially inspected the wrapper rather than nested forecast status; use `research-journal-counts-corrected.json` for journal status/count claims. No missing status was interpreted as a failed forecast.

Source files were not changed. Exact lock ownership, continuous request-level availability, every transient UI state, and official sources' current publication status were not established. No inference that these are the only possible undiscovered defects is warranted; this register covers the failure classes supported by the reviewed session evidence.
