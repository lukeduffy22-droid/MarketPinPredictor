# Forecast availability and close-capture repair — October 5, 2026

The October 2 official-close gap was repaired through the existing guarded post-close runner. It had prevented otherwise eligible future-session research forecasts from using current daily history. No forecast, opening-range record, frozen selection, or earlier failed receipt was rewritten.

## Observed results

- Exact-date official artifacts now support October 2 closes: SPX 7,722.72; NDX 30,807.93; RUT 2,832.8948; VIX 15.31; SPY 769.64.
- Both running research services incorporated the appended closes without a restart. SPX and NDX now have 75 verified daily closes and 73 qualifying next-session returns, above the unchanged 60-return requirement.
- Both services produced next-session SPX/NDX research estimates after recovery. Availability is still conditional on current input validity. At 14:53:50 Central, SPX failed primary paired-quote coverage (6.67%, required 10%). The preview's last capture withheld both core symbols; the original research service still had an eligible NDX capture. This is not a claim of continuous forecast readiness.
- The original live backend remained generation 1, with active handoff and the same subscription epoch. The bounded read reported connected, progressing transport and synchronized processing-clock telemetry. Optional RUT and VIX calculations remained ineligible.

## Frozen October 2 forecast comparisons

These are diagnostic comparisons against exact-date official outcomes, not promoted accuracy records.

| Symbol | Forecast saved (Central) | Selected estimate | Official close | Absolute error |
|---|---|---:|---:|---:|
| SPX | Oct 2, 2:47:18.095890 PM CDT | 7,730.116448518537 | 7,722.72 | 7.396448518537 |
| NDX | Oct 2, 2:55:06.052263 PM CDT | 30,706.826886460087 | 30,807.93 | 101.103113539913 |
| RUT | Oct 2, 12:50:42.898732 PM CDT | 2,822.1168260799564 | 2,832.8948 | 10.777973920044 |

Selection: `data/prediction_score_selections/2026-10-02/0966b71b4560a7941218f8586c8b46475bd2fae0bea2eefcd818540bf8b6dc3b.json`.

Settlement: `data/postclose_pipeline/2026-10-02/835ba64f92ae002b719ea21ee5817b00ae189b002aabba05d3629e85d0cbb73c.json`.

## Implemented prevention

1. **Retry opportunities.** The installed Windows settlement task retains its 5:05, 5:20 and 5:45 PM weekday runs and its 6:30 AM run. Added 7:45 AM, 8:15 AM and current-user sign-in triggers. Existing action, principal and settings were preserved; before/after task definitions were retained. Each catch-up attempts at most one published, pending frozen selection from the preceding seven days. A computer that is off or has no eligible interactive session can still miss a run; these triggers do not guarantee capture.
2. **Overlapping-run protection.** Manual and scheduled settlement CLI operations now share an operating-system file lock. Selection uses a separate lock so delayed settlement cannot prevent the current day's selection. A contending settlement reports BUSY before fetching or ingesting. Lock release is automatic on process exit.
3. **Append-only attempt evidence.** Successful and failed operations save content-addressed attempt receipts with actual start/completion times under `data/postclose_attempts`. New settlement results also distinguish start from completion. Existing receipts retain their original bytes and timestamp semantics; the recovery result predates this timestamp correction.
4. **Freshness-aware display.** The companion dashboard rechecks display eligibility every 15 seconds and reads the local research service once per minute after a submitted selection. Unsubmitted horizon changes are not automatically applied. Today's estimate is withheld at its target close, even if the capture is still recent. Display timestamps use 12-hour Central time with DST handling; source timestamps remain UTC.

The display uses Streamlit's supported fragment refresh mechanism: https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment .

## Activation and validation

- Preview dashboard `http://127.0.0.1:8502/` was restarted only after verifying its exact process command and parent. New frontend worker PID 37296, launcher 22596; activation 19:51:44 UTC. Research backend PID 16784 was not restarted. Original backend, recorder, research service and main dashboard were not stopped.
- Browser verified the new refresh toggle, EOD comparison rows, next-session target date, and changing eligibility. Without a second click, submitted/read time advanced from 2:52:56 PM to 2:53:59 PM and capture time advanced from 2:52:36 PM to 2:53:38 PM. Ineligible estimates disappeared. No browser exception was observed.
- 48 focused Python tests passed, covering horizon/learning integration, close catch-up, locks, immutable attempts, cutoff withholding and Central/DST display. Three PowerShell launcher tests passed. Scoped whitespace checks passed.
- The new UI is active in the companion dashboard. The original elevated main dashboard has not been restarted to load this UI module. Official-close recovery already benefits both research services.

## Remaining limits

**NOT YET VERIFIED:** better out-of-sample prediction accuracy, uninterrupted paired-quote coverage, tomorrow's opening capture, and execution of the newly added scheduled triggers. The exact cause of the missed Monday morning task was not established. Friday's SPX artifact had initially lacked the requested date; successful later retrieval does not retrospectively make the earlier failure a valid close.

Historical model comparisons remain research only. The adaptive challenger is withheld pending independently verified forecast issuance and sufficient chronological training/test sessions. No validity thresholds, publication cutoff, model-promotion requirements, subscriptions, GPU environment, or historical records were relaxed or replaced.

Evidence: `output/forecast_reliability_20261005/official-close-recovery.json`, task XML exports, frontend activation receipt, and `20261005T195350632527Z-runtime.json`. The release manifest records exact file hashes and the verification scope.
