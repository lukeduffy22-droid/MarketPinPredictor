# MarketPin repair and forecast-evaluation receipt — October 6, 2026

Repair window: after the October 5 cash session, before the October 6 opening. Canonical checkout: `C:\Cprojectsgpu_app\MarketPinPredictor`, branch `feature/2026-09-legacy-score-backlog`, HEAD `2487a7a27de3bc40b7eece93788bf0023ffac48e`. Existing staged and unstaged work was preserved. No model was promoted, accuracy threshold reduced, missed opening bucket fabricated, or finalized tape rescanned.

The earlier assessment remains in `docs/SESSION_FAILURE_ASSESSMENT_2026-10-05.md`. New evidence is under `output/session_repairs_20261006`; its manifest records hashes. OBSERVED means directly inspected runtime, retained evidence, or test result, with that scope stated. INFERRED is a supported explanation rather than proof. NOT YET VERIFIED identifies outstanding operational or performance evidence.

## Implemented and activated

| Failure | Repair and evidence | Result |
|---|---|---|
| Supervisor repeatedly could not identify a responding server | Reproduced Windows TCP inventory omitting listening rows. Added a fallback that holds a loopback connection and requires the same positive server PID for its exact local/remote port pair in two OS snapshots. Existing repository, process ancestry, and endpoint checks remain mandatory. Five ownership tests include a real held socket and rejection of ambiguity/foreign ancestry. | OBSERVED: corrected main startup proved backend PID 7360 and dashboard PID 41144. |
| A successful manual launch subsequently disappeared | Backend log recorded `forrtl: error (200): program aborting due to window-CLOSE event`. Manual launcher now starts backend and dashboard with hidden console windows; scheduled launcher already did. | OBSERVED: new main services launched at approximately 03:20 CT and remained reachable at 03:46 CT. The exact sender of the earlier close event is NOT YET VERIFIED. |
| Clock telemetry and opening capture could wait behind calculation work | Tested and loaded retained changes that sample provider timestamps independently of expensive calculation, before symbol mapping, with replay/generation safeguards. Host-clock evidence remains diagnostic; the provider clock's required evidence was not bypassed. | OBSERVED: offline tests passed and backend startup hashes match the repaired source. Full October 6 first-bucket capture is NOT YET VERIFIED. |
| One transient queue-full warning froze future-expiration staging for the entire session | Added one guarded recovery per prepared subscription plan. It requires a fresh clean window, current epoch/generation, clock, paired coverage, progress, acceptable lag, no slow-client/skipped-record/reconnect evidence, and completion of the opening hour. It writes an immutable recovery receipt before rearming and rechecks identity/counters after the write. A second queue event freezes again. | OBSERVED: recovery, disk-failure, identity, clock, integrity and receipt-race tests passed. Actual live future-expiration admission is NOT YET VERIFIED. |
| Structural estimate lacked committed calculation identity | Loaded the retained lifecycle repair that binds the structural estimate to the exact durably committed source calculation; missing, expired or mismatched identities continue to abstain. | OBSERVED: regression tests and loaded source proof. A new eligible live structural estimate remains NOT YET VERIFIED before the market opens. |
| Passport publication and database contention | Loaded bounded passport-write retry and runtime-reader isolation. Post-close settlement now checks the necessary existing tables and avoids the broad database initialization/reconciliation path. General ingestion retains its explicit initialization option. | OBSERVED: contention/reader tests and real settlement succeeded. Broad initialization contributing to October 5 contention is INFERRED; the exact historical competing writer was not proven. |
| One unavailable official publisher prevented retaining the other successful retrievals | Source acquisition now attempts each publisher independently and appends a hash-addressed outcome receipt, including failed/missing-source states and actual retrieval times. Partial evidence never becomes an ingestible official bundle. | OBSERVED: all-source failure and mixed successful/failed retrieval tests passed. October 5 settlement subsequently succeeded using the existing fully verified candidate bundle. |
| Startup displayed API unavailable although authenticated access worked | The passport check's three-second deadline was shorter than an observed successful response (3.56 seconds). It now uses bounded two-second connection and ten-second response deadlines, reports timeouts separately, and preserves authentication failure handling. | OBSERVED: authenticated HTTP 200 after the change; ten related tests passed. |
| Historical forecast comparison disappeared when there was no eligible current forecast | Added `backend/eod_retrospective.py` and `--completed-sessions-only` to `tools/evaluate_eod_candidates.py`. It needs no live API. It scores frozen candidates against exact-session verified closes, separates unverified issuance, rejects altered/conflicting records, and compares each candidate with the baseline at the same origins. Current-date completed sessions can be scored after publication. | OBSERVED: 25 focused tests plus 69 forecast integration/horizon tests passed; immutable comparison report produced. No training or promotion occurs. |

The socket ownership implementation follows the OS connection identity fields documented in [Microsoft's TCP owner-PID structure](https://learn.microsoft.com/en-us/windows/win32/api/tcpmib/ns-tcpmib-mib_tcprow_owner_pid). An open port alone still does not establish ownership.

## Runtime postconditions

At 03:46 CT, October 6:

- OBSERVED: backend `/health` and `/health/live`, research services on 8001/8002, main dashboard on 8501 and companion dashboard on 8502 all returned HTTP 200. Process ownership was separately inspected.
- OBSERVED: all 72 backend startup source hashes matched the current files. Both research services matched the current research fingerprint after their own source refresh. The main backend was preserved during that research-only refresh.
- OBSERVED: the main feed reported `preopen_wait`, generation 0. It was deliberately not subscribing at this hour. This is not proof of fresh market quotes or forecast eligibility.
- OBSERVED: Windows reported synchronized time without a leap warning. Provider timestamp calibration still needs its own live evidence.
- OBSERVED configuration: enabled AutoStart at 07:00 CT; watchdog at 07:50 CT; official-close selection at 15:05 CT; settlement retry at 06:30 CT. Scheduled configuration does not prove the upcoming execution.
- OBSERVED: pre-open preparation disclosed `PRIOR_SESSION_FALLBACK`, source October 5, target October 6. It did not claim yesterday's cache was today's live quote stream. Current-day universe preparation and actual live subscriptions still require verification in the permitted window.

The main application is [available locally](http://127.0.0.1:8501/); the companion research view is [available locally](http://127.0.0.1:8502/).

OBSERVED follow-up at 03:51 CT: all six endpoint checks still passed, all 72 backend files still matched, and both research workers retained their process identities and completed six additional collection cycles since the 03:46 sample. These off-hours cycles are service-progress evidence, not market-quote or forecast-persistence evidence.

## Verified October 5 closes and diagnostic scores

The immutable selection receipt was preserved and its hash matched. The existing guarded settlement runner appended official-close observations and diagnostic scores at approximately 02:12:59 CT on October 6. It used official source artifacts retrieved at approximately 01:50–01:52 CT, rather than inventing an earlier publication or retrieval time.

| Symbol | Forecast time, October 5 CT | Selected estimate | Official close | Absolute error, points |
|---|---|---:|---:|---:|
| SPX | 02:46:20 PM | 7,773.29268292683 | 7,773.95 | 0.65731707317 |
| NDX | 02:54:54 PM | 31,005.661944047606 | 31,076.44 | 70.77805595239 |
| RUT | 09:08:55 AM | 2,841.191410071195 | 2,847.1357 | 5.94428992880 |

VIX 15.52 and SPY 774.83 were also captured as official closes. The RUT comparison uses an early-morning forecast; it is not a close-time estimate. These are individual diagnostic research scores, not a calibrated performance claim or proof of decision-grade accuracy.

Evidence: `verified-close-postconditions.json`; settlement receipt `data/postclose_pipeline/2026-10-05/b5eee9f67466f6a09758a0cc659f5a8c7f135212bf4ea71da880295caf500235.json`; selection `data/prediction_score_selections/2026-10-05/e7fe4a34a663ac14d6b4e917ed6ee5eeea52777b555bae599f05681171d3307a.json`.

## What the accuracy evaluation actually supports

OBSERVED: the separate research journal contained 5,574 hash-checked frozen EOD forecasts. The completed-session comparison found 7 independent SPX sessions and 9 NDX sessions with qualifying close evidence across the available lead buckets. All scored historical rows were in the unverified-issuance diagnostic cohort; the old journal did not retain the newer post-commit receipt proof. Repeated intraday rows were not counted as independent sessions. Missing/conflicting close evidence excluded 1,858 rows.

For example, in the 45–60 minute lead bucket, damped momentum had MAE 6.2034 versus 5.4620 for its matched SPX last-price baseline, and 30.8091 versus 27.2289 for NDX (six sessions each). That candidate was worse on those diagnostic comparisons. Other candidate results vary by symbol, lead and available components; those small historical comparisons are not a basis for picking a new production winner.

The current research collector records new forecast commits prospectively. The adaptive candidate still requires 20 qualifying training sessions plus 20 later walk-forward test sessions and its improvement screen. Historical close backfills retain actual availability times and cannot enter earlier training windows. More frequent snapshots or CUDA cannot manufacture independent sessions.

## Remaining constraints and October 6 acceptance checks

1. NOT YET VERIFIED: current-day universe, valid SPX/NDX live paired quotes, provider-clock acceptance, and persistence advancement across at least two intervals. An HTTP 200 is insufficient.
2. NOT YET VERIFIED: exact first opening bucket and each 5/15/30/60-minute ORB. October 5's missing opening samples remain missing. Late provider arrival can still make an exact bucket unavailable; do not infer it or silently replace it with later prices.
3. NOT YET VERIFIED: live ETF indicator entitlement. A bounded historical access check retrieved two SPY/QQQ bars successfully; the prior live `license_denied` response is a separate condition. No new entitlement was bought and index parity was not relabeled as observed OHLCV.
4. OBSERVED unresolved model artifact: the retained SPX checkpoint has bare `net.*` tensors, no ordered feature contract, a different architecture, and accompanying metadata for distance-to-pin rather than the current EOD target. It remains rejected. A compatible EOD model requires a correctly versioned feature/target pipeline and causal validation; filling in guessed metadata would be incorrect.
5. NOT YET VERIFIED: an accuracy improvement. Existing eligible research forecasts can run with fresh inputs, but neither these repairs nor one favorable SPX error establishes reliable forecasting skill. TCBBO promotion and other evidence gates remain intact.
6. OBSERVED retained external issue: the auxiliary Codex logon task last returned 1. The market autostart and settlement tasks are distinct and last returned 0. The Codex task was not treated as market-capture authority or modified.

## Validation record

Retained successful runs include Python suites of 106, 153, 61, 25, 69 and 10 tests, plus 208 supervisor/launcher checks and the final 17-check manual-launcher suite. Suites overlap; these counts must not be summed as unique tests. Initial test-collection errors and failed startup attempts were retained separately. Named-file whitespace validation passed. Runtime evidence, diagnostic authentication, loaded fingerprints, exact official-close comparisons and the immutable evaluation receipt supplement the offline tests.

The October 5 finalized tape and original assessment/selection receipts were preserved. The repaired system is running in its overnight waiting state; complete live readiness and improved accuracy remain separate acceptance criteria.
