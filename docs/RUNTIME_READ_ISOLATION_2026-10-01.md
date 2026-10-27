# Symbol and health read isolation — October 1, 2026

## Outcome

Implemented and activated a backend change that prevents forecast persistence from blocking current runtime-identity and active-session health reads. **328 offline tests passed in 17.73 seconds.** Following the user's explicit activation request, the guarded backend restart completed at **14:36:31 CT**. The loaded streamer hash matches the tested source.

## Evidence and diagnosis

The user's screenshot records `RUNTIME_READ_TIMEOUT` at 14:13:35 CT for VIX, with NDX also showing an HTTP error. Those are bounded backend read failures; they do not establish a VIX price direction or a specific chain-validation failure.

At 14:16 CT, a new local read returned NDX's research-only prediction in 0.047 seconds while VIX returned a separate, fast validation abstention. At 14:25 CT, both dashboard symbol requests returned HTTP 200; NDX remained research-only, and VIX failed its chain requirement with one primary-expiration strike against a minimum of 30. Live messages advanced from 10,659,717 to 10,799,522. Thus the timeouts are intermittent, and a successful read does not make VIX's evidence eligible.

Source inspection identified a shared wait: `prediction_publication_guard` holds `_prediction_publication_lock` through forecast persistence, while identity getters and health used the same lock. API projections need current process/generation/handoff identity to validate retained payloads; waiting for an unrelated commit can exhaust their one-second deadline.

An offline threaded reproduction confirmed that the unmodified context, generation, handoff, and health reads all waited for a deliberately delayed publication. Four tests failed before the repair; the generation-transition serialization test passed. This proves the blocking mechanism, not that every observed production timeout has this single cause. No production thread stack was captured.

## Change

- Add a short, memory-only runtime-context lock for identity reads and writes. Generation and handoff writers still acquire the original publication barrier first, preserving forecast commit ordering.
- Make runtime-context and identity getters independent of the persistence barrier.
- Let active-session health snapshot identity without waiting for forecast persistence. An actual warming-to-degraded transition still uses the original barrier and rechecks its condition.
- Preserve API freshness, process/generation, validity, and passport requirements; no prior response is cached as current and no read timeout is increased by this change.

The original publication lock still protects identity transitions and other existing critical operations. This patch does not eliminate all possible lock waits, slow calculations, SQLite contention, or provider slow-reader causes. It does not change subscriptions, formulas, model gates, close records, or the recorder start policy.

## Verification

Canonical `.venv`, offline fixtures and an absolute test-only `DATABASE_URL` under `output/runtime_read_isolation_20261001/`:

- A delayed forecast commit permits identity and active health reads to complete.
- A generation change still waits for the commit; an old-generation payload becomes ineligible immediately after the transition.
- In-process FastAPI requests complete within a 0.15-second read budget while publication is held. Fresh eligible fixtures remain readable; stale, invalid and mismatched-generation fixtures still abstain.
- The actual mocked quote consumer and health reader both progress while persistence is delayed.
- Existing publication-order, Databento-only, workstation, API isolation, clock and staged-subscription regressions pass.

The first broad run found a scheduler test assumption exposed by the faster identity reads: it required a new worker on every scheduler wait, even while the previous worker was still returning. The fixture now allows normal polling until the two required attempts complete, with a bounded deadline. Its assertions still require attempts for generations 7 then 8 and acceptance of generation 8 only. The final expanded run passed all 328 tests.

`git diff --check` passed. The staged diff SHA-256 remained identical to its pre-change value. Existing work was preserved; nothing was staged or committed.

Evidence: `output/runtime_read_isolation_20261001/baseline.json`, `before-tests.xml`, `focused-tests.xml`, `tests.xml`, and `live-postconditions.json`. Source baselines are retained in the same output folder.

## Activation and limits

The initial implementation and tests were offline. The subsequent user-authorized activation used the canonical guarded backend restart with verified ownership and the expected listener PID. Backend PID 41700 was replaced by verified PID 52440; dashboard PID 29916 stayed running. SPX/NDX/VIX/RUT, cbbo-1s, near-term-shadow, zero replay minutes, and the current-day universe source hash were preserved. The backend portion of the earlier ORB change is also loaded; earlier dashboard-only source changes were not independently activated by this backend restart.

Loaded streamer SHA-256: `6eaa17d719b9906c5ca4fcf3cfc1eefd1dea12c32e49b66e88731f5652e204fb`.

Post-startup samples through 14:39:30 CT showed messages advancing from 4,282 to 43,943 and no `RUNTIME_READ_TIMEOUT`. Health and symbol-dashboard reads completed in 4–95 ms; ORB reads completed in 0.404–1.513 seconds. SPX and NDX resumed valid current-generation research predictions. A later SPX request correctly abstained because only 24 qualifying strikes were available against its minimum of 30; VIX also remained invalid. These are separate validation failures, so the session is not unconditionally healthy.

Read-only pre/post checks returned SQLite WAL mode and `quick_check=ok`. Calculation runs/input blobs increased from 23,230 to 23,245; audit rows from 41,401 to 41,425; prediction rows from 14,337 to 14,347; market snapshots from 16,099 to 16,109. New SPX/NDX rows carry the new subscription epoch and generation. The 15-minute gamma-pin table remained at its 14:30 bucket; its next scheduled bucket was outside this observation interval. Current-process skip/slow-reader/queue-full counters remained zero in sampled health. Restarting counters does not erase the earlier session's warnings or repair missed data.

Activation evidence is retained under `output/runtime_read_isolation_20261001/activation_1432/`. `verification.json` reconciles process ownership, source hash, unchanged staged diff, configuration, reads, and persistence. The temporary wrapper could not retrieve its exited child's Windows `ExitCode` and recorded null; the canonical invocation log independently confirms `outcome=success`, `normalized_exit_code=0`. No additional restart was performed to work around that receipt issue.

VIX may still legitimately show ABSTAIN after activation because its chain is too thin. The missing closing-tape capture and older close-archive artifacts are separate unresolved issues documented in `CLOSING_TAPE_PANEL_DIAGNOSIS_2026-10-01.md`.

### Later activation on October 1

The NDX undefined-price repair retained this lock-isolation change and was activated at 14:56:48 CT in backend PID 27484. Its loaded streamer hash is `f30c3baaf570017459a744c8bb22292d39a544eef54ef1531e3bf6e48c38bb38`, superseding the earlier hash above. Dashboard PID 41628 now visibly renders the ORB changes. See `NDX_ORB_UNDEFINED_PRICE_REPAIR_2026-10-01.md` for the tested source, guarded activation, corrected live ranges, and continuing forecast-validity limitations.
