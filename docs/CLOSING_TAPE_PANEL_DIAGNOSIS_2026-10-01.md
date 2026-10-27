# Closing-tape panel diagnosis

Observed October 1, 2026, approximately 13:16–13:22 America/Chicago. Source changes are prepared offline; running services have not been restarted.

## Findings

- `/closing-tape/status`, `/closing-tape/readiness`, and `/closing-tape/production-predictions` returned HTTP 200. The screenshot's missing counts are not the earlier ORB endpoint timeout.
- There is no October 1 closing-tape directory/catalog or Python recorder process. The watchdog is running, but repeatedly records `primary_capture_not_ready` and preserves the existing backend without launching a recorder.
- The read-only start check at 13:19:45 CT returned two blockers: `PRIMARY_HEALTH_PROVIDER_SKIPPED_RECORD_WARNINGS_NONZERO` and `PRIMARY_STAGED_SUBSCRIPTION_STATE_UNSAFE`. A subsequent health observation reported 13 skipped-record warnings and subscription staging `canceled`. These are safety conditions, not a missing admin privilege or a model-training prerequisite. The exact number of lost records was not established.
- The normal close-minus-15 analysis boundary is 14:45 CT today. Being before that time does not explain a missing catalog: capture should already have been running when eligible.
- Readiness reports 8/10 independent verified sources, 7/60 labeled sessions, and 0/20 paper sessions. No frozen model manifest or promoted prediction exists. Passing the capture gate alone does not promote a model.
- September 30's completed source is the additional unlabeled eligible session; its label queue lacks all five official family closes. Completing those labels could add one labeled session, not the other 52 required to reach 60.
- The latest-close inventory reports 73 sessions, but the stricter status check validates all verified ledger rows, retained bytes, and correction history. Of 397 verified ledger rows, 32 lack a file matching their recorded artifact hash in the expected date/family vault directory. The first failing record is NDX, August 25, observation 2. This inventory establishes absence at the expected path, not deletion or the availability of recoverable copies elsewhere. The stricter check correctly remains unavailable.
- The verified-close settlement task's latest reported result was 1 for September 30 at 17:45 CT. Its underlying failure was not investigated in this scoped panel diagnosis.
- The broader backend remains intermittently degraded: a later health read at 13:22 CT returned HTTP 503. The panel diagnostics do not repair publication/read contention.

Retained local read evidence: `output/closing_tape_panel_20261001/evidence.json`. No official-close sources were fetched and no ledger rows were modified.

## Source changes

- Explain when the requested day's catalog is absent, independently of model eligibility and transport failure.
- Replace clipped `Unavailable` metric values with a dash and an explicit unavailable-not-zero explanation.
- Explain that accumulated readiness totals span dates and do not prove full close-archive validation.
- Include date, family, and observation ID in retained-close artifact failures while keeping validation strict.

The recorder start conditions, model thresholds, formulas, subscriptions, and production data remain unchanged. Existing staged and unstaged work was preserved; nothing was staged or committed.

## Validation

Run with the canonical `.venv` and an absolute test-only `DATABASE_URL`:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_live_panel_view.py tests\test_closing_tape_status.py tests\test_closing_tape_start_gate.py -q --junitxml=output\closing_tape_panel_20261001\tests.xml
```

Result: **94 passed in 102.04 seconds**. The scoped `git diff --check` also passed. These tests use offline fixtures and do not activate the running app. Interpreter imports required elevated read access to the existing `.venv`; no packages were installed or changed.

## Remaining activation and recovery

1. Load the tested API/dashboard source during a guarded maintenance window, verifying process ownership and loaded source afterward. The dashboard's production file watcher is disabled, so saved source alone does not activate the change.
2. Resolve the primary feed's skipped-record and canceled-staging conditions before allowing an additional recorder connection. Re-run the read-only start gate and verify current-day catalog creation, advancing DBN/status records, and final integrity after an authorized capture launch. Do not reset counters or weaken the guard to make it pass.
3. Locate and hash-verify any recoverable copies of the 32 missing close artifacts, and investigate the September 30 settlement failure. Restoring/reconciling production evidence is a separate controlled action; this task neither invents replacement bytes nor rewrites original observations.
4. Accumulate independent clean, officially labeled sessions, then satisfy validation, paper evaluation, and model-promotion requirements. Refreshing the panel cannot create those records.

## Follow-up: first start blocker and recovery inventory

Read-only follow-up on October 1 established the sequence:

- Recorder startup intentionally waits until 09:35 CT to protect the primary opening-hour capture.
- `logs/runtime/backend.stderr.log` records Databento `skipped_records_after_slow_reading` for `cbbo-1s` at 09:03:27, 09:18:53, and 09:22:08 CT, before that first permitted start.
- `logs/runtime/watchdog.log` records `primary_capture_not_ready` at 09:35:10 CT and subsequent attempts. The watchdog's historical message omits individual issues, so the two precise blockers were established by the later read-only gate check, not reconstructed as an exact 09:35 payload.
- The gate requires cumulative skipped-record counters to equal zero and rejects canceled staging. Therefore, a subsequently advancing primary feed alone cannot clear the recorded conditions. The upstream reason the consumer fell behind (compute, persistence, scheduling, or another bottleneck) is not established by the provider error alone.

The 32 missing rows cover 30 symbol/date pairs over August 25, 26, 27, 28, 31 and September 2. Breakdown: NDX 7, SPX 7, RUT 6, SPY 6, VIX 6. This same missing-vault problem was documented on September 17 in `docs/verified_close_reconciliation.md`.

A bounded local recovery scan enumerated 3,919 files across eight existing evidence roots in the canonical/sibling checkouts and parent evidence folders. It hashed 1,576 relevant candidate files (548,392,298 bytes), with no read errors or scan limit reached, and found **no exact original hash matches**. This was not a whole-disk, cloud, or compressed-backup search. Scope and results: `output/closing_tape_panel_20261001/recovery_inventory.json`.

For every affected pair, a newer verified ledger record exists with retained files matching its own SHA-256. All 32 older prices exactly match their newer counterpart. This is price agreement and byte-integrity evidence, not proof that the older missing artifacts existed or that all official-source and correction-lineage checks pass. Comparison: `output/closing_tape_panel_20261001/replacement_comparison.json`.

Recovery order:

1. If exact original bytes can be recovered from another backup, validate their recorded hashes and official source/date/symbol semantics, then restore them through a reviewed append-only evidence operation. Revalidate correction lineage afterward; restoring bytes alone may not resolve duplicate roots.
2. If original bytes cannot be recovered, keep those older records explicitly unverifiable. A new, tested append-only adjudication path would need to identify each affected record and independently validated replacement, preserve prices/timestamps/hashes, and distinguish usable replacement evidence from incomplete historical provenance. The current alias-reconciliation tool cannot do this: it requires the missing bytes, and duplicate-root reconciliation also validates both artifacts.
3. A new download or equal-price replacement must never be renamed or relabeled as an old artifact. It has its own hash and observation time. Since newer retained records already cover every affected pair, first validate those records rather than blindly importing another duplicate set.

This follow-up changed no production records, vault files, services, provider connections, or gate criteria.

