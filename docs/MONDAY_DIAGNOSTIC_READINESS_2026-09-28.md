# Monday diagnostic readiness and corrective actions

Prepared September 25, 2026. Source/configuration changes are implemented and tested;
activation and the next regular-session acceptance are still pending. This is not a
promise that unavailable market observations or invalid inputs will become valid.

## Implemented durable corrections

1. **One private passport credential.** `backend/governance_credentials.py` supplies
   the backend and diagnostic client from the canonical private configuration. Setup
   preserves existing credentials, rejects conflicting values, and never prints a
   secret. `tools/configure_diagnostic_access.py` provisions explicitly and checks
   authenticated access. The full-stack launcher provisions before process changes
   and checks the actual endpoint afterward. The local ignored `.env` is configured.
2. **Independent source identity.** Lifecycle forecasts without calculation IDs can
   bind only when a hash independently computed from the inference input matches the
   immutable passport source hash. The workstation and passport use the same canonical
   encoding. Snapshot-origin requirements remain strict; a missing hash never grants
   permission to explain a number. Binding version is `diagnostic-binding-v3`.
3. **Answer the forecast question first.** Verified retained estimates or exact binding
   blockers appear before the operational sections. This does not generate a new AI
   market forecast or claim calibrated accuracy. Archived reviews are not rewritten.
4. **Richer, bounded observations.** Allowlisted fields now include input lineage,
   expiry selection, paired-quote counts and coverage, chain breadth, concentration,
   quote age and context-only status. Existing freshness and validation gates remain.
5. **Repeatable acceptance.** `tools/verify_diagnostic_readiness.py` collects two new
   observations and requires both SPX and NDX to be fresh, valid, current-generation,
   exactly bound, with advancing messages and an active healthy pipeline. Failed
   endpoint collection blocks readiness. Off-hours never passes live acceptance.

Earlier fixes retained: shared manual/automatic input limits measured on the actual
submitted payload, completion/refusal/schema validation, final saved-receipt display,
specific validation reasons, and correct post-close/future-window classifications.

## Observed evidence and unresolved causes

The automatic job's 64,618-byte evidence exceeded its old 64,000-byte local limit;
there were zero API attempts. The unified submitted-input budget fixes that mismatch.
The historical failed job remains an honest record and was not reset or retried.

Friday's retained audit files show a subscription epoch transition around
19:24–19:27 UTC. The older epoch had SPX 338/338 valid audits and NDX 336/337.
The newer epoch had SPX 24/38 and NDX 29/32. Its closing-window audit counts were
SPX 6/16 valid and NDX 11/14. These are audit counts, not capture-attempt counts,
unique events, or proof of continuous coverage. The transition correlates with
startup log timestamps; causation of quote attrition is not established.

SPX went from 42 paired quotes in a valid 19:00 observation to four in its final
19:59:36 invalid observation. NDX went from 105 paired quotes to 18; its final
coverage was 18/245 = 0.07347, below 0.1. The latest persisted SPX input blob had
290 fresh rows but only five complete strike pairs. Definition admission was
complete (SPX 325/325 and NDX 404/404); this localizes the discrepancy to usable
current quote sides rather than proving a missing definition subscription.

VIX had no valid audits in either epoch. Its forward-expiry, context-only chain
is not a valid cash-index observation or a reason to veto otherwise valid required
symbols. No thresholds, quote ages, subscription permissions or models were relaxed.

The backend log also contains a checkpoint feature-contract error. Model readiness
requires investigating the checkpoint's ordered feature contract; do not substitute
weights or promote a model to make this error disappear.

Exact opening evidence, complete closing coverage and an official close remain
separate verification requirements. Missing observations cannot be reconstructed as
observed records by the diagnostic AI.

## Activation and Monday acceptance sequence

1. Establish executable/command-line ownership of the backend and dashboard under
   the canonical repository. Current Windows queries return no such identity for
   their listeners; no service was stopped or restarted during this repair.
2. Once ownership can be verified, use the normal full-stack launcher from the
   canonical checkout: `./start_databento_app.ps1 -NoBrowser`. Preserve its preflight
   and ownership guards. Do not kill a listener solely because it occupies a port.
3. Run `./.venv/Scripts/python.exe tools/configure_diagnostic_access.py --check-live`.
   Require `AUTHENTICATED` and HTTP 200. The September 25 check returned HTTP 503:
   private configuration exists, but the old running backend has not loaded it.
4. During Monday regular trading run
   `./.venv/Scripts/python.exe tools/verify_diagnostic_readiness.py --session-date 2026-09-28`.
   Require `READY_FOR_RESEARCH_EXPLANATION`; retain its timestamped output. A good
   HTTP response alone is insufficient. Separately compare persistence over at least
   the actual configured capture cadence and inspect exact opening coverage.
5. Run **Refresh live evidence and ask AI** once the access and binding checks pass.
   Verify a completed real response, final SAVED receipt, exact input/response
   read-back, actual model and location. No new paid request was made in this repair.
6. Investigate recurring paired-quote attrition with same-epoch, timestamped quote
   sides and rejection counts before changing ingestion. Compare missing opposite
   sides, quote ages, generation cutoffs and primary-expiry selection. Fix a proven
   data-path defect with replay tests; never hide genuine invalidity.

## Verification

- 247 focused Python tests passed, covering diagnostics, automation, credential
  reuse/conflicts, independent lineage matching/rejection, passport API, workstation
  behavior and readiness gates.
- 16 launcher guard tests passed.
- After adding the answer-first UI regression, its complete 19-test module passed.
- Read-only runtime check at 2026-09-25T20:41:37.754094+00:00 reported
  `OFF_HOURS_LIVE_ACCEPTANCE_PENDING`: messages advanced, but both core inputs were
  stale/invalid and passport access returned 503. This is not Monday acceptance.
- Immutable tapes, reviews and failed job records were preserved. Existing unrelated
  changes were preserved. No service restart, provider subscription change or model
  promotion occurred.
