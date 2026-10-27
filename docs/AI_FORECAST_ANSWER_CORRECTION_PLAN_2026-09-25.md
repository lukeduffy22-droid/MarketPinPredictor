# AI forecast-answer correction plan

## Evidence and diagnosis

Reviewed the exact saved v3 review:
`data/diagnostic_reviews/2026-09-25/9464e5df15d3bd7166f2cd07a0400bc8dda9e9fe9f68050ec790b83d39e722da.json`.
Its filename/content hash verifies. OpenAI returned `completed`, actual model
`gpt-5.6-luna`; runtime was observed at `2026-09-25T19:42:56.498346+00:00`.
This was a successful AI request with blocked forecast evidence, not a JSON or model failure.

1. **Passport authorization is unconfigured.** Saved SPX/NDX receipts report
   `AUTHORIZATION_MISSING` with zero network elapsed time: the client stopped before
   calling the passport endpoint. This diagnostic environment and its `.env` also lack
   the credential. A new unauthenticated read of the exact SPX passport endpoint returned
   HTTP 503. The router's access dependency returns this when governance authorization
   is unconfigured. No credentials were printed or changed.
2. **Stage 2 rejects a supported live origin.** Both exact IDs were located in the
   configured local database using SQLite `mode=ro` and `query_only`. Their immutable
   origins are `lifecycle_revision`, with `prediction_snapshot_id=null`. This agrees with
   `backend/api/lifecycle.py::_write_revision_passport`. The current verifier treats any
   null snapshot ID as missing evidence. Replaying the saved dashboards and these local
   records through `verify_binding` returns `BINDING_EVIDENCE_MISSING`, field `snapshot_id`,
   for both SPX and NDX. This is a verifier bug; merely configuring the credential will
   not fix it. The local database lookup is debugging evidence, not a replacement for
   authenticated runtime retrieval or proof of the running backend's database identity.
3. **The response format does not directly answer the question.** The model selects IDs
   from a catalog; the app generates the generic operational summary. A request for
   SPX/NDX closing estimates receives operational categories with the binding blocker
   buried underneath. Zero market-data/application issues does not mean the forecast
   explanation integration is ready.
4. **Price-source provenance is missing from this packet.** SPX/NDX dashboard
   `pin_payload.spot_source` is absent. The collector already allowlists that field;
   widening the allowlist alone will not repair an absent upstream value. Do not infer
   cash-index spot from a provider label.
5. **An unrelated warning appears directly after AI history.**
   `advisor_view.py` renders the old-source warning after `render_diagnostic_agent`.
   It concerns previously recorded proposal selections and their source fingerprint,
   not this newly saved review. Preserve the safeguard but move and relabel it.

The closing-window statement is correct for the recorded observation: 19:42:56 UTC
precedes the final-15-minute start at 19:45 UTC for that session. The earlier 19:39
timestamp belongs to the preview, not the submitted packet. Neither should be interpreted
as a current observation when reopening this saved report.

## Priority 0 — repair the blockers together

### A. Origin-aware binding, offline first

Files: `app/services/diagnostic_forecast_binding.py`,
`tests/test_diagnostic_forecast_binding.py`; inspect/reuse the contracts in
`backend/api/lifecycle.py`, `backend/prediction_passport.py`, `backend/workstation.py`.

- Explicitly dispatch supported origin kinds. For `prediction_snapshot`, require the
  matching non-null snapshot ID. For `lifecycle_revision`, allow null snapshot IDs only
  under that documented origin contract, while requiring the immutable origin key,
  forecast ID and content integrity plus existing symbol/calculation/model/epoch/
  generation/value/timestamp/target checks. Reject unknown origins and inconsistent
  non-null IDs. Do not broadly waive all missing identity fields.
- Add a sanitized lifecycle-origin fixture reflecting the saved SPX/NDX shapes;
  label it reconstructed. Keep exact original reviews immutable. Test both origins,
  wrong origin key/hash, same-symbol wrong forecast, null IDs in snapshot origins,
  conflicting duplicate fields, stale data, wrong horizon and selected-session mismatch.
- Run the stored real evidence through the corrected pure verifier as an offline
  regression. This is not sufficient for live activation acceptance.

### B. One shared private credential and an explicit readiness check

Files: `diagnostic_forecast_binding.py`, `diagnostic_runtime.py`,
`diagnostic_agent.py`; credential loading in `backend/config.py` and the verified
launch path (`start_databento_app.ps1` and its actual child-process configuration).

- Verify whether any active process already has a valid credential before generating
  one. Reuse it when available; otherwise securely provision one shared value into the
  backend and diagnostic frontend configuration. Never include it in packets, model
  prompts, screenshots, receipts or logs. Keep loopback restrictions and authentication.
- Verify the loading path for each process. Writing `.env` does not prove an already
  running process has loaded the change.
- Add a safe readiness indicator separating OpenAI request availability from passport
  authorization/retrieval and forecast binding. For forecast questions, explain the
  blocker before spending a model request on a guaranteed numberless result. Preserve
  operational diagnosis as an available capability when passport access fails.
- Activate only after verifying process ownership. Preserve ongoing capture; do not
  blindly restart an unknown process. Credential activation in an unconfigured backend
  may require a controlled backend restart, unlike a frontend-only renderer change.
- Acceptance: authenticated exact-ID read succeeds against the actual backend, then the
  final collector and verifier produce a matching result for an available valid origin.
  Correct refusal/unbound behavior remains required for invalid or stale current-use inputs.

## Priority 1 — answer the requested question first

Files: `diagnostic_agent.py`, `diagnostic_findings.py`, UI regression tests.

- Introduce an explicit forecast-explanation presentation alongside operational diagnosis.
  Retain requested symbols and question/task intent in the review contract; do not allow
  the model to invent prices or infer a new forecast from general market knowledge.
- Lead a supported answer with a compact table: symbol, bound research closing estimate,
  issue time, target time, reference/source label, freshness, and authority/calibration
  limits. Numbers must come from verified passport paths.
- If blocked, lead with the specific reason and required correction, e.g. “SPX and NDX
  estimates cannot be verified because passport access is not configured.” Do not bury
  that below “0 issues.” Keep market input health and AI integration readiness separate.
- A research point estimate is not automatically a calibrated “most likely close.” Say
  what the stored estimate is and whether empirical reliability is known.
- Keep detailed diagnostics/citations available below the direct answer. Make preview
  timestamps subordinate to the submitted observation time, and retain report-age labels.

## Priority 1 — repair provenance and warning placement

Files: `backend/api/lifecycle.py`, `backend/workstation.py`, the actual dashboard
response path, `diagnostic_runtime.py`, `diagnostic_findings.py`, `advisor_view.py`.

- Trace why the SPX/NDX lifecycle payload lacks the explicit source kind even though
  relevant streamer paths record it. Carry the contemporaneous, calculation-bound
  source field into new observations without inferring it or rewriting old passports.
- Move the fingerprint warning into the recorded-proposals panel. Suggested wording:
  “Previously selected change proposals were saved against different source code.
  Review them again before implementation. This does not invalidate saved AI reviews.”
- Preserve approval invalidation for changed code; never automatically reapprove a
  proposal just to remove the warning.

## Completion gate

1. Regression tests pass for both actual passport origins, credential failure paths,
   verified numeric paths, answer-first UI and separately labeled proposal history.
2. A real authenticated request retrieves and binds the observed forecast IDs. Save
   the exact evidence and safe final receipt; read the review back and verify its hash.
3. The running dashboard visibly shows the corrected answer and the same IDs/issue/
   target times as the saved artifact. Source tests alone are not this proof.
4. If the session has ended, identify the output as that retained session's estimate;
   do not present it as a fresh pre-close prediction or regenerate history after the outcome.
5. Existing capture, forecast authority, immutable artifacts and unrelated dirty work
   remain intact. No new model promotion, trading automation or historical forecasting
   subsystem is part of this repair.

This document records debugging and the correction plan. No production code, credential,
review artifact or running service was changed in this debugging turn.
