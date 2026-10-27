# Stage 1 results and Stage 2 implementation scope

Date: September 25, 2026. Workspace: `C:\Cprojectsgpu_app\MarketPinPredictor`.
Stage 1 is implemented and tested in source. Dashboard activation is not verified.
The Stage 2 outline below was recorded before implementation; no passport retrieval
or forecast binding was added in Stage 1. Stage 2 is now implemented separately:
see [Stage 2 results](AI_STAGE2_RESULTS_2026-09-25.md).

## Stage 1 changes

| File | Implemented behavior |
|---|---|
| `app/services/diagnostic_findings.py` | Derives typed, evidence-cited facts. Research-only output is a limitation; future closing windows are not due; missing opening proof is unverified. Optional invalid symbols do not imply a required-symbol veto. Reference source is labeled derived only when the source field establishes that; missing provenance stays unknown. |
| `app/services/diagnostic_agent.py` | The model selects fact and investigation IDs. Application templates render all wording, numbers, categories and summary; mandatory caveats cannot be omitted. Validates selections locally and saves v2 input, raw model output, selected IDs and rendered review. Records prompt/schema/rule versions and hashes. Separates current and legacy rendering. |
| `app/services/diagnostic_runtime.py` | Carries explicit collection start/end and backend sample time, required/optional roles, and allowlisted source/identity fields. Preserves bounded two-observation collection and explicit endpoint failure behavior. |
| `app/services/diagnostic_history.py` | Separates request-time collection from retained observations and quote freshness. Reads old v1 reviews without rewriting them. Shows automatic-job outcome and latest saved review independently. |
| `tests/test_diagnostic_findings.py` and existing diagnostic tests | Exercise misclassification, stale quotes despite fresh polling, optional roles, missing provenance, generation reset, early close, legacy retention, selection rejection and Streamlit rendering. |

The AI's Stage 1 role is prioritization of evidence and predefined investigations.
It cannot invent a diagnosis outside the catalog or freely propose a new correction.
This is a deliberate limit and is disclosed in the UI. Numeric forecast explanations
are deferred to Stage 2; the exact observed dashboard values remain in the saved packet.
No claim is made that this wording change improves prediction accuracy.

## Verification

- **67 focused tests passed**, including Streamlit AppTest rendering for old and new
  reviews and calendar-aware early-close classification.
- A real request completed using **gpt-5.6-luna** at
  `2026-09-25T18:19:57.672807+00:00` (13:19:57 Chicago time).
- Evidence packet created at `2026-09-25T18:19:39.708165+00:00`; last runtime
  observation at `2026-09-25T18:19:39.567613+00:00`. All recorded endpoint reads succeeded.
- Exact review saved and read back at
  `data/diagnostic_reviews/2026-09-25/23ea79b20402465f8969fcf26543ad7eb71d03cf3d18bc18f5a64459c283ea73.json`.
- Final **SAVED** receipt, including review path, at
  `data/diagnostic_receipts/2026-09-25/3996c9a583e9859ff066d433e1f043e453da75179a361c3661b735f3e06c5311.json`.
  The immutable review embeds the earlier VALIDATED receipt; the separate final
  receipt records completion of persistence and links to its content-addressed file.
- The report correctly separated research estimates, optional invalid symbols,
  a stale VIX input, and the not-yet-started closing window. It kept missing SPX/NDX
  reference-source provenance unverified rather than guessing from the provider.
- A final summary grammar adjustment and empty-issues UI message followed the real
  request. Replaying its saved selection against the final renderer produced identical
  findings and investigations; only summary wording changed. The original review was
  preserved. The final 67-test run includes those changes.

Command used for the focused suite (with the canonical absolute DATABASE_URL set):

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_diagnostic_findings.py tests/test_diagnostic_live_review.py tests/test_capture_review_agent.py tests/test_diagnostic_history.py tests/test_diagnostic_automation.py tests/test_advisor_workflow.py tests/test_live_advisor.py -q
```

Live delivery boundary: port 8501 belonged to PID 54884 when checked. Windows returned
null executable path and command line, and `.streamlit/config.toml` has
`fileWatcherType = "none"`. The real request verified the updated source through the
CLI, not the existing dashboard process. No service was restarted. Loading the changes
into the dashboard requires a verified frontend owner and a controlled frontend reload
or restart, followed by visible v2 rendering. Backend capture need not be restarted.

## Stage 2: bind explanations to the exact forecast

### Intended user behavior

The existing refresh-and-review action will collect current diagnostics and, when a
dashboard forecast has a verifiable immutable identity, attach its matching passport.
The report can then explain the exact displayed estimate, its target, source freshness,
authority and available calibration. It must say when an estimate cannot be bound.

An explanation will display issue time and target time together, identify the reference
price source, and distinguish forecast authority from measured historical accuracy.
Heuristic confidence must never be presented as a calibrated probability.

### Implementation sequence and code locations

1. **Map the existing forecast contract.** Inspect `backend/workstation.py`,
   `backend/prediction_passport.py`, and the dashboard response adapter. Establish the
   actual locations of forecast ID, snapshot/calculation ID, symbol, model version,
   subscription epoch/generation, source time, prediction time and target time.
   Use real contracts; a provider name alone is not a price-source kind.
2. **Add a bounded read adapter.** Add a UI-independent
   `app/services/diagnostic_forecast_binding.py` module. Reuse the authenticated,
   read-only `GET /v1/workstation/passports/{forecast_id}` contract from
   `backend/api/routers/passports.py`, with existing governance authorization.
   Never transmit that credential to the model or receipts. Missing authorization,
   timeout, missing passport and identity mismatch must have distinct result codes.
   Set a small per-review cap and deduplicate IDs; no replay or write endpoints.
3. **Verify identity before allowing numeric claims.** The adapter must validate the
   immutable passport integrity and its relationship to the observed symbol,
   snapshot/calculation/model lineage, source and target timestamps, and runtime
   generation where applicable. Use existing verification functions where possible.
   Distinguish identity mismatch from an older, correctly bound forecast whose source
   is stale for a current-use question. Neither is fresh eligible evidence.
4. **Integrate one explanation path.** Extend `diagnostic_runtime.py` and
   `diagnostic_agent.py` to attach the bounded binding result and retrieval receipt.
   Reuse the typed claim/path verification and numeric rendering pattern in
   `backend/passport_analyst.py`; extract a reusable pure verifier if necessary.
   Retain one model call for review selection rather than launching another model
   call for each symbol. Preserve Stage 1 operational diagnostics on binding failure.
5. **Render and retain the binding.** Add forecast issue/target times, identity,
   authority, reference-source label, freshness and calibration availability to the
   UI. Save the exact passport evidence, retrieval outcome, binding decision and
   supported claims alongside the existing exact packet and model output. Extend
   review schema/version dispatch without rewriting older reviews.

If an estimate lacks a matching passport, display **Unbound research estimate**.
Do not create a passport from dashboard numbers, substitute the latest same-symbol
passport, or infer missing forecast dependencies from global required-symbol policy.

### Acceptance gate before Stage 2 is considered complete

- Positive fixture: matching forecast identity yields only verified numeric claims.
- Negative fixtures: different forecast with same symbol, different symbol/horizon,
  mismatched snapshot/calculation/model/generation, unavailable source, inconsistent
  reference, stale evidence, malformed passport, authorization failure and timeout.
- A forecast changing during collection cannot acquire another forecast's passport;
  retain the observed identity and explicit mismatch/unverified result.
- Invented model numbers and unsupported evidence paths fail local validation.
- Missing calibration stays unavailable, not zero confidence or a hit probability.
- Original operational diagnosis still works when passport retrieval fails.
- Exact evidence and final receipt are saved/read back; old v1/v2 reviews remain readable.
- Extend `tests/test_passport_analyst.py`, `tests/test_passport_api.py` and diagnostic
  tests; add dedicated binding tests. Verify one real matched request if available,
  or explicitly report that only the unbound/failure path has live verification.

Stage 2 does not train or promote a model, recover missing observations, relax input
validation, add historical retrieval, or implement an AI forecaster. More evidence can
make an explanation better supported; predictive improvement requires separate
held-out evaluation in the later research stage.
