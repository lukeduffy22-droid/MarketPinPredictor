# Stage 2 — exact forecast explanation

September 25, 2026. Source implementation complete; live matched-forecast verification
and dashboard activation remain pending configuration and verified process ownership.

## Implemented

- `app/services/diagnostic_forecast_binding.py`: authenticated, read-only exact-ID
  passport retrieval; at most four unique IDs per review, 24 KiB per response,
  connect/read timeouts, a checked collection deadline, no redirects, and no latest-ID
  substitution. Receipts contain fixed outcome categories, timing, status and response
  hash, never access headers or error bodies. Credential-like content is rejected.
- The adapter uses the existing passport API and `passport_contract_is_complete`
  for immutable origin/content verification. It compares the displayed symbol,
  forecast/snapshot/calculation identity, model version, epoch/generation, point and
  reference values, source and prediction times, official cash-close target and horizon.
  Duplicate identity fields are checked when supplied; missing required evidence
  prevents numeric claims. Matching stale forecasts remain bound but are explicitly
  stale for current use. A passed freshness threshold is not forecast eligibility.
- `diagnostic_runtime.py`: carries required timestamp/model/freshness fields and
  retrieves passports only for the last observed dashboard IDs. The first observation
  cannot replace a failed last observation. Operational diagnostics survive passport
  retrieval failure. No additional model call is made for a passport.
- `diagnostic_findings.py`: revalidates passport binding before rendering any numeric
  explanation. Reuses `backend.passport_analyst.verify_analyst_answer` and its verified
  evidence paths for numeric facts, authority, target, source age, model identity and
  intervals. Unknown reference-source provenance stays unknown. Calibration metadata
  alone stays unverified; absent calibration stays unavailable. Raw/heuristic confidence
  is never described as a calibrated hit probability.
- `diagnostic_agent.py` and `diagnostic_history.py`: version 3 saved reviews include
  the exact submitted packet, retrieved immutable passport core and metadata, binding
  decision/receipt, fact catalog, raw model selection and rendered claims. Binding
  version/source hash is recorded in request receipts. Input is capped at 192 KiB to
  accommodate bounded passports alongside operational evidence. Existing v1/v2 archives
  remain unchanged and readable.

The displayed result has verified forecast explanations when binding succeeds, or
**Unbound research estimate** with a specific reason when it fails. Source kind is only
classified as OPRA-derived when explicitly supplied by the bound observation; it is
otherwise unknown. Mutable passport outcome projections are excluded from Stage 2;
historical outcomes remain a later-stage capability.

## Verification

**131 tests passed** across the new binding suite, diagnostic/automation/UI suites,
and existing passport analyst/API suites. Coverage includes:

- Hash-bound matched forecast and locally rendered numeric claims.
- Same-symbol/different-ID, symbol, snapshot, calculation, model, epoch/generation,
  reference, point, timestamp and horizon mismatches; malformed/hash-tampered records.
- Missing source/identity evidence, stale versus mismatched forecasts, selected-session
  mismatch, absent calibration, and forged binding-status labels.
- Missing/rejected authorization, timeout, missing passport, malformed responses,
  redirects, response-size limits, secret exclusion and request deduplication.
- Authenticated FastAPI TestClient round-trip through the real passport route with
  a synthetic immutable record; save/read-back of an exact matched v3 review; Streamlit
  display of verified evidence paths.

Run with the canonical absolute DATABASE_URL configured:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_diagnostic_forecast_binding.py tests/test_diagnostic_findings.py tests/test_diagnostic_live_review.py tests/test_capture_review_agent.py tests/test_diagnostic_history.py tests/test_diagnostic_automation.py tests/test_advisor_workflow.py tests/test_live_advisor.py tests/test_passport_analyst.py tests/test_passport_api.py -q
```

One real `gpt-5.6-luna` review completed at `2026-09-25T18:47:05.893920+00:00`
(13:47:05 Chicago), with no operational endpoint-read errors. The packet was created
at `18:46:53.049027+00:00`, and the last runtime poll completed at
`18:46:52.800579+00:00`.

- Exact review: `data/diagnostic_reviews/2026-09-25/e282a6831b06343d77da3350eafc8d85dd4f8121a2d19efeec45db9c09cfa3fa.json`.
- Final SAVED receipt: `data/diagnostic_receipts/2026-09-25/8c39464c3f9deb6acbb82f6cb8ffaf883e336d0a52a16cf6423073cfad5a3dec.json`.
- All four passport retrievals reported `AUTHORIZATION_MISSING`: the diagnostic
  environment did not contain `MARKETPIN_GOVERNANCE_ACCESS_TOKEN`. The generated review
  explained this explicitly and still retained operational findings. This verifies the
  real unbound path, not a successful live passport match.
- Additional duplicate-field consistency guards were added afterward. The final source
  replayed the saved packet/selection identically; the original review was not changed.

## Activation and remaining live proof

The matched path is tested against the authenticated test API. To exercise it against
the running app, configure the same private `MARKETPIN_GOVERNANCE_ACCESS_TOKEN` in the
backend and diagnostic frontend environments. Reuse an existing configured credential
where one exists; do not post it in AI questions, reports or receipts. The API already
requires this credential and loopback access. No local-database fallback bypasses it.

Port 8501 still belonged to PID 54884 at the final ownership check; Windows returned
null executable path and command line. File watching is disabled. No service was
restarted, and the existing dashboard's loaded Stage 2 code is unverified. Any activation
must first verify the process owner, use the project's established launcher workflow,
and preserve active capture. Frontend-only code changes do not require a backend restart;
an absent backend credential needs separately controlled configuration activation.

After activation, run the existing refresh-and-review action or
`tools/verify_diagnostic_ai.py --ask-ai --session-date YYYY-MM-DD`. Acceptance requires a
real BOUND result for an available eligible record, retained ID/field checks and evidence
paths, a final SAVED receipt and read-back, plus visible v3 UI rendering. If evidence
does not match, keep the specific unbound result; do not relax the checks to get a number.

Stage 3 history retrieval and later forecasting research were not implemented here.
This change does not establish improved prediction accuracy or repair missing opening
observations, thin chains, or quote pairs.
