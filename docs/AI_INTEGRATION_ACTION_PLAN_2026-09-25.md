# AI integration implementation plan — September 25, 2026

Status: Stages 1 and 2 source implementation and verification completed September 25;
dashboard activation remains unverified. Stages 3–5 remain proposed and unimplemented.
See [Stage 2 results](AI_STAGE2_RESULTS_2026-09-25.md) for authenticated retrieval,
binding checks, test evidence, and the live credential/activation boundary.
See [Stage 1 results and Stage 2 scope](AI_STAGE1_RESULTS_AND_STAGE2_SCOPE_2026-09-25.md)
for verification artifacts, delivery limits, and the next implementation contract.
The source problems below describe the original inspection baseline.
Workspace: `C:\Cprojectsgpu_app\MarketPinPredictor`.
Inspected branch: `feature/2026-09-legacy-score-backlog`, with substantial existing
tracked and untracked work. Treat current source as the planning baseline; preserve it.

## Objective and delivery sequence

Make AI explanations faithful to application state, add bounded historical evidence,
and measure whether any later AI forecasting contribution improves held-out outcomes.
Operational diagnosis, forecast explanation, and experimental forecasting are separate
capabilities. A successful diagnosis request is not proof of forecasting skill.

Implement as five separately reviewable changes:

1. **P0: Correct status semantics, price provenance, and review history presentation.**
2. **P1: Bind forecast explanations to immutable forecasts and verified claims.**
3. **P1: Add recent-history and historical-outcome retrieval.**
4. **P2: Add bounded, read-only AI follow-up queries.**
5. **P3: Evaluate a separately registered AI shadow forecaster, only after the earlier gates pass.**

Do not combine the thin-chain/quote-pair investigation, official-close ingestion
repairs, model promotion, or runtime recovery into the wording change.

## What the code already provides

| Existing location | Inspected capability | Direction |
|---|---|---|
| `app/services/diagnostic_agent.py`: `build_packet`, `diagnose`, `render_diagnostic_agent` | Packet construction, strict response checks, enumerated citation IDs, live request action, rendering and automatic save | Keep the request/save path; replace ambiguous facts and unconstrained status classification |
| `app/services/diagnostic_history.py`: `session_context`, `summarize_attempts`, `save_review`, `load_reviews` | Session timing, retained-attempt counts, content-addressed review history | Separate collection origin from report retention; keep original archives readable and unchanged |
| `app/services/diagnostic_runtime.py`: `FIELDS`, `safe_fields`, `collect_live_packet`, `persistence_observation` | Two endpoint observations, sanitized projections, message/count deltas, local DB row timestamps | Preserve failure handling; extend projections deliberately rather than sending raw payloads |
| `app/services/diagnostic_receipts.py`: `new_receipt`, `write_receipt` | Append-only request outcomes and saved-review location | Add prompt/schema/collector identity and retrieval receipts |
| `app/services/advisor_view.py`: `render_advisor` | Deterministic diagnostics plus the AI-review entry point | Make diagnostic refresh and model-generated review visually distinct |
| `backend/diagnostic_automation.py`: `collect_packet`, `run_once`, `read_job_status` | Post-close review and separate automatic-job state | Preserve failed attempts; display automatic outcome separately from latest successful review |
| `backend/passport_analyst.py`: `CLAIM_EVIDENCE_PATHS`, `_render_claim`, `verify_analyst_answer`, `ask_passport` | Typed claim selection, path verification, locally rendered numeric facts | Reuse this pattern for diagnostic status claims; do not make this analyst less strict |
| `backend/prediction_passport.py`: `read_prediction_passport`, `list_prediction_passport_summaries`, `_latest_outcome` | Immutable forecast identities, provenance and outcome projection | Bind explanations to a forecast ID, not a floating dashboard value |
| `backend/api/routers/passports.py` | Passport list/detail, replay and ask routes; analyst rate limiting | Reuse read/list/detail contracts; exclude replay and mutations from the proposed read-only tool allowlist |
| `backend/forecast_repository.py`: `load_research_history` | Bounded query-only reads with source-event and availability cutoffs; conflict handling | Reuse cutoff/read patterns, but do not confuse current-runtime eligibility with historical descriptive validity |
| `backend/database.py`: `MarketStructureObservation`, `PredictionAccuracyObservation` | Retained source/availability timestamps, derived reference source, GEX levels, outcome data | Prefer these structured records to scraping logs or parsing AI prose |
| `app/services/shadow_research_view.py`: `load_shadow_history` | Paginated shadow-journal history with forecasts, targets, abstentions and outcomes | Extract a UI-independent query adapter; current search has no strict as-of outcome-availability filter |
| `backend/market_research_service.py`: `MarketResearchService` | Isolated research journal and forecast collection | Reuse isolation and identities; avoid making an AI request on each worker tick |
| `backend/forecast_horizons.py`: `evaluate_methods` | Frozen candidate evaluation, verified closes, chronological disjoint test windows | Reuse evaluation principles; its forecast schema/horizon contract is not a drop-in LLM journal |
| `backend/postclose_scoring.py`: `prepare_prediction_score_selection` | Freezes the last eligible pre-close selection for later scoring | Keep selection independent of outcome and never select the best forecast after seeing the close |
| `backend/research/shadow_formula.py`, `backend/research/live_shadow.py` | Separate shadow predictions, abstentions, target timestamps and scoring | Use as architecture references; do not silently change existing formula definitions or prediction IDs |

## Change 1 — P0: trustworthy wording and status classification

### Confirmed source problems

- `session_context()` unconditionally says runtime is "not a fresh poll". That is
  appropriate for a reused observation but misleading for request-time collection.
- `diagnostic_agent.py` uses the same free-text finding shape for working, failing,
  changed and unverified. Enumerating citation IDs validates references, not meaning.
- `diagnostic_runtime.FIELDS` permits required/optional symbol fields, but
  `build_packet.PIPELINE_FIELDS` drops those fields from its compact runtime evidence.
- The current allowlist omits useful binding/provenance fields such as `forecast_id`,
  `prediction_snapshot_id`, `spot_source`, and `usable_for_prediction`. Confirm their
  exact dashboard/health locations before adding them; never infer missing values.
- The UI displays the automatic job status separately from saved reviews without a
  clear reconciliation headline. An old failed job can appear to describe all reviews.

### Implementation directions

1. Add a pure `app/services/diagnostic_findings.py` module (new file) that derives typed
   facts from supplied evidence. Suggested fact codes: `COLLECTED_FOR_REQUEST`,
   `RETAINED_OBSERVATION`, `RESEARCH_AUTHORITY`, `WINDOW_NOT_DUE`, `WINDOW_IN_PROGRESS`,
   `COVERAGE_UNVERIFIED`, `REQUIRED_SYMBOL_INVALID`, `OPTIONAL_SYMBOL_EXCLUDED`,
   `REFERENCE_SOURCE_UNKNOWN`, and `COLLECTION_FAILED`.
2. Extend `session_context` with explicit collection mode, collection start/end,
   backend sample time, report generation time, and source timestamp where available.
   A poll timestamp never refreshes the underlying quote timestamp. Legacy inputs
   default to retained/unknown; only the collector can mark request-time collection.
3. Make independent axes explicit: transport health, input eligibility, forecast
   authority, coverage status, and empirical forecast performance. Do not compress
   them into one global "healthy" or "failing" label.
4. Render authoritative status wording locally. The AI may select fact IDs and explain
   observations, but it may not turn research-only into an operational failure,
   NOT_STARTED into a missed deadline, or optional exclusion into a required-symbol veto.
   The summary must follow the same constraints as the detail sections; checking only
   the section lists would leave the main overclaim path open.
5. Suggested UI groups: **Working**, **Data/application issues**, **Changes observed**,
   **Forecast limitations**, and **Not due / Not yet verified**. An empty issues group
   should say no issue established by these checks, not promise general system health.
6. Preserve explicit source kinds: observed cash, OPRA-derived reference, futures,
   verified official close, and unknown. Never rename a parity reference "cash spot".
7. Resolve symbol dependencies from the relevant model/passport when available.
   Runtime required/optional lists describe pipeline policy; they do not by themselves
   establish which variables an individual forecast uses. Unknown dependency stays unknown.
8. Show two dated statuses: **Automatic post-close job** and **Latest saved AI review**.
   Link each to its own receipt and evidence time. Never reset an old FAILED job to
   SUCCESS because a later manual review succeeded.
9. Introduce an explicit new review/claim-format version. Update strict schemas,
   validation and save/load dispatch together. Keep v1 archives unchanged and provide
   a v1 display adapter. Record prompt, schema, collector and rule versions/hashes.

### Acceptance checks

- Freeze the user's September 25 report as a regression example, labeled as user-
  supplied text. A reconstructed fixture must be explicitly synthetic; do not claim
  to have the exact submitted packet unless its saved artifact is located and verified.
- Mid-session NOT_STARTED is "not due"; missing evidence after close is unverified,
  not automatically proven missing capture. Include early-close and different-day cases.
- Research-only is a limitation even when source validation passes. Optional VIX/RUT
  invalidity does not veto SPX absent a documented dependency.
- Request-time collection is labeled accurately; stale underlying quotes remain stale.
- An invented number, incorrect evidence path, mismatched forecast ID, or contradictory
  fact classification is rejected or replaced with a deterministic insufficient-evidence result.
- Failed new request cannot relabel an older saved result as current. v1 history remains readable.

Extend `tests/test_diagnostic_live_review.py`, `tests/test_diagnostic_history.py`,
`tests/test_capture_review_agent.py`, and `tests/test_advisor_workflow.py`; add
`tests/test_diagnostic_findings.py`. Prompt snapshots alone are not acceptance tests.

## Change 2 — P1: explain the exact forecast

1. Carry forecast ID, calculation ID, prediction time, target time, model version,
   reference source and data age from the dashboard through the allowlist and packet.
2. Read the matching immutable passport. Verify symbol, forecast ID, calculation/model
   lineage and timestamps before using its numbers. Missing passport means unbound
   research estimate, not permission to manufacture a passport or substitute another forecast.
3. Reuse `passport_analyst` typed claims for state/authority, point-versus-reference,
   source freshness, intervals, model provenance and outcome-versus-baseline facts.
   Add a small orchestration adapter; do not route every operational question through
   a single-forecast analyst or call a second model unnecessarily.
4. Show forecast issue time and target time together. Display unavailable calibration
   explicitly. A heuristic confidence value must not become an asserted hit probability.
5. Keep optional generated causal explanations labeled as hypotheses and tied to
   concrete evidence; a valid price-distance calculation does not prove causation.

Tests: extend `tests/test_passport_analyst.py`, `tests/test_passport_api.py`, and the
diagnostic packet tests. Reject same-symbol/different-forecast, stale, cross-generation,
different-horizon and inconsistent-reference matches. Assert the numeric rendering
uses verified source fields, not numbers copied from free-form AI prose.

## Change 3 — P1: recent context and previous outcomes

Add `app/services/diagnostic_evidence_repository.py` (new, UI-independent) with typed,
parameterized, query-only readers. Every request must include a fixed `as_of_utc`.

### Recent history

- Summarize 5/15/30/60-minute windows of retained structure observations: derived
  reference change, forecast change when genuinely linked, GEX/level change, eligible
  and invalid counts, first/last sample time, largest gap and subscription transitions.
- Query `market_structure_observations` plus retained calculation/capture evidence for
  pair/strike coverage. The structure table does not itself prove all those metrics exist.
  Return `UNAVAILABLE` for fields absent in the retained schema; do not synthesize them.
- Apply BOTH source timestamp and captured/available timestamp cutoffs. Keep raw
  record identity and provenance. Do not compare across generations or incompatible
  price sources without explicitly separating the segments.
- `load_research_history` currently conditions intraday validity on current runtime
  identity/eligibility. Do not use a currently invalid symbol to erase its historical
  valid observations. Keep original validity, comparability and current eligibility separate.
- Return compact summaries plus evidence IDs and bounded representative records.
  Do not send thousands of ticks, the entire database, or unbounded logs to the model.

### Previous outcomes

- Join immutable issued forecasts to verified outcomes by forecast/snapshot identity,
  symbol and target. Preserve rejected, unscored and abstained denominators.
- Group by symbol, horizon, time-to-target and model version. Freeze cohort rules
  before inspecting results; avoid selecting only "similar successful days".
- Report sample/session counts, error against the no-change baseline, systematic bias,
  and interval coverage only when actual intervals exist. Calculate metrics in code.
- Require outcome availability by the analysis cutoff. The existing shadow history
  search is a display reader, not a leakage-safe historical replay API; add availability
  metadata or abstain where it cannot be established.
- Do not mix five-minute derived-reference outcomes with official cash-close outcomes.

Initial proposed budgets: 2,000 input rows per symbol/window, 200 returned detail rows,
two-second query deadline, and pagination. Measure adequacy before finalizing these
limits. Current manual input limit is 96,000 bytes and automation packet limit is
64,000 bytes: share an explicit budget policy and return truncation/missing-evidence
receipts rather than silently exceeding one path's limit.

Tests: extend `tests/test_forecast_repository_conflicts.py`,
`tests/test_shadow_research_history.py`, `tests/test_postclose_scoring.py`; add
`tests/test_diagnostic_evidence_repository.py`. Cover late ingestion, conflicting rows,
timezone boundaries, generation changes, insufficient coverage, bounded reads and
unchanged source-file contents. Historical replay must not see today's corrected data
before its actual availability time.

## Change 4 — P2: controlled AI follow-up queries

Add a narrow dispatcher in `app/services/diagnostic_tools.py` (new). Proposed tools:

- `get_recent_symbol_history(symbol, window_minutes, as_of_utc)`
- `get_forecast_evidence(forecast_id, as_of_utc)`
- `get_historical_forecast_metrics(symbol, horizon, model_version, as_of_utc)`
- `get_capture_quality(symbol, session_date, as_of_utc)`

All four call trusted adapters from Changes 2–3. The model cannot provide SQL, file
paths, arbitrary URLs, shell commands or tools that change subscriptions/restart services.
Validate symbols, dates, horizon and row limits server-side. Bind the conversation to
one analysis cutoff; current refresh begins a new observation set rather than silently
mixing later records into an earlier forecast explanation.

Start on demand with at most three tool calls and a bounded request deadline. Reuse
already-collected evidence rather than polling again for every question. Store tool
arguments, as-of time, source IDs/hashes, sanitized results, query duration and failures
with the final review. Declare missing evidence on timeout instead of guessing.

Refactor shared API response validation/receipt helpers only after compatibility tests
cover manual, automatic and passport paths; retain each path's distinct authority rules.
Do not add a background LLM polling loop in this change. Any later schedule should be
separately configured, budgeted, deduplicated and quiet on unchanged/non-actionable state.

Tests: new dispatcher/orchestration tests for SQL/path/URL injection, prompt injection
in tool data, excessive calls, timeouts, unavailable outcomes and replayable evidence
binding. Inspect safe receipts for absence of credentials and provider error bodies.

## Change 5 — P3: shadow forecasting experiment

This is a separate experiment, not an extension of the diagnostic assistant's authority.
Define the hypothesis before collecting results: does a fixed AI candidate improve
forecast error versus the same-time incumbent and no-change reference baseline?

1. Preregister one symbol/horizon cohort, issuance schedule, source requirements,
   evaluation period, failure/abstention treatment and success metrics. Use existing
   applicable governance floors; do not invent a lower floor to finish earlier.
2. Version the model, prompt, schema and candidate. Freeze a point-in-time packet and
   retain its hash, forecast value/interval, issue and target timestamps, reference
   source, baseline, request receipt and abstention reasons in a separate journal.
3. Audit the existing shadow journal before reusing it: its formula contracts and
   scoring semantics may not match LLM/EOD candidates. Prefer a separate candidate
   schema/adapter over rewriting existing rows or formula identities.
4. Score only matured, appropriately sourced outcomes. Official-close EOD scoring and
   short-horizon derived-reference scoring remain separate. Freeze selection before
   outcomes; preserve every scheduled failure, abstention and missing outcome.
5. Use chronological held-out sessions, purge overlapping target windows where
   applicable, and compare against both baselines on the same eligible cohort. Report
   absolute error, bias, direction metric definition, interval coverage when applicable,
   coverage/abstention rate, independent session count, uncertainty, latency and cost.
6. Change one information source at a time: current evidence only, then recent history,
   then eligible historical metrics. Keep separate candidate versions and an untouched
   final holdout. More context earns inclusion only through measured improvement.
7. Retain research-only status. No automatic production promotion, formula changes,
   threshold relaxation or trading authority. A human review of held-out evidence is
   a separate promotion step; diagnostic prose does not satisfy it.

Extend the existing forecast/shadow evaluators only through explicit adapters and
tests for lineage, immutable inputs and horizon compatibility. Do not assume
`evaluate_methods()` can score arbitrary AI response JSON without such an adapter.

## Verification and release checklist

For each change, record source revision/diff, relevant tests, evidence artifact and
activation status separately. Existing historical test passes are not new proof.

1. Recheck branch, dirty files and concurrent ownership before edits. Stage only named
   files if a commit is requested. Exclude generated data/receipts from source publication.
2. Run affected tests and Streamlit AppTest cases. Use the repository virtual environment
   and an absolute configured database path; do not change local env files merely for tests.
3. Maintain an offline evaluation set for wording and attribution with mandatory failure
   cases. Require all safety-critical assertions to pass; record omissions/rejections as
   well as successful explanations. Validate the final summary, not only structured fields.
4. After offline checks pass, verify one explicitly requested real review from newly
   collected runtime evidence, with actual model, completion, valid claims, saved artifact
   and hash-verified read-back. Preserve failed attempts. Do not retry uncertain requests blindly.
5. Inspect fresh process ownership before any activation. Verify the visible new UI and
   loaded behavior separately. A dashboard-only release must preserve backend identity.
   Backend changes require their own scoped activation and evidence; prior September 21
   ownership limitations are historical, not assumed-current blockers.
6. For history retrieval, show which records were available at the fixed analysis cutoff.
   For forecasting, collect later outcomes; neither tests nor a successful AI request
   establish an improvement in predictive accuracy.

Recommended first implementation ticket: **Change 1 only**, including the source-field
projection corrections, typed status rules, summary constraints, backward-compatible
history rendering and regression fixtures. Its acceptance criterion is that the user's
September 25 example can no longer classify research-only status or a not-yet-started
closing window as operational failures, or imply an unsupported VIX/RUT veto of SPX.
