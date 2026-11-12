# Opening capture reliability — implementation plan

Prepared September 29, 2026. **Status: proposed; no implementation or runtime changes authorized by this planning document itself.**

## Objective and limits

Make the existing five-second opening sampler ready to persist an eligible observation in the exact opening bucket, then sustain capture through the 5-, 15-, 30-, and 60-minute ranges. The next live acceptance opportunity is September 30, 2026, beginning at 8:30 AM America/Chicago. Production scheduling must derive the opening time from the exchange calendar and timezone, including daylight-saving changes.

Today we can prove source semantics, rejection behavior, storage compatibility, timing under simulated load, and release readiness. Tomorrow we can observe whether sufficient real quotes arrive and all gates pass before the opening deadline. Neither offline tests nor a scheduled task guarantees full capture.

Preserve models, formulas, quote freshness limits, minimum pair counts, the 50-sample processing-clock minimum, opening bucket boundaries, subscription scope, and immutable history. Correcting a schema interpretation requires an explicit versioned evidence contract; it must not silently change the meaning of historical validation.

## Evidence baseline

These are retained observations, not a fresh claim about the runtime at document-reading time.

| Classification | Finding | Consequence |
|---|---|---|
| OBSERVED | September 29 SPX first eligible sample was 15 seconds late; NDX 60 seconds; RUT 25 seconds; VIX 23 minutes. | The exact first bucket was missed. Later coverage cannot repair it. |
| OBSERVED | Opening attempts initially had fewer than 50 processing-clock samples, followed by handoff and quote-pair failures. | A clock-warming rejection does not by itself demonstrate Windows clock drift. |
| OBSERVED | The app had already started before the open. | Moving startup earlier alone is insufficient. |
| OBSERVED | ORB read filtering, raw-tape launch admission, and the direct recorder's database path were repaired and activated earlier today. | Preserve these changes; avoid repeating or undoing them. |
| OBSERVED | Existing ingestion and opening validation require a trade event timestamp; the legacy reference table and immutable triggers require non-null event bounds. | A streamer-only change would still fail storage or downstream validation. |
| OBSERVED | Databento documents CBBO `ts_recv` as the interval end, and `ts_event` as the last trade time, which can be undefined if no trade occurred in the session. | Quote observation timing and last-trade timing need distinct treatment. |
| INFERRED | Requiring a current-session last trade can reject a current CBBO interval observation and reduce opening pair availability. | Reproduce the defect with schema-faithful fixtures before activation; quantify the affected candidates where retained evidence permits. |
| NOT YET VERIFIED | That correcting this mismatch supplies enough real pairs and processing-clock samples within the first five seconds. | This remains a live acceptance requirement, not a promised outcome. |

Baseline receipts: `output/session_review_20260929T160938Z/REPORT.md` and `output/capture_repair_20260929T1614Z/REPORT.md`, including the latter's sealed manifest. Do not alter those receipts.

Provider contract: [Databento schemas and data formats](https://databento.com/docs/schemas-and-data-formats). An interval snapshot does **not** prove that the bid and ask changed at that instant. TCBBO trade replay is not a substitute for missing CBBO interval observations.

## Work package 1 — lock the contract and reproduce the failure

**Deliverable:** reviewed timestamp contract, failing regression fixtures, and a hash-addressed baseline before production edits.

1. Record canonical checkout, branch, dirty-file inventory, loaded source fingerprints, installed task configuration, and relevant runtime generation. Preserve unrelated changes. Implementation should use a suitable isolated checkout with the necessary existing changes accounted for; a clean branch alone may omit today's deployed fixes.
2. Define supported schema/record combinations. For CBBO, retain provider interval time, optional last-trade time, local receive/processing time, mapping identity, subscription identity, and validation-contract version as separate fields.
3. Define sentinel handling explicitly: an absent last trade is nullable trade evidence, not an invented event timestamp. When present, validate the last-trade timestamp against the documented record semantics, including provider-session boundaries. An older trade is acceptable only where that contract permits it. Do not copy `ts_recv` into `ts_event`.
4. Keep current-session and freshness checks on the appropriate interval observation. Keep current mapping, epoch/generation, quote-quality, handoff, replay status, processing-clock, and pair-count gates.
5. Build fixtures for fresh intervals with a same-session trade, an older trade, and no trade. Preserve rejection fixtures for genuinely old quote intervals, wrong mappings, stale generations, malformed timestamps, crossed quotes, and insufficient pairs.
6. Prefer SDK-created or retained provider records to establish field interpretation. Label synthetic workloads explicitly. Use any retained opening CBBO evidence read-only; do not claim a historical TCBBO replay reproduces every opening CBBO observation.

**Pass condition:** the current defect is reproducible and the proposed semantics distinguish a stale quote observation from an old or absent last trade. Unsupported schemas fail closed. Existing non-CBBO semantics remain unchanged unless separately verified.

## Work package 2 — implement one complete ingestion-to-storage change

**Deliverable:** a versioned quote-timing path that survives calculation, persistence, read-back, and audit inspection.

Primary files: `backend/databento_streamer.py`, `backend/database.py`, `backend/market_structure.py`. A small dedicated timestamp-normalization module is appropriate if it avoids duplicating schema rules.

1. Apply the contract consistently to ingestion, candidate selection, pair provenance, and opening reference construction. Compute interval bounds separately from optional trade-event bounds. Missing trade evidence must remain visibly missing.
2. Add new versioned reference storage and matching immutable decision storage. Proposed design: an additive table pair for the new timestamp contract, with nullable trade bounds and required validated interval bounds. Finalize names during contract review; distinguish this timing-contract version from the existing ORB API's `v2` identity contract.
3. Preserve the legacy table, NOT NULL constraints, immutable triggers, receipts, and historical interpretation. Do not rebuild it, falsify event fields, or disable constraints to admit new observations. The new decision table needs its own correct reference relationship; the legacy decision foreign key cannot reference a different sample table.
4. Add version-aware readers before enabling new writes. Prevent double counting and mixing legacy/new validation contracts into one supposedly continuous range. Unknown contracts remain diagnostic-only. Keep raw provider fields and validation version in sample hashes and provenance.
5. Update `backend/opening_incident_import.py`, `tools/preflight_opening_acceptance.py`, `tools/inspect_opening_capture.py`, and affected schema/fingerprint checks. Validate old and new databases, including restart after migration. Ensure accepted evidence has a persisted final-decision record and verified read-back.
6. Confirm every caller of reference capture and ORB aggregation supports the new contract. Preserve the bounded read behavior in `backend/api/routers/orb.py`.

**Pass condition:** fresh, otherwise valid CBBO observations with no last trade can be stored and read under the new contract; actual stale or invalid quotes are still rejected; all old records and hashes remain unchanged. Do not ship just the ingestion half of this package.

## Work package 3 — decouple clock evidence from option eligibility

**Deliverable:** processing-clock warmup that can use legitimate provider timing before an option passes pricing/mapping eligibility, with accurate failure reasons.

The existing receive-lag collector sits after mapping and price checks. Move schema-appropriate transport timing collection ahead of those filters, without allowing rejected prices into calculations.

- Count legitimate timestamped live records from the active client. Define duplicate handling and sampling explicitly; do not seed the live window with synthetic values or historical replay merely to meet the sample count.
- Preserve the 50-sample minimum, existing lag tolerance, negative-lag policy, and bounded window. Materially negative observations must remain evidence of clock trouble; filtering them out would hide the failure.
- Specify behavior for reconnects, subscription generations, replay completion, malformed timestamps, and clock jumps. Do not carry unrelated old clock evidence across a reset without a separately reviewed validity contract.
- Expose distinct diagnostics for `warming / insufficient samples`, measured clock inconsistency, and Windows time-service health. Retain compatibility for existing consumers where reason-code changes would otherwise break them.
- Verify the installed Windows time startup/recovery tasks, service state, last results, and recovery after boot/resume or temporary network loss. Reuse the existing bounded repair path. Avoid competing repair loops or repeated app restarts as a response to clock warmup.
- Verify pre-open host synchronization independently. Host synchronization alone does not establish fresh option observations or sufficient pairs.

**Decision gate:** if real or representative arrival evidence cannot establish the unchanged 50-sample requirement within the first bucket, this package does not solve opening readiness. Document that limitation and evaluate an independently validated pre-open clock-attestation design as a separate policy change. Do not lower the minimum or relabel unknown as synchronized.

## Work package 4 — protect the opening deadline and prove coverage

**Deliverable:** deterministic opening capture under realistic application load and an automatic, per-symbol explanation when it fails.

An independent sampler and retries within the intended bucket already exist. Improve their measured bottlenecks rather than adding another scheduler or a second competing sampler.

1. Measure ingestion, clock readiness, mapping/handoff readiness, paired-quote availability, snapshot extraction, calculation, final decision, and persistence separately. Store elapsed times and rejection causes in bounded diagnostics.
2. Keep heavy calculations and database/UI work out of the provider receive loop. Preserve short snapshot locks and the exact-deadline checks. Test concurrent dashboard reads, normal prediction work, and permitted recorder activity at the actual launch times.
3. Verify first-bucket capture at 8:30:00–8:30:05 AM Central, with the existing decision/commit rules. A receipt can be inspected later; its authoritative sample and decision times cannot be moved back into that interval.
4. Extend the existing `backend/monitor_opening_acceptance.py` and `tools/inspect_opening_capture.py` flow with an explicit first-bucket milestone, followed by 5-, 15-, 30-, and 60-minute results. Reuse existing receipt and notification mechanisms, with one actionable failure notification per incident and deduplicated recovery notifications.
5. Report each configured symbol separately. SPX/NDX core readiness must not be disguised by aggregate quote counts; RUT/VIX failures remain explicit. SPY is not silently added to the parity ORB subscription scope.
6. Retain bucket identity, provider interval bounds, processing/decision times, pair counts, clock sample count/status, source version, process identity, generation, and persistence read-back in immutable receipts.
7. Display coverage numerically alongside policy status. At five-second cadence, the four windows contain 60, 180, 360, and 720 expected buckets. Distinguish **100% observed bucket coverage** from passing the existing completeness threshold; do not change that threshold.

**Pass condition:** end-to-end rehearsal creates and reads exact first-bucket evidence under load; injected late decisions, failed commits, generation changes, and unavailable pairs remain partial/unavailable. Every live miss produces a specific retained cause rather than a generic clock message.

## Verification matrix

| Case | Required result |
|---|---|
| Fresh CBBO interval, old or absent last trade | Eligible only under the new contract and only if all other gates pass; optional trade time remains accurately represented. |
| Prior-session interval, stale quote, malformed interval, invalid time ordering | Rejected with explicit reason; no first-bucket or current-prediction promotion. |
| Wrong mapping, old generation, replay-only evidence, unknown contract | Rejected for current opening authority. |
| Valid quotes but fewer than required pairs | Remains ineligible; preserve the shortage count. |
| 49 versus 50 valid timing samples | Unknown below the unchanged minimum; evaluate the existing lag policy at/above it. |
| Negative receive lag or local clock jump | Clock risk remains visible; no filtering designed to make the clock pass. |
| Input becomes ready near the five-second deadline; persistence is slow | A late decision/commit never masquerades as the first opening bucket. |
| Backend restart or subscription transition during a range | Separate identities/segments; no false continuity. |
| Old-only, new-only, and mixed-version databases | Correct version-aware results; legacy hashes preserved; no duplicate samples or silent schema fallback. |
| UI/database load, queue pressure, and interrupted writes | Bounded reads, explicit capture failures, and immutable receipt consistency. |
| Calendar holidays and daylight-saving transition | Correct session date and local opening boundaries. |

Use absolute temporary SQLite paths for offline tests. Target relevant suites: `test_databento_streamer`, `test_opening_quote_selection`, `test_opening_transition_stress`, `test_orb_e2e_rehearsal`, `test_market_structure_orb`, `test_orb_read_admission`, `test_inspect_opening_capture`, `test_monitor_opening_acceptance`, `test_opening_acceptance_preflight`, and `test_opening_incident_import`. Run applicable launcher/supervisor Pester suites if their behavior changes. Add focused contract/migration cases rather than broad tests that merely repeat implementation details.

## Implementation ownership and order

Two agents could help after agreeing on the contract. This is a proposed split, not a dispatch request.

| Owner | Responsibility | File boundary |
|---|---|---|
| Agent A — capture and persistence | Timestamp contract, ingestion, processing-clock collection, additive schema, version-aware aggregation, focused unit tests | Own streamer/database/market_structure and any new timing module. |
| Agent B — verification and operations | Independent fixtures, opening rehearsal, milestone inspector/monitor, preflight, clear UI failure reasons, release checklist | Own tools/monitor/UI changes and separate new test files; consume A's agreed contract. |
| Integrator | Reconcile shared fingerprint registry and test fixtures, review immutable-storage guarantees, run combined checks, activate and collect receipts | One owner for deployment and runtime actions. |

Sequence: baseline and contract → reproducible fixtures → additive storage/readers → ingestion and clock changes → integrated deadline tests → milestone/UI checks → release review. Agent B can prepare workloads and acceptance assertions while A implements, but cannot finalize schema checks before the contract is agreed. A single agent can execute the same sequence serially.

## Safe activation today

1. Finish one complete, tested vertical change. If timestamp normalization, storage, or readers are unfinished, do not deploy a partial patch to meet a calendar promise.
2. Keep today's running capture stable during implementation. Schedule activation around the actual recorder, finalization, and settlement task state; the market close alone is not proof that those jobs are finished. Avoid overlapping settlement or replay processes.
3. Preserve a consistent database backup using the SQLite backup facility or an approved quiescent procedure; do not copy only the main file of an active WAL database. Record source hashes, schema version, test receipts, and rollback source.
4. Apply the additive schema and verify it. Activate through the existing guarded launcher/restart process with one runtime owner. Confirm loaded source fingerprints and database path, then verify API reads and persistence. Off-hours lack of new quotes must not be labeled a failed deployment or a successful live test.
5. If rollback is needed before the next open, disable the new write path and restore the prior code/configuration through the guarded procedure. Retain new tables and receipts. Unknown new evidence must remain audit-visible and non-authoritative to an older reader. Do not automatically restart repeatedly during the opening capture window.

Do not change models, thresholds, subscriptions, past scores, or missed opening records as part of activation. Official-close capture and diagnostic scoring remain separate workflows; validate that this release does not interfere with them.

## Next-open acceptance — September 30

Use the exchange calendar for these local times and reuse the installed supervisor/acceptance machinery. The checklist in `docs/PREMARKET_EVIDENCE_CHECKLIST.md` is a manual checklist, not proof that these automatic milestones already exist.

- **Before 8:15 AM CDT:** verify startup actually ran, canonical process ownership, loaded source, current-day universe, host time state, database schema, available disk, and provider connection. Record genuine missing prerequisites.
- **At 8:25 AM:** repeat readiness checks and verify the opening sampler is armed. Distinguish provider clock sample warmup from host synchronization. Avoid discretionary restarts or subscription changes near the open.
- **Just after 8:30:05 AM:** inspect the committed first-bucket sample and final decision for each symbol. If absent, preserve the cause and notify through the existing configured channel. Continue recording; do not fill the opening bucket retrospectively.
- **After 8:35, 8:45, 9:00, and 9:30 AM:** verify window coverage, gaps, first/end buckets, identities, clock evidence, and persisted range results. Preserve per-symbol receipts with real retrieval times and hashes.
- **After the session:** inspect loss/backpressure and capture finalization separately, compare observed failures against the baseline, and update the failure register only for the acceptance criteria actually met.

## Definition of done

**Offline complete:** schema-correct behavior, compatible immutable storage, clock semantics, bounded timing, migration/rollback, and fail-closed cases pass together. Record this as tested implementation, not live capture success.

**Live opening verified:** each symbol claimed successful has an eligible exact first-bucket sample and final decision, stable authoritative identity, and the required persisted window coverage. Report full coverage only when all expected buckets are present. Report other outcomes per symbol as partial or unavailable.

**Reliability improving:** retain results over subsequent sessions, including reboot/reconnect recovery evidence. One successful morning verifies that morning; it cannot establish that clock or provider problems will never recur.

If adequate quote pairs genuinely do not arrive in time after the timestamp defect is corrected, a parity-derived ORB may be unavailable by design. Evaluate a separately sourced underlying-price ORB as a distinct product/data decision, with explicit provenance and entitlement review. Do not substitute it silently for OPRA parity history, expand subscriptions, or relax the opening rules under this plan.
