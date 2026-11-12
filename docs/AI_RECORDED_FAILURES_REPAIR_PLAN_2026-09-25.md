# Recorded AI/application failures: repair plan and first implementation

## Evidence and scope

Primary artifact: `data/diagnostic_reviews/2026-09-25/173c2077d9ca2b46590f78194e5662997a5e18de1e1a546cbea51bd0431a4bb7.json`.
Its content hash was verified. Runtime observation: 2026-09-25 20:18:08.940800 UTC,
after that session's 20:00 UTC cash close. Also reviewed the user's full packet and
saved-history attachment; historical September 17 excerpts are not new September 25
incidents. Preserve all reviews, capture records and failed automation outcomes.

The model completed successfully. The embedded VALIDATED receipt precedes saving.
Final SAVED receipt for attempt `1e545462189049448f5f35708e8178f4`:
`data/diagnostic_receipts/2026-09-25/3be968f318b695ae532833e530f0f392d717401c6b57d6c04f97aabf8e4f0b37.json`.
The null embedded review_path is not a save failure.

## Work register: every reported condition

| Condition | Diagnosis / evidence | Correction and acceptance |
|---|---|---|
| Automatic job INPUT_LIMIT, zero API attempts | Retained automated packet is 64,618 bytes; old automation cap was 64,000, while manual review allowed a larger bounded input | Share exact input serialization and 192 KiB input cap, including fact catalog and question. Oversize fails before API use; old terminal job is not silently retried. Initial code implemented. |
| Passport credential missing | Saved binding receipts say AUTHORIZATION_MISSING; prior exact endpoint probe returned 503 | Configure one private shared credential through verified startup paths; prove loaded authorization and authenticated exact-ID retrieval. Never log secret values. Pending operational activation. |
| Valid lifecycle passports rejected | Earlier saved SPX/NDX IDs have lifecycle_revision origins with null snapshot IDs | Dispatch by supported origin contract. Null is allowed only for lifecycle revisions with valid origin key and immutable integrity; snapshot origins require matching positive IDs. Initial code implemented and regression tested. |
| Additional missing calculation binding | Replaying those exact earlier records after origin repair now stops at calculation_id; both dashboard and passport calculation IDs are null | Trace lifecycle source-payload hash/calculation lineage; publish independently verifiable revision evidence or calculation identity for new records. Do not blindly waive missing fields or modify old passports. Pending investigation. |
| SPX invalid | Source reason PSEUDO_PARITY_TOO_FEW_PAIRED_QUOTES: 4 pairs < 5. Session: 362 valid / 379 retained attempts; final 15 minutes: 6 / 17 valid | Trace pair attrition by exact calculation, selected primary expiry, same-generation call/put freshness, crossed/invalid quote filters, strike coverage and subscription definitions. Preserve thresholds. Acceptance: explain rejected pairs and show valid advancing results in a later eligible window. |
| NDX invalid | PRIMARY_PAIR_COVERAGE_LOW: approximately 7.35% < 10%. Session: 365 / 372 valid; final 15 minutes: 11 / 15 | Reconcile expected versus paired primary contracts and quote ages at the same calculation; distinguish selection/subscription defects from genuine insufficient inputs. Preserve denominator semantics and threshold. |
| VIX invalid / optional exclusion | One primary-expiration strike and extreme concentration in runtime; 0 / 368 valid retained attempts, 0 / 13 in final 15 minutes | Audit definition mapping, expiry selection, subscription coverage and retained quotes. Determine whether sparse expiry is expected before changing policy. Keep excluded from optional context while invalid; no implied SPX/NDX veto. |
| SPX/NDX/VIX stale after close | Post-close observation and old underlying quotes | Keep explicit source age and session phase. Do not mark inactivity alone as an intraday outage or repoll to claim fresh quotes. Genuine validation reasons remain visible. Initial wording adds the missing reason codes and temporal qualification. |
| Outputs not decision-grade | Authority/calibration limitation, not a software exception | Keep research/abstention labels. Repair inputs first; evaluate any promotion separately against existing out-of-sample gates. No status override or confidence inflation. |
| OPRA-derived reference, not cash spot | Correct provenance description in the latest packet | Preserve the label. Earlier packets lack the source field; trace upstream omissions and carry explicit provenance into new observations, never infer provider = cash quote. |
| Opening bucket unverified | Packet hardcodes unavailable exact opening proof; attempt counts cannot establish it | Add bounded exact-first-bucket evidence from existing opening receipts/ORB records, with symbol/session/epoch/source/code identity. Distinguish absent packet fields from genuinely missing eligible opening records. Never reconstruct observations. |
| Closing coverage unverified | Window elapsed; valid/total counts exist but are not continuous coverage proof | Include counts now; later reconcile expected cadence/boundaries/gaps and validity against retained timestamps. Initial code exposes 6/17, 11/15 and 0/13 rather than only saying unverified. |
| Official close and forecast accuracy unverified | No saved_closing_prices evidence in this packet; not proof that every other store lacks a close | Inspect verified-close ledger, provider provenance, ingestion and frozen pre-close score selection. Read existing verified close if present; otherwise report unavailable. Never substitute parity, user-reported prices or a post-close estimate as official truth. |
| Generic answer instead of requested forecast | Fact selector plus generic operational summary | Add an answer-first forecast presentation using verified fields, issue/target time and calibration limits, or lead with the exact binding blocker. Separate market health from AI integration readiness. Pending. |
| Misleading old-source warning | Warning belongs to previously selected change proposals, not AI review validity | Move inside recorded-proposals panel and explicitly distinguish saved AI reviews. Initial code implemented; approval invalidation retained. |
| Old reviews appear among current results | Retained history, including legacy wording | Preserve immutable originals and legacy labels. Consider newest-first collapsed history and show observation age/session on each. Do not rewrite older conclusions as current findings. |
| Embedded VALIDATED/null path | Validation receipt is captured before content-addressed review persistence | Resolve a separate final receipt, verify both receipt hash and linked review bytes, display SAVED/path first, and explain embedded pre-save receipt. Initial code implemented. |

## Execution order and code directions

1. **First batch: source fixes and regression proof.**
   `diagnostic_forecast_binding.py` origin dispatch; `diagnostic_findings.py` bounded
   reason codes and final-window counts; `diagnostic_receipts.py` verified final receipt
   lookup; `diagnostic_agent.py` receipt UI and shared input serialization;
   `backend/diagnostic_automation.py` shared input budget; `advisor_view.py` warning
   placement. Preserve dirty work and every retained original. Implemented in this turn.
2. **Complete authenticated forecast binding.** Resolve the missing calculation lineage
   before claiming a successful match. Inspect `backend/api/lifecycle.py`,
   `backend/prediction_passport.py`, `backend/workstation.py` and source hash contracts.
   Test real lifecycle-origin shapes, exact forecast identity, conflicting duplicates,
   model/source mismatches and stale current-use evidence. Then provision/load the
   credential with verified process ownership and capture-preserving activation.
3. **Run the separate input-data investigation.** Use query-only retained rows from
   the final 15 minutes and a healthy comparison interval. Group reason codes, IDs,
   expiry selection, pair numerator/denominator, quote ages and subscription revisions.
   Inspect `backend/databento_streamer.py`, market-structure validation and subscription
   selection. Produce per-symbol attrition evidence before choosing a code correction.
   Do not lower thresholds, enable extra provider subscriptions or promote a model as
   an incidental fix. Genuine irrecoverable missing observations stay missing.
4. **Collect opening/closing/outcome evidence.** Extend `diagnostic_runtime.py` and
   `diagnostic_history.py` with bounded read-only receipts, not raw unbounded logs.
   Reuse existing opening evidence and verified-close/post-close scoring contracts.
   Represent exact status, provenance and missing evidence separately for each symbol.
5. **Answer-first UI and live acceptance.** Lead with the requested symbols and verified
   estimates when available, otherwise explicit blockers. Test rendered UI, then one
   authenticated real review plus automatic save/read-back. Verify the loaded dashboard
   before claiming delivery. For a past session, label retained pre-close estimates
   explicitly and never use outcomes to manufacture a better historical prediction.

## Verification and remaining boundaries

First-batch tests cover lifecycle versus snapshot origins, unsupported/inconsistent
origins, validation reason preservation, post-close staleness classification, closing
attempt counts, final receipt/hash resolution, shared input budget and historical-job
non-retry behavior, plus the existing diagnostic and passport suites.

Result: **138 tests passed** in the focused diagnostic, automation, advisor, passport
analyst and passport API suites. This is source/test proof, not runtime activation.

Read-only replay of the earlier exact SPX/NDX records confirms the snapshot rejection is
removed but exposes missing calculation identity. This is not a live matched-passport
success. No new paid AI request is necessary to prove these deterministic fixes.

No credentials were changed, no services restarted, no failed job reset, and no market
artifact rewritten in this first batch. Authorization/configuration activation, source
lineage completion, data-path repairs and next-session proof remain open work.

## Subsequent implementation

See [Monday readiness and corrective actions](MONDAY_DIAGNOSTIC_READINESS_2026-09-28.md)
for credential provisioning, independent source-hash binding, answer-first rendering,
the repeatable readiness command, 247 passing tests, retained quote-pair investigation,
and the remaining live activation/market-input acceptance requirements.
