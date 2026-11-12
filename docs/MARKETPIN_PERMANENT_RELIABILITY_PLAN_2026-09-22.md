# MarketPin permanent reliability and history recovery plan

Prepared September 22, 2026, after the regular session. This is an implementation plan, not a claim that its proposed changes are deployed. The bounded history qualification completed at 15:23:10 America/Chicago.

## Decision

Use two coordinated agents: **A owns live-session reliability and capture; B owns verified history, forecast data, and the final dashboard integration.** Keep a single live-process owner (A) and a single historical-import owner (B). Start by completing reliable input acquisition and retention. Keep model formulas, thresholds, subscription budgets, and promotion gates unchanged during these reliability repairs.

The durable design separates instrument discovery, clock/feed readiness, raw evidence capture, eligible calculations/ORBs, official daily-close acquisition, and model evaluation. Each has its own state, receipts, retry policy, and progress checks. An ineligible prediction must remain an abstention, while independent, safely bounded evidence acquisition can continue.

**Backfill is feasible now for the daily-history requirement.** Public first-party downloads for SPX, NDX, RUT, VIX, and SPY each passed the existing approved-source, artifact-hash, exact symbol/date/close, timestamp, and continuity checks for 70 consecutive exchange sessions, June 11 through September 21. This produced 70 complete five-symbol bundles (350 closes) in memory, without opening or importing a database. The current pure history evaluator reports 69/65/60 qualified returns for horizons 1/5/10 respectively, for every symbol. The live database still needs a controlled import and read-back verification.

## Evidence and boundaries

| Finding | Evidence class and consequence |
|---|---|
| Unsynchronized Windows Time prevented both 07:00/07:15 metadata preparation attempts. After clock recovery, the 08:25 discovery protection boundary forced cache-only startup using September 21 fallback provenance. | **OBSERVED.** The coupling converted one failure into a second missing input. Why the configured NTP peers were unreachable is **NOT YET VERIFIED**. |
| Metadata-only preparation has been decoupled from the clock gate in a narrow launcher repair. | **OBSERVED in source/tests:** 27 focused tests and 163 supervisor checks under each of PowerShell 7 and Windows PowerShell 5.1 passed. Live preparation/adoption and the next opening remain **NOT YET VERIFIED**. Normal live clock gates remain intact. |
| Exact first opening bucket, 08:30:00–08:30:05 CT, had rejected attempts for SPX/NDX/RUT/VIX; no accepted ORB samples were found for the session. | **OBSERVED.** First attempts failed clock/handoff checks before pair evaluation; later failures were predominantly fallback provenance. Opening audit coverage was 719/720 buckets, including one silent gap. SPY was not configured for live ORB. |
| Messages and rejected audit records advanced, but eligible calculation/prediction/ORB persistence did not advance during the bounded check. | **OBSERVED.** Transport activity and HTTP 200 are insufficient readiness evidence. |
| The canonical daily-close ledger has only six distinct sessions through September 2, including a September 1 gap. | **OBSERVED.** This is an ingestion gap. A retained September 21 SPX download ends September 18 and reproduces the stale-source failure. That explains a concrete settlement failure path; the original terminal exception and cause of every missing day are **NOT YET VERIFIED**. |
| Research APIs and worker progress were observed, but requested forecasts abstained. | **OBSERVED.** Missing valid current-session inputs and positive-horizon history requirements remain distinct blockers. Horizon zero does not require the 60-return daily-history gate. |
| Current public-source downloads contain all 70 required sessions and pass existing offline qualification. | **OBSERVED**, including source-byte hash verification. Runtime adoption, effective correction handling against existing rows, and longer-horizon forecast availability after import are **NOT YET VERIFIED**. |
| Backfilling closes can satisfy history eligibility without waiting 60 new trading days. | **INFERRED from the observed qualification and current gate implementation**, conditional on controlled import, source-vault resolution, and read-back. It does not establish model skill or current-price eligibility. |

Retained evidence:

- [Operating verification](../output/operating_verification_20260922_1303/REPORT.md)
- [Universe/clock repair and close-gap investigation](../output/universe_clock_fix_20260922/REPORT.md)
- [Additional prediction-blocker review](../output/prediction_blocker_review_20260922T200331Z/REVIEW.md)
- [New offline backfill qualification](../output/permanent_reliability_plan_20260922/offline-backfill-qualification.json)

## Agent ownership and coordination

| | Agent A: session reliability and capture | Agent B: verified history and forecast data |
|---|---|---|
| Primary outcome | Correct current-session inputs, dependable recording, exact opening evidence, and truthful live status. | Current, provenance-verified close history; reliable daily updates; explicit forecast eligibility. |
| Main code areas | `start_market_day.ps1`, `ensure_market_app.ps1`, launcher/supervisor modules, `backend/databento_streamer.py`, ORB/capture paths, health API and live-status view module. | `tools/fetch_verified_close_*`, `tools/ingest_verified_closes.py`, post-close worker/scripts, close artifact registry/evidence, `backend/forecast_repository.py`, research history inventory/API/view. |
| Runtime ownership | Sole operator for any authorized service activation/restart, after fresh ownership and loaded-code checks. | No live restart. Sole owner of the coordinated append-only import and reconciliation job. |
| Shared surfaces | Supplies versioned per-symbol status contract and fixtures to B. | Sole editor/integrator for `app.py`; renders A's status contract. Owns shared database/schema integration if needed. |
| Restrictions | No threshold relaxation, new subscriptions, historical ledger rewriting, or model changes. | No synthetic closes, backdated acquisition, scoring side effects during history import, or model promotion. |

Use isolated worktrees from the same agreed baseline, for example `codex/session-reliability` and `codex/verified-history`. The canonical checkout is currently dirty at `0659fce033323d106d4d78caebe123d54bbb08e2`, branch `feature/2026-09-legacy-score-backlog`; HEAD alone does not include all today's fixes. Inventory and preserve the current changes, agree on the exact baseline/patch manifest, then construct worktrees. Do not reset, clean, blindly copy the whole working directory, or let both agents edit a shared file. Do not start a second provider-connected backend. Runtime activation must load the integrated, tested result through the established full-stack launcher.

Before coding, agree on a small shared contract: symbol, session, source identity/date and availability, active epoch/generation, raw event freshness, matched-pair freshness, accepted-calculation freshness, last persisted eligible event, ORB bucket result, verified-close-through date, and reason codes. Preserve existing consumers through explicit versioning or compatible additions.

## Phase 1: recover the two missing foundations in parallel

### A1. Pre-open preparation, clock diagnosis, and admission

1. Carry forward the tested metadata-only repair and verify the actual scheduled command uses it. Preparation must retain requested families and the existing budget/date/deadline checks even if the clock is unsafe. It must never report live-ready or launch components from that path.
2. Investigate Windows Time reachability and receive/process timestamp consistency. Compare host wall time, provider event time, receive time, and monotonic elapsed time; distinguish peer reachability, provider delay, clock step, and calculation queue delay. Do not treat every negative lag as the same cause. Choose/configure any clock remedy only from measured results.
3. Add durable, bounded pre-open checks with proposed checkpoints at 07:00, 07:15 and 08:15 CT, before the existing 08:25 protection boundary. Report a missing/admission-failed cache or unsafe clock while recovery is still possible. Reuse existing scheduled jobs where possible; avoid duplicate supervisors.
4. Admit only a universe whose trading date, source, content hash, instrument definitions, expiry/settlement identity, and coverage meet the existing contract. Treat prior-day OI publication time separately: a legitimate prior-day OI as-of time does not authorize a prior-day fallback instrument universe.
5. Rehearse the handoff and required clock-sample accumulation before the open. Do not manufacture clock samples or reuse a warm-up across an invalid generation change. Record a specific pre-open failure if actual provider activity cannot satisfy the contract.

Acceptance: clock-unready metadata tests stay green; metadata-only mode cannot start/restart anything; stale caches remain ineligible; current-date discovery is demonstrably adopted by the running generation; pre-open failures have timely receipts. The next actual opening is required for live proof.

### B1. Source-qualified backfill with no scoring side effects

1. Use the already retained 70-session, five-symbol downloads as the first candidate batch. Fetch at most once per requested source/range, then parse locally. Target a rolling 120–252 sessions after the first batch is proven, with calendar and corporate-action review; the current loader cap is 300 observations.
2. Audit existing ledger rows against their retained artifacts. A `source_verified` flag plus a hash string is not sufficient if the bytes are absent. Restore an old artifact only if the exact old hash matches. A new download is new evidence with its own hash and real retrieval time; never relabel it as the old artifact.
3. Implement an explicit **history-only import mode**. The existing `ingest()` invokes governed-outcome reconciliation even without explicit diagnostic prediction selections, so omission of `--score-prediction` alone is not a sufficient no-scoring guarantee. Separate acquisition, ledger commit, and approved scoring. Keep frozen prediction-selection and complete-bundle requirements for scoring.
4. Validate in a scratch database first: approved source, exact cash index/ETF identity, official unadjusted close, units, correct exchange session, one unambiguous value, full hash-backed source bytes, real acquisition/availability time, and gap/conflict checks. Review duplicates/revisions against the existing six sessions. Preserve originals; use explicit append-only correction/reverification lineage and an unambiguous effective view.
5. Produce a create-only import manifest with intended inserts, existing matches, revisions, conflicts, artifact hashes, and expected before/after counts. Retain one content-addressed source file plus explicit per-session registry receipts where supported, rather than relying on ambiguous aliases for multi-day files.
6. Once implementation/import is authorized, coordinate a single import owner with scheduled settlement jobs, take a consistent SQLite backup, verify the canonical absolute database binding, commit idempotently, and read back via query-only access and the research API. No broad repair command or automatic scoring. Keep the source vault and correction receipts backed up together.

Acceptance: repeat import is a no-op; no original row, tape, or receipt is overwritten; no model outcome changes; every effective qualifying close resolves to verified bytes; all five history inventories reach at least 60 qualified returns at horizons 1/5/10 through the required previous session. Current-session input gates remain independently enforced.

## Phase 2: prevent recurrent capture and history gaps

### A2. Recording independent of prediction eligibility

The current closing-tape startup can wait for valid primary calculations and exact all-four 5/60-minute ORBs until its start window expires. Replace that dependency for raw diagnostic acquisition with a separately resource-guarded path. Prefer a bounded tee of the existing authorized stream if its schema supports the required evidence; do not add a broad second OPRA subscription as a shortcut. Preserve ownership, capacity, and latency protection.

Retain source/event/receive times, symbol definitions, epoch/generation, sequence/gap evidence, and rejection reasons even when calculations abstain. Use bounded queues and explicit backpressure/drop receipts. An unsafe clock, missing pairs, or fallback universe must still prevent raw records from becoming eligible observations. Maintain separate raw, rejected, and eligible persistence counters.

Make ORB decisions deterministic by intended bucket: the first interval is exactly 08:30:00–08:30:05 CT. Record accepted, rejected, deadline-missed, or collector-unavailable results for every expected bucket, without assigning a later quote to an earlier interval. Restart recovery must retain gaps and identities. Do not rewrite September 22's missing/rejected live evidence.

Diagnose paired coverage by exact underlying, strike, expiration/settlement, call/put, generation, timestamp age, spread, and mapping state. Total cached quotes cannot substitute for usable matched pairs. Track planned versus active versus deferred contracts. Separate required SPX/NDX forecast readiness from optional RUT/VIX context and from recording requirements. Produce an explicit support matrix for SPY and QQQ; any new direct-equity subscriptions are a separate reviewed scope. VIX option parity remains forward context, not a cash-VIX baseline.

Acceptance: bounded replay/failure fixtures demonstrate no silent bucket loss, no fabricated early sample, no cross-generation admission, bounded queue behavior, and persisted gaps after restart. Then observe actual raw/rejection/eligible progress separately over longer than the capture cadence during a healthy session. Capture must not depend on already having completed the opening range it is intended to observe.

### B2. Daily close maintenance that survives late publication

Give each source/date an independent acquisition state: pending publication, downloaded, invalid, verified, committed, and scoring pending/completed. Fetch failures for one family must not prevent acquisition and retention for others. Use bounded retries with backoff, a next-morning catch-up, and an exchange-calendar gap queue. A not-yet-published close is retryable; do not import the previous date's value for the requested date.

Retain HTTP/fetch outcome, requested and actual session, artifact hash, parser version, and stderr/error category for every attempt. Avoid fixed candidate filenames that prevent a legitimate retry; each retry gets an immutable receipt. Separate history maintenance from whether a live prediction was made that day. Either introduce a reviewed per-symbol history commit contract or preserve complete bundles while retrying only missing members; never silently relax scoring's bundle contract.

Harden history qualification so a counted row resolves to retained source bytes, using immutable hash-keyed validation/caching to avoid repeated large-file reads in dashboard requests. Maintain a per-symbol coverage ledger, latest required/verified date, missing dates, last attempt, and next retry. Preserve actual retrieval/availability times; any independently proven original publication time must be stored as distinct evidence.

Acceptance: late publication, one-source outage, changed source content, duplicate task starts, restart mid-batch, holiday/early-close boundaries, missing artifact bytes, duplicate closes, and explicit corrections have meaningful tests. A missed day is discovered and reconciled without manual discovery or overwriting existing evidence. Backup and restore are tested for both database and artifact vault.

## Phase 3: integrated app acceptance

B integrates the UI using A's contract. Display status connection freshness, raw source freshness, latest accepted calculation, matched pairs, universe provenance, eligible persistence, ORB coverage, and daily history separately. Do not label a recent diagnostic payload as a fresh market tick. Keep the headline abstention reason concise and expose detailed source evidence on expansion.

Show service availability, data eligibility, and model validation as different states. Positive-horizon screens show actual target session, qualified/required returns, latest verified session, current-input blocker, and research-only status. A healthy API or a sufficient history count is not a forecast. A 0-session selection must not blame the daily-history gate when its actual blocker is missing current-session input.

After offline checks, A activates the integrated code only when authorized and ownership is established. Verify loaded fingerprints and both API/UI postconditions. Observe **three consecutive openings** as a proposed operational burn-in, not a model-performance sample requirement. For each supported live symbol require current admitted inputs, an actual eligible first bucket under healthy conditions, complete decision coverage, advancing persisted eligible records, and post-close maintenance through the next required session. If a gate fails, retain the precise failure and do not mark the session accepted.

## Backfill scope: what it can and cannot establish

| Requirement | Can historical data help? | Completion standard |
|---|---|---|
| 60 qualified daily returns | **Yes; source availability and offline qualification are OBSERVED.** | Minimum 61/65/70 consecutive qualifying closes for horizons 1/5/10, current through the required previous session, plus valid current inputs to issue a candidate. |
| Missing live ORB | Only a separate historical reconstruction, if appropriate source coverage exists. | Preserve the live miss. Label replay as reconstructed research with exact schema, timestamps, definitions, gaps, and source manifest. |
| 60 held-out model/expiration sessions | Potentially a properly designed historical evaluation; downloading closes alone is insufficient. | Frozen model/feature pipeline, point-in-time inputs and outcomes, chronological held-out design, overlap purging/embargo where applicable, declared baselines, coverage/error/calibration evidence, and existing promotion criteria. Retrospective evaluation is not prospective live validation. |

The new 70-close batch has **69, 65, and 60 return windows** at horizons 1, 5, and 10. Overlapping return windows are not 60 independent market sessions for statistical confidence. A newly downloaded historical close is available to the app now, not automatically at its original date. Do not backdate it to manufacture historical forecast decisions. The current evaluator correctly prevents such hindsight forecasts without appropriate point-in-time availability evidence.

Historical OPRA recovery is a separate, bounded second-stage project. The repository has `tools/backfill_closing_tape_history.py` and an import tool, with plan-hash, byte, and estimated-cost controls. First inventory entitlements/schema/date coverage and run a small 1–2-session pilot after any required spend authorization. Databento documents historical TCBBO and CBBO-1s availability, but trade-triggered TCBBO is not equivalent to complete interval quote evidence for an exact first five-second bucket. Select the appropriate quote schema, definitions, and as-of OI; verify gaps and replay semantics before scaling. Never merge reconstructed records into the immutable live-capture ledger or claim a paid estimate is a hard billing cap.

QQQ is outside the existing five-symbol approved-close adapters. Add and validate an exact official QQQ source separately; no SPY/NDX proxy substitutions. It need not block recovery of SPX/NDX/RUT/VIX/SPY.

## Phase 4: model research after inputs are reliable

Future option expirations may be useful context for a 0DTE forecast, but they are not substitutes for missing same-day pairs and do not change the forecast target date. The current blend is disabled; leave it disabled during reliability work. Test later-expiry term structure, relative gamma concentration, and disagreement as separately labeled shadow features against a frozen 0DTE-only baseline.

Audit time-to-expiration/settlement handling first. The current helper floors its year fraction at the default 1/365 year, approximately one day. Evaluate actual remaining seconds, AM/PM settlement, expiry identity, and early closes offline. Any formula correction or expiry-weight change needs explicit versioning, replay comparison, and leakage-safe evaluation before deployment. More data does not by itself validate either a gamma-pin close forecast or a confidence interval.

## Copy-ready agent briefs

**Agent A:** Implement Phase A1 and A2 from this plan in an isolated worktree built from the agreed preserved baseline. Own clock/universe admission, generation handoff, raw capture independence, exact ORB decision coverage, and the versioned health/status contract. Preserve all fail-closed gates, budgets, existing history, and unrelated changes. Do not add subscriptions or tune formulas. Deliver focused fault/replay tests, a reviewed task-only diff, and a bounded live-verification procedure. You are the sole process owner; do not restart an unverifiable process. Coordinate API fields and activation with Agent B.

**Agent B:** Implement Phase B1 and B2 from this plan in an isolated worktree built from the same baseline. Use the retained 70-session official-source qualification as candidate evidence. Own source retention, history-only ingestion without scoring side effects, append-only revision handling, per-source retry/gap maintenance, hash-backed research qualification, and final dashboard integration. Stage and validate in a scratch database before a separately authorized canonical import. Preserve real acquisition times and all old records. Do not restart the live backend or change forecast/model gates. Deliver before/after inventory, idempotency and provenance tests, and explicit remaining current-input blockers. Coordinate import ownership and shared contracts with Agent A.

## Source references and limits

- Official sources observed in this review: [Cboe SPX history](https://cdn.cboe.com/api/global/us_indices/daily_prices/SPX_History.csv), [Cboe RUT history](https://cdn.cboe.com/api/global/us_indices/daily_prices/RUT_History.csv), [Cboe VIX history](https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv), [Nasdaq NDX history request](https://api.nasdaq.com/api/quote/NDX/historical?assetclass=index&fromdate=2026-06-11&todate=2026-09-22&limit=500), [NYSE SPY history](https://www.nyse.com/api/nyseservice/v1/quotes?symbol=SPY). A successful bounded probe does not promise future endpoint availability, publication time, or licensing beyond the requested use.
- [Databento OPRA schema/history discussion](https://databento.com/blog/opra-improvements-coming-soon) supports evaluating quote versus trade-triggered historical schemas; account entitlement, per-date completeness, and cost remain unverified.
- [Databento time synchronization guidance](https://databento.com/docs/architecture/ntp-service) is a reference for the clock investigation, not evidence that changing the host's peer alone will resolve the observed condition.

This planning run downloaded and qualified public source artifacts and wrote this plan only. No database import, service restart, subscription change, model/threshold change, or rewrite of a market record occurred. The output packet uses create-only files, hashes, and read-only attributes for local tamper detection; this is not an external WORM guarantee. Production adoption, durable backup/restore, next-open capture, and model performance remain to be demonstrated separately.
