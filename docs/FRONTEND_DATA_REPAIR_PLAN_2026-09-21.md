# Frontend data and research-service repair plan

Date: 2026-09-21. Operator timezone: America/Chicago.
Status: proposed implementation plan; no runtime activation authorized by this document.

## Outcome and scope

First repair the `Research service unavailable` messages in the multi-day panel. Then address the related observed-bar/AI, GEX display, RUT, and expiration-coverage gaps identified in the same review. A functioning service is not a promise that every symbol or horizon has enough evidence for a forecast.

Completion means the UI displays a verified research estimate where eligible, or a specific, accurate reason and required evidence where unavailable. It must not manufacture forecasts, disguise invalid input, or lower validation thresholds to fill the screen.

## Evidence baseline

These runtime findings were observed during the preceding diagnosis on September 21, approximately 14:21-14:41 CT. Refresh them before implementation or activation; they are not a permanent status assertion.

- Port 8001 refused connections. The frontend groups symbols by horizon, then collapses transport, HTTP, schema, and decoding failures into the same generic warning.
- Current source calls `tools/start_forecast_research.ps1` from `start_databento_app.ps1`, but `ensure_market_app.ps1` has no research-service integration. The standalone research launcher reports a PID without waiting for post-launch readiness. This establishes a supervision gap, not proof of the historical reason the process was absent.
- The research evaluator supports horizons 0-10. Horizons 1-10 additionally require current verified input and at least 60 qualified horizon-return samples; starting the API alone cannot create those inputs.
- The Databento frontend adapter unconditionally supplies an empty indicator dataset. The existing EQUS.MINI validator reported a license denial. Those are separate integration and entitlement blockers.
- GEX chart data is populated from the top-five-strike summary. The retained NDX calculation at 14:21 CT had 112 calculated calls and four puts, not an entirely missing put side.
- RUT had 266 active primary contracts but only three usable quote pairs at the latest check, below five. No valid RUT audit was present that session at the time of inspection.
- 1,078 later-expiration contracts were deferred. Promotion was blocked by coverage/transport conditions and missing exact 60-minute opening-range evidence.
- Independent reconstruction reproduced NDX pin 30,500/max pain 29,440 and SPX pin 7,775/max pain 7,645 from hash-verified retained evidence. Arithmetic agreement does not establish complete coverage or validate model assumptions.

## Safety and change boundaries

- Use `C:\Cprojectsgpu_app\MarketPinPredictor` and its `.venv\Scripts\python.exe`. Preserve existing dirty work and stage only named repair files if publication is later requested.
- Keep Databento-only operation. Do not reinstall Polygon or resurrect its fetch paths.
- Keep production database reads query-only from research. Research forecasts belong in their separate journal; never rewrite old forecasts, audits, input blobs, or close records.
- Planning and offline tests do not authorize a process launch/restart, scheduled-task mutation, additional provider connection, paid entitlement, universe change, or formula promotion.
- Coordinate one runtime owner. Recheck process ownership, loaded code, active epoch/generation, and pending monitoring obligations before an approved deployment. Do not reset monitor outboxes or alter the pinned heartbeat protocol.
- Research failure must not stop the production backend, Streamlit, OPRA capture, or closing recorder. Research must not create a second OPRA connection.

## Phase 1 - Restore the independent research service

Primary files: `tools/start_forecast_research.ps1`, `tools/run_forecast_research.py`, `backend/research_api.py`, `backend/market_research_service.py`, `start_databento_app.ps1`, `ensure_market_app.ps1`.

1. Reproduce the launch path in isolated tests. Check interpreter, imports, source/journal path separation, startup exceptions, existing logs, and port ownership. Determine whether the service never launched or exited; retain uncertainty if history is unavailable.
2. Harden the research-only launcher: serialize concurrent launch attempts, verify ownership/endpoint identity, preserve per-launch stdout/stderr, handle early process exit, and perform a bounded readiness check. Never evict an unknown port owner.
3. Extend the existing supervisor to check this independent component even when backend/dashboard are already running. Respect an explicit disabled setting; do not add a competing scheduler. Add bounded retry/backoff so repeated failures cannot create a restart storm.
4. Distinguish process liveness, research-worker progression, and per-symbol forecast eligibility. Expose safe error codes, capture timestamp/age, worker status, and code/source identity without exposing secrets.
5. Initial recovery should use retained local data only. Do not enable the optional equity collector or change subscriptions as a side effect of starting research.

Acceptance: after an explicitly approved research-only launch, the correct process owns loopback port 8001; `/health` has the expected schema; captures advance across at least two configured collection intervals. Requests to `/v1/research/forecasts` succeed for supported symbols and every horizon 0-10. Eligible results persist in the separate journal; an all-abstain cycle still proves worker progression without fabricating journal rows. Core process IDs/epoch/generation remain unchanged and its data/persistence continue advancing. Test missing service, early exit, disabled service, foreign port owner, concurrent launch, source DB lock, and stalled worker.

## Phase 2 - Make horizon availability usable and truthful

Primary files: `app/services/forecast_research_view.py`, `backend/forecast_repository.py`, `backend/forecast_horizons.py`, `backend/forecast_calendar.py`.

1. Return structured transport/timeout/HTTP/schema errors instead of one catch-all string. Present one service-level outage message plus per-symbol data reasons when the service is reachable.
2. Bind displayed results to the submitted symbol/horizon selection. Clear or explicitly label old selections, prevent stale results appearing current, and retain partial successes without concealing failed groups.
3. Show requested horizon, target trading-session date, source/as-of time, qualified history count, required count, and explicit abstention reasons.
4. Inventory current verified daily history per symbol/horizon. Preserve the 60-qualified-return-sample requirement and calendar continuity checks. If short, prepare a separate approved, licensed, provenance-preserving history import through the existing verified-close workflow; do not copy 0DTE targets across future dates.
5. Keep SPX/SPY, NDX/QQQ, RUT/IWM, and DJI/DIA identities and units distinct. Explicitly identify unsupported cash-index sources.

Acceptance: isolated API tests and Streamlit AppTest cover mixed horizons 0/1/5/10, every supported horizon boundary, weekends/holidays, stale/future captures, insufficient history, invalid symbol input, partial group failure, malformed responses, and changing selections. A rendered-page check proves the screenshot's generic outage disappears after service restoration; evidence shortages remain specific rather than becoming invented forecasts.

## Phase 3 - Connect observed bars before enabling indicator AI

Primary files: `backend/equity_context.py`, `backend/underlying_validator.py`, `app/services/live_data_client.py`, `app.py`.

1. Inventory licensed Databento datasets using retained account/error evidence first. Obtain a user decision before any additional entitlement or provider connection. Do not assume an EQUS.MINI license denial applies to every Databento dataset.
2. Decide the instrument contract: genuine supported index observations, or explicitly labeled ETF-context bars. ETF price/volume must not be rescaled and labeled as observed SPX/NDX/RUT cash data.
3. Use a single authorized collection owner and retained, completed bars. Add bounded read-only API access with instrument, dataset, schema, timestamps, session, gap coverage, and provenance.
4. Replace the unconditional empty indicator surface only when those verified bars satisfy the indicator-specific lookback and continuity requirements. Keep unavailable, incomplete, stale, and denied-source states explicit.
5. Run indicator-based AI only on eligible observed inputs. Keep any separate options-context narrative clearly labeled and subject to its own source gates.

Acceptance: denied entitlement and absent/gapped/stale bars never invoke indicator AI; valid fixture bars produce independently checked indicator values. An approved live trial proves captured bar -> retained record -> API -> frontend values/timestamps. Provider availability and frontend wiring must both pass; changing the warning is not acceptance.

## Phase 4 - Repair GEX visibility and investigate coverage

Primary files: `backend/databento_streamer.py`, relevant GEX response serialization, `app/services/live_data_client.py`, `app/utils/gamma_wall_evidence.py`, `app.py`.

1. Publish a bounded full calculated primary strike series separately from the top-five wall summary. Build it in the existing calculation worker; GET/UI requests must not recompute or query the provider.
2. Render call, put, and net series with explicit sign/units, calculated-side counts, and coverage. Preserve unknown sides as unknown; do not replace absent put calculations with zero.
3. Add per-side rejection counts: selected, active, received/current, positive OI, in-band, IV-eligible, and calculated. Preserve zero OI versus missing OI and report unverified OI timing.
4. Trace NDX put sparsity and RUT pair losses through mapping, current generation, quote freshness, OI joins, strike pairing, and IV bounds. Reproduce defects on retained inputs before changing source behavior.

Acceptance: full-series side sums reproduce retained call/put/net/gross totals; fixtures include puts outside the top five, mixed sides at a strike, missing sides, zero OI, and asymmetric coverage. RUT is only marked usable when its real inputs pass existing gates. If source evidence remains insufficient, report the unresolved source limitation rather than claiming the repair restored valid predictions.

## Phase 5 - Resolve later-expiration availability safely

Primary files: `backend/databento_streamer.py`, expiration-view adapters, and staged-subscription tests.

1. Show selected, active, deferred, quoted, and calculated counts separately for each expiration, with exact blocking reasons.
2. Correct any demonstrated startup/coverage defect that prevented existing gates from passing. A missed exact opening bucket cannot be recreated retrospectively; verify the next session's capture where required.
3. Review the coupling between all-four opening-range completion and supplemental-expiration research as a separate policy decision. Do not silently remove it, broaden subscriptions, or enable replay to make today's tables populate.
4. If a revised supplemental policy is explicitly approved, test bounded same-client subscription, transport limits, independent optional-family failures, and unchanged primary authority before activation. Keep cross-expiration estimates research-only.

Acceptance: an eligible staged transition is observed with exact identity/count receipts and advancing later-expiration quotes/calculations; an ineligible transition stays deferred with accurate reasons. No additional OPRA connection, primary-feed regression, or false opening-range completion.

## Phase 6 - Calculation assurance, not forced alignment

1. Add immutable regression fixtures for the independently checked NDX and SPX calculations, using an approved minimal/sanitized fixture format rather than committing raw live databases or tapes.
2. Recompute full-input IV/gamma/GEX and full-OI max pain independently. Test payout minimization, aggregation/signs, ties, expiry/settlement conventions, partial chains, and OI provenance. Do not use top-five-only verification as whole-chain proof.
3. Evaluate the current one-day minimum time-to-expiry against actual remaining settlement time offline. Any proposed change requires a separate formula version, regression evidence, and leakage-safe held-out evaluation before promotion.
4. Explain the differing input populations of pin and max pain in the UI. Do not force their values together or call either a guaranteed close.

Acceptance: stored baseline values reproduce within stated numerical tolerances, while coverage/assumption limitations remain visible. No formula or model promotion is part of the service/UI repair.

## Test and deployment sequence

- First use temporary databases, injected provider mocks, API tests without live lifespan collection, and launcher mocks. No test may contact Databento or mutate production records.
- Extend the existing forecast integration/horizon, equity context/adapter, live-data-client, gamma-wall, staged-subscription, and PowerShell launcher/supervisor suites. Add real Streamlit AppTest and rendered-page checks for the changed UI states.
- Deliver phases 1-2 first as the smallest complete repair for the attached screenshot. Phases 3-6 are related workstreams, not reasons to postpone restoring the research service.
- Before approved activation, record baseline process owners/IDs, source fingerprints, epoch/generation, telemetry counters, and persistence timestamps. Deploy only affected components with one owner; respect the dashboard's disabled file watcher.
- Roll back only the new source/configuration changes if capture progression, latency, counters, or identity checks regress. Do not revert/restore application databases or delete retained evidence. A rollback involving a process still requires verified ownership and activation authority.
- Report separately: implemented, offline verified, loaded in runtime, live verified, and blocked by external data/entitlement. Do not call the work complete merely because HTTP returns 200 or every card has a number.

## Decisions required before activation or expansion

1. Approval to implement phases 1-2, and later approval for the scoped research-service launch/deployment under existing process constraints.
2. Choice and entitlement confirmation for observed bars; default to preserving the current abstention until a legitimate source exists.
3. Separate approval for subscription-policy changes, historical provider retrieval/import, or any formula experiment beyond offline analysis.

The research readiness gates (clean sessions, labeled evidence, verified closes) remain intact. Their counters advance only from qualifying evidence; repairing services cannot retroactively create missing captures or outcomes.
