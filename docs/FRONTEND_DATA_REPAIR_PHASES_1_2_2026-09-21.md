# Frontend repair phases 1–2: implementation and offline evidence

Scope: phases 1–2 of `FRONTEND_DATA_REPAIR_PLAN_2026-09-21.md`. Implemented offline in the canonical checkout, on `feature/2026-09-legacy-score-backlog`, using `.venv\Scripts\python.exe`. No runtime activation is claimed.

## Implemented

- The research launcher uses an independent, cross-session mutex, canonical interpreter and absolute entrypoint, exact Python/entrypoint ownership, endpoint PID/schema/code/source identity checks, unique retained stdout/stderr logs, early-exit detection, and bounded readiness. The import/path check is bounded to 15 seconds and readiness to 20 seconds (each HTTP attempt is bounded to two seconds). It never stops a listener. Retry state records the launch PID and creation time; an unbound pending process requires review. Retry delay doubles from 60 seconds to a maximum of 30 minutes. Only observed completion of two captures clears the accumulated failure count.
- `ensure_market_app.ps1` checks research independently during ordinary upkeep, including when core services already listen. Failure is isolated from core upkeep. Explicit backend/dashboard operations remain scoped to those components. New watchdog supervision requires `MARKETPIN_FORECAST_RESEARCH_AUTOSTART=1`; unset does not activate it. `0` explicitly disables both launch paths. Existing manual full-stack research startup behavior is retained. No environment or scheduled-task settings were changed.
- The standalone runner constructs no equity collector, including when an equity environment setting is enabled. It uses retained local data and the existing loopback runtime-context read; it creates no OPRA/provider client. `--check` validates imports and database path separation without starting a worker, API, provider, or journal. Separate source/journal paths and hard-link aliases are checked.
- Health distinguishes process identity, worker thread status, capture attempts/completions, last attempt/completion times, capture age, and safe error codes. All-abstain cycles advance completion counters without fabricated journal rows. A source lock is explicit; its history counts remain unknown rather than becoming zero. Stale/future captures and stopped/erroring workers suppress estimates without modifying retained state.
- The frontend classifies timeout, transport, HTTP, decode, schema, and invalid-selection failures. It validates symbol/horizon binding before retaining a group, shows one aggregate request warning, keeps successful groups, and includes failed groups in the table. Saved results display their submitted selection/time; changed selections hide previous results until submission. Saved numbers are checked again for age on every render.
- Rows show horizon, target session, source/as-of times, qualified/required return counts, latest verified daily session, units, and specific abstention reasons. Index and ETF identities remain distinct; unsupported direct cash-index sources and the absence of a DJI cash source are explicit.
- The daily-only repository read and history inventory use existing normalization, provenance, session continuity, 300-observation cap, and 60-return requirement. Forecast equations, calendar rules, and promotion thresholds were not changed.

## Verification

Initial targeted checks confirmed no port-8001 listener and no retained launch logs at `output/forecast_research`. This does not determine whether an earlier launch never occurred or exited. Canonical interpreter/import checks passed. The investigation did not repeat the earlier provider, subscription, GEX, or ORB diagnosis.

Offline Python/API/Streamlit/calendar/launcher-contract suite: **137 passed**. Reports: `output/frontend_repair_20260921/pytest.xml`.

Research PowerShell suite: **16 passed on both PowerShell 7 and Windows PowerShell 5.1**, including actual mutex contention between independent runspaces, with service process/listener/HTTP operations mocked. Reports: `output/frontend_repair_20260921/research-supervisor.xml` and `research-supervisor-winps.xml`.

Existing core PowerShell supervisor suite: **163 passed**. Report: `output/frontend_repair_20260921/core-supervisor.xml`. Its bounded helper-process tests use isolated test processes; no application services are started or stopped.

Coverage includes every API horizon 0–10 and all 25 tracked symbols, invalid input, mixed 0/1/5/10 selections, weekends/holidays and early closes, missing/future/stale inputs, insufficient/gapped/unverified daily history, source locks, all-abstain progression, journal separation, safe worker failure, malformed responses, partial group failures, selection changes, and rendered outage/restoration states. The rendered check uses real Streamlit AppTest with injected transport; it is not a deployed-browser acceptance receipt.

The initial dirty-file hashes and the staged diff were unchanged at the preservation check. Additional concurrent diagnostic work appeared in the checkout and was left untouched. No staging, commits, branch changes, resets, production-data changes, provider connections, subscription changes, or service control were performed for this repair.

## Retained history and separate import proposal

Read-only snapshot: **2026-09-21 20:47:54 UTC / 15:47:54 CT**, `data/market_data.db`, through `mode=ro` and `PRAGMA query_only=ON`, daily history only. Full per-symbol/horizon inventory: `output/frontend_repair_20260921/history_inventory.json`.

| Symbols | Qualifying daily closes each | Latest qualifying date | Qualified returns h=1 / h=5 / h=10 |
| --- | ---: | --- | --- |
| SPX, NDX, RUT, VIX, SPY | 6 | 2026-09-02 | 4 / 0 / 0 |
| DJI, QQQ, DIA, IWM, RSP, VTI, MDY, XLK, XLF, XLY, XLP, XLE, XLI, XLV, XLU, SMH, TLT, IEF, HYG, LQD | 0 | None | 0 / 0 / 0 |

No symbol meets the 60-return floor for any positive horizon. A sufficient continuous history needs at least 61, 65, and 70 qualifying closes for horizons 1, 5, and 10 respectively, with all intervening sessions retained and the latest session current. A current verified input is also required; history alone cannot issue a forecast.

Separate proposed history work, requiring approval before retrieval/import:

1. Confirm licensed official, unadjusted close evidence for the exact symbol. The existing verified-close workflow supports SPX (Cboe/S&P Global), NDX (Nasdaq), RUT (Cboe/FTSE Russell), VIX (Cboe), and SPY (NYSE Arca). Other symbols need a separately reviewed source contract; do not bypass the allowlist or substitute paired ETFs/cash indexes.
2. Assemble at least 70 continuous reviewed sessions through the prior required cash session for horizon-10 eligibility. Retain original licensed artifacts, their SHA-256 hashes, original observation timestamps, actual ingestion availability, exact dates/symbols/units, and explicit correction lineage. Missing sessions remain missing.
3. Prepare the existing `tools/ingest_verified_closes.py` CSV contract (`symbol`, `trading_date`, `official_close`, `source`, `source_reference`, `source_artifact_sha256`, `source_artifact_path`, `observed_at_utc`, `correction_of_id`). Validate against a temporary database using the existing verified-close tests before a separately approved append-only production import. Do not request prediction scoring as a side effect. Never rewrite earlier closes or research forecasts, and never backdate ingestion availability.
4. Re-run the same history inventory after that approved import. Do not copy a 0DTE target onto future dates or loosen the 60-return/current-input gates.

No historical retrieval or import was performed or scheduled.

## Remaining activation steps

Status: **implemented and offline verified**. **Loaded runtime and live verification: not yet verified.** History insufficiency remains an external evidence blocker.

1. Obtain scoped activation authorization and establish one runtime owner. Refresh exact core process owners/PIDs, loaded fingerprints, epoch/generation, current persistence timestamps/counters, and monitoring obligations. Preserve existing monitor outboxes/protocols.
2. Under that authorization, use the research-only launcher, verify the correct owner of loopback 8001 and both health schemas, and compare loaded code/source identity to `--check`. Do not invoke a full-stack launcher just to activate research. A foreign owner, an unbound pending process, or a stalled worker must be reviewed rather than automatically killed.
3. Observe at least two configured collection intervals. Require increasing completed-capture count and advancing capture timestamps. An all-abstain result is valid worker-progression evidence; only eligible estimates should append to the separate research journal. Verify core PIDs/epoch/generation remain unchanged and core persistence continues advancing.
4. Activate the changed dashboard through the separately approved, ownership-verified dashboard procedure; its file watcher is disabled. Exercise mixed horizons in the deployed page. API 200 alone is not acceptance, and evidence shortages should remain visible.
5. If ongoing upkeep is approved, enable `MARKETPIN_FORECAST_RESEARCH_AUTOSTART=1` in the existing supervisor's execution environment. Do not add a second scheduler. Roll back only repair source/configuration if necessary; any process action still requires scoped authority, and databases/tapes remain intact.
