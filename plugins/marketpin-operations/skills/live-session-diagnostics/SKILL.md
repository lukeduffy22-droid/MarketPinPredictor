---
name: live-session-diagnostics
description: Diagnose or monitor a MarketPinPredictor Databento, FastAPI, Streamlit, audit, and SQLite session. Use for stale SPX/NDX/VIX data, wrong-looking gamma values, service health, guarded recovery, or post-close evidence checks; do not use for generic finance research.
---

# MarketPin Live Session Diagnostics

Resolve the current repository from the user's workspace. Confirm it contains `backend/api/routers/health.py`, `ensure_market_app.ps1`, and `app.py`. Read repository instructions and current source before relying on legacy documentation. Preserve dirty worktrees, databases, WAL files, logs, audits, and runtime artifacts.

Use the packaged `host-workspace-operator` policy for workspace access. Start read-only. Use the packaged `sandbox-python-executor` when deterministic parsing, SQLite inspection, or verification materially improves confidence and the host exposes Python.

## Establish context

Record the assessment timestamp and timezone. MarketPin's regular US cash-session window is normally 08:30-15:00 America/Chicago on open weekdays. Verify holiday status when it changes the conclusion. Before open, warming can be normal; after close, evaluate final audits and persistence instead of expecting message progression.

## Evidence gates

Inspect the smallest relevant scope and classify the session as `healthy`, `warming/off-hours`, `degraded`, or `failed`.

During regular hours, require applicable evidence for:

- backend and dashboard reachability;
- transport counters advancing across samples;
- current subscription generation and handoff state;
- fresh, valid SPX and NDX audits, with VIX evaluated separately when its chain is thin;
- gamma invariants and explicit invalid reasons;
- SQLite WAL mode, integrity, required-table coverage, timestamps, and persistence advancement;
- current runtime-log anomalies.

Open ports alone do not prove health. A single database snapshot proves integrity and coverage, not advancement. Never turn stale, missing, or invalid data into a plausible zero or historical fallback.

## Reconcile a questionable value

Before action, capture the UI value and timestamps, the backend payload for the same symbol/time bucket, the newest matching audit artifact, and the latest matching SQLite row through a read-only connection.

Classify the mismatch:

- UI older than backend/audit: stale UI or browser session.
- Audit and SQLite agree while UI differs: display or cache defect.
- Audit and SQLite disagree: serialization, transaction, or database-routing defect.
- Audit and SQLite both carry an invalid reason: feed, formula, coverage, or validation gate.

When available, run a repository-provided offline verifier only after inspecting its command and side effects.

## Recovery boundary

Do not restart anything for a status or diagnosis request. Recovery is allowed only when the user asks to start, fix, recover, or keep the session running and evidence identifies a failed component.

Before restarting, inspect the owning process and verify it belongs to the resolved repository. Restart only the failed component using the repository's guarded launcher. Never automatically run a preflight or cleanup command that can stop unrelated port owners. Repeat the full evidence check after recovery.

## Completion

Lead with the state and give concise supporting evidence: timestamp/session phase, endpoints, transport delta, symbol freshness/validity/generation, audits and invariants, SQLite integrity/coverage/advancement, log anomalies, actions taken, and post-action proof. Label predictions and price targets as analytical estimates, not guarantees or financial advice.
