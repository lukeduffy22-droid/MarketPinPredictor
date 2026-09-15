# MarketPinPredictor Copilot instructions

## Current architecture

- `start_databento_app.ps1` is the canonical Windows launcher for the complete local stack.
- `backend/app.py` is the canonical FastAPI application; `server.py` launches `backend.app:app`.
- `app.py` is the Streamlit dashboard.
- `backend/databento_streamer.py` owns live Databento market-data ingestion and publication.
- `backend/closing_tape/` owns OPRA TCBBO recording, replay, integrity, readiness, evaluation, and promotion.
- `backend/prediction_authority.py`, `backend/prediction_passport.py`, and `backend/workstation.py` enforce provenance and authority contracts.
- `app/core/gex.py` remains the source of truth for GEX formulas and sign conventions.

Do not redirect new work to the older Polygon-first `app/api/main.py` path unless the issue explicitly targets compatibility code there.

## Evidence contract

Keep two truths separate throughout schemas, APIs, UI labels, tests, and documentation:

- **Observed** data is provider evidence: immutable trade and pre-trade NBBO records, definitions, event and receive timestamps, corrections, flags, and daily open-interest statistics.
- **Inferred** data is an estimate derived from observed evidence: buyer/seller-side heuristics, flow classifications, positioning estimates, model outputs, and forecast ranges.

OPRA does not publish aggressor side or participant holdings. Never label inferred side, dealer inventory, or positioning as observed fact.

Fail closed when evidence is stale, incomplete, invalid, internally inconsistent, or too thin. Preserve the explicit reason for `ABSTAIN`, `RESEARCH_ONLY`, or `NOT_READY`; do not replace it with a plausible value. CUDA availability, record volume, or a successful process start is not proof of forecast accuracy or production readiness.

Research and shadow results cannot self-promote. Production authority requires the existing lifecycle, maturity, evaluation, calibration, and promotion gates. Do not bypass those gates in an API, dashboard helper, test fixture, or maintenance script.

## Repository boundary

Git contains source, tests, configuration templates, and reviewed documentation. It must not contain credentials or machine/runtime evidence such as:

- `.env` files or API keys;
- databases, WAL/SHM files, OPRA/DBN captures, parquet datasets, JSONL/NDJSON exports, logs, or generated reports;
- model weights or pickles;
- virtual environments, caches, compiled Python, `node_modules`, build outputs, Git bundles, or ZIP backups;
- embedded `pytorch` or `vision` repositories.

Use explicit file-scoped staging and review the staged name list. Never use `git add -A` in this repository. Do not delete or rewrite local capture files as part of a source change.

## Change workflow

1. State the files in scope, acceptance criteria, and tests before editing.
2. Search for an existing helper or contract before adding a parallel implementation.
3. Make the smallest behaviorally complete change and add focused tests.
4. Keep observed, inferred, research-only, and production-authorized fields visibly distinct.
5. Run targeted tests, then the relevant broader suite.
6. Run `python tools/check_publication_boundary.py --base origin/friday-1/9` before pushing.

Preserve unrelated work and do not reset, clean, force-push, merge unrelated histories, alter other worktrees, or restart the live feed unless the task explicitly requires it.

## Local development

Use the repository virtual environment on the Windows workstation:

```powershell
.\.venv\Scripts\python.exe -B -m pytest tests\test_closing_tape_readiness.py -q -p no:cacheprovider
.\.venv\Scripts\python.exe -B -m pytest tests\test_streamer_provider_selection.py -q -p no:cacheprovider
```

For a broader offline check:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
```

Do not start the backend and Streamlit independently for a normal live session. Use `start_databento_app.ps1`, and treat HTTP 200 or an open port as insufficient readiness evidence.

## Coding standards

- Support Python 3.11 in CI and keep imports grouped as standard library, third party, then local.
- Add type annotations for public interfaces and descriptive errors without secret values.
- Preserve API response shapes unless the change includes migration and compatibility coverage.
- Never mix Streamlit session state with backend global state.
- Use timezone-aware timestamps. Display timezone and market-session timezone are separate concerns.
- Preserve `total_gex >= abs(net_gex)` and the existing GEX sign conventions.
- Tests must use temporary databases and must not read or mutate the configured live database.
