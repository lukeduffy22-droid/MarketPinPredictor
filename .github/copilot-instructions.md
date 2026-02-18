# Copilot Instructions for MarketPinPredictor

Guidance for coding agents working in this repository. Keep changes minimal and localized to the issue at hand.

## Architecture & Conventions
- Product split: FastAPI backend (`app/api/main.py`, launched via `server.py`) and Streamlit dashboards (`app.py`, `main.py`). Do not mix their state or dependencies.
- Gamma exposure invariant: `total_gex >= abs(net_gex)` must always hold. Tests in `tests/test_gex.py` and `tests/test_gex_invariants.py` guard this.
- Performance targets (backend): p99 latency ≤200ms; GC pauses <20ms during 3:45-4:00 PM ET. Avoid heavy allocations or synchronous I/O in hot paths.
- Ticker format: use Polygon index tickers like `I:SPX`.
- Snapshot data: append-only NDJSON files; never rewrite historical rows.

## Critical Paths
- `app/core/gex.py` — performance-sensitive GEX computation and invariants.
- `gamma_scheduler.py` — background snapshot collection and scheduling.
- `app/ingest/` — WebSocket streaming (backend-only; not used by Streamlit).

## Build & Test
```bash
pip install -r requirements_local.txt
pytest
python server.py       # FastAPI backend
streamlit run app.py   # Streamlit frontend
```

## Coding Standards
- Preserve existing API contracts and GEX sign/invariant behavior.
- Reuse existing utilities under `app/` before adding new modules; avoid refactors outside the issue scope.
- Keep edits minimal; do not adjust unrelated modules or tests.
- Prefer targeted tests first (e.g., `python -m pytest tests/test_gex.py -q`) before broader runs.

## Validation Checklist
- For backend changes: `python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000` then verify `GET /health`.
- For Streamlit changes: `streamlit run app.py` locally and validate key views manually.
- For GEX logic changes: run `python -m pytest tests/test_gex.py -q` and `python -m pytest tests/test_gex_invariants.py -q`.

## Common Pitfalls
- Mixing Streamlit session state with FastAPI global state causes cross-request leakage—keep them isolated.
- Using non-Polygon ticker formats will break ingestion; always prefer `I:SPX` style.
- Rewriting snapshot files breaks append-only expectations; only append new NDJSON lines.
- Avoid blocking calls or heavy allocations in hot paths to meet latency/GC targets.

## Task Execution Tips
- Start with a short plan and keep scope tight (e.g., “update `app/core/gex.py` and `tests/test_gex.py` only”).
- State acceptance criteria, tests to run, and allowed files up front.
- If a chat run fails, retry with fewer files and narrower prompts.
