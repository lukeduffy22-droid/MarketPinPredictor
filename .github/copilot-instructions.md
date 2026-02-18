# Copilot instructions for MarketPinPredictor

## Project context
- Python market-index prediction system with:
  - FastAPI backend (`app/api/main.py`, launched via `server.py`); `/health` is the smoke check.
  - Streamlit dashboards (`app.py`, `main.py`) for visualization.
- Core gamma-exposure logic and invariants live in `app/core/gex.py` and are validated by tests in `tests/test_gex.py` and `tests/test_gex_invariants.py`. This module is the single source of truth for GEX formulas/sign conventions.

## Code changes
- Keep changes minimal and localized to the issue; avoid drive-by refactors.
- Do not alter GEX sign conventions or invariants; reuse `app/core/gex.py` helpers instead of duplicating logic.
- Preserve existing API contracts and data shapes for both FastAPI and Streamlit flows.
- Prefer existing utilities under `app/` before adding new modules or dependencies.

## Development setup
- Python 3.9+; install deps with `pip install -r requirements_local.txt` (fast path) or `uv pip install .` (full env).
- Avoid adding new dependencies unless required for the task.
- Keep secrets, API keys, and large artifacts out of the repo.

## Validation commands
- Run targeted tests first:
  - `python -m pytest tests/test_gex.py -q`
  - `python -m pytest tests/test_gex_invariants.py -q`
- If backend code is touched, verify app startup:
  - `python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000`
  - check `GET /health` returns JSON
- If working on dashboards, smoke test with `streamlit run app.py` (or `main.py` for alt UI) locally.

## Copilot task execution guidance (to avoid VS Code chat task failures)
- Break work into small, file-scoped tasks (for example: "update `app/core/gex.py` and `tests/test_gex.py` only").
- Always include explicit acceptance criteria (expected behavior, tests to run, and files allowed to change).
- Ask Copilot for a short plan first, then request implementation.
- If chat returns an error code before completion, rerun with a narrower prompt and fewer files in scope.
