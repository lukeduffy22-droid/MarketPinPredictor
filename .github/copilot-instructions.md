# Copilot Instructions for MarketPinPredictor

## Project context
- This repository contains a Python market-index prediction system with:
  - a FastAPI backend (`app/api/main.py`, launched via `server.py`), and
  - Streamlit dashboards (`app.py`, `main.py`).
- Core gamma-exposure logic and invariants live in `app/core/gex.py` and are validated by tests in `tests/test_gex.py` and `tests/test_gex_invariants.py`.

## Code changes
- Keep changes minimal and localized to the issue.
- Do not refactor unrelated modules while fixing a targeted issue.
- Preserve existing API contract and GEX sign/invariant behavior.
- Reuse existing utilities under `app/` before adding new modules.

## Validation commands
- Run targeted tests first:
  - `python -m pytest tests/test_gex.py -q`
  - `python -m pytest tests/test_gex_invariants.py -q`
- If backend code is touched, verify app startup:
  - `python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000`
  - check `GET /health` returns JSON

## Copilot task execution guidance (to avoid VS Code chat task failures)
- Break work into small, file-scoped tasks (for example: "update `app/core/gex.py` and `tests/test_gex.py` only").
- Always include explicit acceptance criteria (expected behavior, tests to run, and files allowed to change).
- Ask Copilot for a short plan first, then request implementation.
- If chat returns an error code before completion, rerun with a narrower prompt and fewer files in scope.
