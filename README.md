# MarketPinPredictor

MarketPinPredictor is a Python market-index prediction system with:

- a FastAPI backend in `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app/api/main.py`
- a Streamlit dashboard in `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app.py`
- canonical gamma-exposure logic in `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app/core/gex.py`

## Active entry points

- API server: `/home/runner/work/MarketPinPredictor/MarketPinPredictor/server.py`
- FastAPI app: `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app/api/main.py`
- Streamlit app: `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app.py`

The files `app_new.py`, `app_backup.py`, and `clean_app/app.py` are legacy or alternate copies and should not be treated as the primary runtime surface unless you are explicitly working on them.

## Requirements

- Python 3.9+
- Optional: `Massive_API` environment variable for live Polygon/Massive market data
- Optional: `DATABASE_URL` for PostgreSQL-backed persistence

## Install

Fast local install:

```bash
pip install -r requirements_local.txt
```

Full environment with `uv`:

```bash
uv pip install .
```

## Run the backend

```bash
python server.py
```

Or:

```bash
python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000
```

Smoke check:

```bash
curl http://127.0.0.1:8000/health
```

## Run the dashboard

```bash
streamlit run app.py
```

## Tests

Targeted GEX tests:

```bash
python -m pytest tests/test_gex.py -q
python -m pytest tests/test_gex_invariants.py -q
```

Targeted API and legacy-surface tests:

```bash
python -m pytest tests/test_api_health.py tests/test_legacy_exports.py -q
```

Run all tests:

```bash
pytest -v
```

## Notes for contributors

- Keep changes minimal and localized.
- Preserve GEX sign conventions and invariants by reusing `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app/core/gex.py`.
- Preserve existing FastAPI and Streamlit data shapes.
- Prefer adding focused tests for stable behavior over broad refactors.
- Legacy exports in `/home/runner/work/MarketPinPredictor/MarketPinPredictor/app/__init__.py` are intentionally unsupported and exist only to fail with clear guidance.
