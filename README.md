# MarketPinPredictor

MarketPinPredictor is a Python market-index prediction system with:

- a FastAPI backend in `app/api/main.py`
- a Streamlit dashboard in `app.py`
- canonical gamma-exposure logic in `app/core/gex.py`

## Runtime Quickstart

### 1) Configure live market data provider

Set at least one provider key:

- `DATABENTO_API_KEY` (preferred for market-open fallback polling)
- `Massive_API` (Polygon/Massive key)

Optional provider mode:

- `MARKET_DATA_PROVIDER=auto` (default, prefers Databento when key is set)
- `MARKET_DATA_PROVIDER=databento`
- `MARKET_DATA_PROVIDER=polygon`

### 2) Run setup verification

```bash
python verify_gamma_setup.py
```

### 3) Start backend

```bash
python server.py
```

Check health and active provider:

- `GET /health` returns `market_data_provider` and per-symbol freshness.

## CUDA + Institutional Training

### Check GPU

```bash
python tools/check_gpu.py
```

### Build full historical dataset and train all symbols

```bash
python tools/train_institutional_models.py --exports-dir ./exports --dataset-csv ./data/full_gamma_history.csv --rebuild
```

This command:

1. Builds one unified dataset from all NDJSON snapshots under `./exports`.
2. Trains SPX/NDX/DJI/RUT models with chronological splits.
3. Uses CUDA automatically when available.
4. Saves artifacts to `./models`.

## Active entry points

- API server: `server.py`
- FastAPI app: `app/api/main.py`
- Streamlit app: `app.py`

The files `app_new.py`, `app_backup.py`, and `clean_app/app.py` are deprecated guard entrypoints and intentionally exit immediately. Always run the dashboard from `app.py`.

## Production prediction architecture

- Live predictions are served by FastAPI `/predict/close` (time-adaptive ridge, versioned metadata, deterministic fallback defaults).
- Streamlit is a thin dashboard client and reads live prediction/gamma/health from backend endpoints.
- Offline/batch CUDA workflows remain in `train_gamma_model.py`, `predict_gamma_model.py`, and `tools/train_institutional_models.py`.
- Model diagnostics and provenance are exposed at:
  - `GET /models/live`
  - `GET /diagnostics/system`

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
- Preserve GEX sign conventions and invariants by reusing `app/core/gex.py`.
- Preserve existing FastAPI and Streamlit data shapes.
- Prefer adding focused tests for stable behavior over broad refactors.
- Legacy exports in `app/__init__.py` are intentionally unsupported and exist only to fail with clear guidance.
