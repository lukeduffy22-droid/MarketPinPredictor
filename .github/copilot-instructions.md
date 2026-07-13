# MarketPinPredictor - Copilot Instructions

## Project context
- Python market-index prediction system with:
  - FastAPI backend (`app/api/main.py`, launched via `server.py`); `/health` is the smoke check.
  - Streamlit dashboard (`app.py`) for visualization.
- Core gamma-exposure logic and invariants live in `app/core/gex.py` and are validated by tests in `tests/test_gex.py` and `tests/test_gex_invariants.py`. This module is the single source of truth for GEX formulas/sign conventions.

**Core Functionality:**
- Real-time gamma exposure calculation from options data (Polygon.io API)
- Machine learning price prediction using VWAP, gamma pins, flow urgency, and microtrends
- Automated snapshot collection and backtesting infrastructure

## Technology stack
- **Python**: >=3.9
- **Backend**: FastAPI, Uvicorn
- **Frontend**: Streamlit
- **Data processing**: NumPy, Pandas, SciPy
- **Machine Learning**: scikit-learn, XGBoost, TensorFlow, PyTorch
- **Visualization**: Plotly
- **Market data**: Polygon.io API client
- **Database**: PostgreSQL (production) / SQLite (development) with SQLAlchemy
- **Testing**: pytest
- **Package manager**: uv or pip

## Project structure
```
MarketPinPredictor/
├── app/
│   ├── api/          # FastAPI backend
│   │   └── main.py   # Main API application
│   ├── core/
│   │   ├── gex.py    # Core gamma-exposure calculations (single source of truth)
│   │   └── predictions.py  # Prediction engine (calibrated coefficients from DB)
│   ├── features/     # Feature engineering (VWAP, microtrend, gamma pins)
│   ├── ingest/       # Data ingestion (Polygon WebSocket, REST backfills — backend-only)
│   ├── models/       # SQLAlchemy models and DB schema
│   ├── services/     # Business services (calibration, persistence)
│   ├── state/        # In-memory state management (ring buffers)
│   └── utils/        # Utilities (market time, settings, sanitization)
├── tests/
│   ├── test_gex.py              # GEX functionality tests
│   └── test_gex_invariants.py   # GEX invariant validation
├── exports/          # NDJSON snapshot exports (by symbol and date)
├── logs/             # Log files and audit snapshots
├── server.py         # FastAPI server launcher
├── app.py            # Streamlit dashboard
├── gamma_scheduler.py  # Background scheduler for snapshot collection
├── database.py       # Database initialization
└── pyproject.toml    # Project dependencies
```

## Code changes
- **Keep changes minimal and localized** to the specific issue; avoid drive-by refactors.
- **Do not alter GEX sign conventions or invariants**; reuse `app/core/gex.py` helpers instead of duplicating logic.
- **Preserve existing API contracts** and data shapes for both FastAPI and Streamlit flows.
- **Reuse existing utilities** under `app/` before adding new modules or dependencies.
- **Follow existing code style** in the files you're modifying.
- **Add tests** for new functionality or bug fixes when appropriate.
- **Never mix Streamlit session state with FastAPI global state.**
- **Snapshot files are append-only NDJSON** — never overwrite.

## Coding standards
- **Type hints**: Use type annotations for function parameters and return values
- **Docstrings**: Add docstrings for public functions and classes (prefer Google-style)
- **Error handling**: Use appropriate exceptions and error messages; avoid bare `except:`
- **Testing**: Write tests that validate both happy path and edge cases
- **Imports**: Group imports (stdlib, third-party, local) with blank lines between groups
- **Naming**: Use descriptive variable names; follow PEP 8 conventions (`snake_case` for functions/variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants)
- **Security**: Never commit API keys or secrets; use `html_escape` for user input displayed in Streamlit UI

## Data conventions

**GEX Calculations:**
- Net GEX = (call_gamma × call_oi) - (put_gamma × put_oi) × multiplier
- Total GEX = abs(call_gamma × call_oi) + abs(put_gamma × put_oi) × multiplier
- **Critical Invariant:** `total_gex >= abs(net_gex)` must always hold
- Sign convention: Positive net GEX = call dominance, negative = put dominance

**Symbols:**
- Index tickers: SPX, NDX, DJI, RUT (internal format)
- Polygon format: `I:SPX`, `I:NDX`, etc. (for API requests)
- ETF fallbacks: SPY (SPX), QQQ (NDX), DIA (DJI), IWM (RUT)

**Timestamps:**
- All times in Eastern Time (ET) for market hours (Mon-Fri, 9:30 AM – 4:00 PM ET)
- Use `pytz` for timezone conversions

## Development setup
- Python 3.9+; install deps with `pip install -r requirements_local.txt` (fast path) or `uv pip install .` (full env).
- Avoid adding new dependencies unless required for the task.
- Keep secrets, API keys, and large artifacts out of the repo.

## Build and test commands

### Running tests
```bash
# Run targeted GEX tests first
python -m pytest tests/test_gex.py -q
python -m pytest tests/test_gex_invariants.py -q

# Run all tests
pytest -v
```

### Running the application
```bash
# Start FastAPI backend
python server.py
# Or directly with uvicorn
python -m uvicorn app.api.main:app --host 127.0.0.1 --port 8000

# Start Streamlit dashboard (in a separate terminal)
streamlit run app.py

# Initialize database
python -c "from database import init_db; init_db()"
```

### Validation commands
- **Run targeted tests first** before making changes to understand baseline behavior
- **For GEX changes**: Run `python -m pytest tests/test_gex.py tests/test_gex_invariants.py -q`
- **For backend changes**: Verify app startup and check `GET /health` endpoint returns JSON
- **For all changes**: Ensure existing tests still pass after your modifications

## Performance targets
- p99 end-to-end prediction latency: ≤200ms after 3:45 PM ET
- GC pauses: <20ms during critical period (3:45–4:00 PM ET)
- Memory: No heap drift during a session (ΔRSS < 100 MB)

## Common workflows

### Modifying GEX calculations
1. Review `app/core/gex.py` and understand current logic
2. Check `tests/test_gex_invariants.py` for invariants that must be preserved
3. Make changes while preserving mathematical invariants
4. Run both GEX test files to validate

### Fixing a bug
1. Write a test that reproduces the bug
2. Make the minimal fix in the relevant module
3. Verify the test now passes
4. Run related tests to ensure no side effects

## Copilot task execution guidance (to avoid VS Code chat task failures)
- **Break work into small, file-scoped tasks**: Example: "update `app/core/gex.py` and `tests/test_gex.py` only"
- **Include explicit acceptance criteria**: Specify expected behavior, tests to run, and files allowed to change
- **Ask for a plan first**: Request a short plan before implementation
- **Retry with narrower scope**: If chat returns an error code, rerun with a narrower prompt and fewer files in scope
- **Validate incrementally**: Test each change before moving to the next step

## Security and best practices
- **Never commit secrets**: Use environment variables for API keys and credentials
- **Validate inputs**: Always validate user inputs, especially in API endpoints and external API responses
- **Handle errors gracefully**: Provide meaningful error messages without exposing sensitive information
- **Test edge cases**: Consider boundary conditions and error scenarios in tests
