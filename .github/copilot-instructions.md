# MarketPinPredictor - Copilot Instructions

## Project Overview

MarketPinPredictor is a real-time stock index price prediction system that leverages options gamma exposure (GEX) and market microstructure to forecast intraday price movements for major US indices (SPX, NDX, DJI, RUT).

**Core Functionality:**
- Real-time gamma exposure calculation from options data (Polygon.io API)
- Machine learning price prediction using VWAP, gamma pins, flow urgency, and microtrends
- FastAPI backend for data ingestion and model serving
- Streamlit frontend for visualization and interactive analysis
- Automated snapshot collection and backtesting infrastructure

## Tech Stack

**Backend:**
- Python 3.9+
- FastAPI (async REST API)
- SQLAlchemy 2.0+ (database ORM)
- PostgreSQL (production) / SQLite (development)
- WebSocket streaming for real-time data

**Data & ML:**
- Polygon.io API (market data provider)
- PyTorch, TensorFlow, scikit-learn, XGBoost (ML models)
- pandas, numpy, scipy (data processing)
- plotly (visualization)

**Frontend:**
- Streamlit (interactive web UI)

**Testing:**
- pytest (unit and integration tests)

## Project Structure

```
/app/                   # Modular backend application
  /api/                 # FastAPI routers and endpoints (main.py)
  /core/                # Core business logic (GEX calculation, predictions)
  /features/            # Feature engineering (VWAP, microtrend, gamma pins)
  /ingest/              # Data ingestion (Polygon WebSocket, REST backfills)
  /models/              # SQLAlchemy models and DB schema
  /services/            # Business services (calibration, persistence)
  /state/               # In-memory state management (ring buffers)
  /utils/               # Utilities (market time, settings, sanitization)

/tests/                 # Unit and integration tests
/exports/               # NDJSON snapshot exports (by symbol and date)
/logs/                  # Log files and audit snapshots

app.py                  # Main Streamlit UI
server.py               # FastAPI server launcher
gamma_scheduler.py      # Background scheduler for snapshot collection
database.py             # Database initialization
```

## Coding Standards

**General:**
- Use type hints for all function signatures
- Follow PEP 8 style guide (use `black` for formatting if available)
- Docstrings for public functions (prefer Google-style)
- Keep functions focused and under 50 lines when possible

**Naming Conventions:**
- `snake_case` for variables, functions, and module names
- `PascalCase` for class names
- `UPPER_SNAKE_CASE` for constants
- Use descriptive names: `gamma_exposure` not `gex_val`

**Error Handling:**
- Use specific exception types (avoid bare `except:`)
- Log errors with context using the `logging` module
- Fail fast for invalid data in critical paths (GEX computation, predictions)

**Security:**
- Never commit API keys or secrets (use environment variables)
- Use `html_escape` for any user input displayed in Streamlit UI
- Validate all external API responses before processing

## Data Conventions

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
- All times in Eastern Time (ET) for market hours
- Market hours: Mon-Fri, 9:30 AM - 4:00 PM ET
- Use `pytz` for timezone conversions

## Build & Test Commands

**Setup:**
```bash
# Install dependencies
pip install -r requirements_local.txt
# OR using uv (faster)
uv pip sync pyproject.toml
```

**Run Tests:**
```bash
# All tests
pytest

# Specific test file
pytest tests/test_gex.py

# With verbose output
pytest -v
```

**Run Application:**
```bash
# FastAPI backend (default port 8000)
python server.py

# Streamlit UI (default port 8501)
streamlit run app.py

# Verify gamma setup
python verify_gamma_setup.py
```

**Database:**
```bash
# Initialize database
python -c "from database import init_db; init_db()"

# Set DATABASE_URL for PostgreSQL (optional, defaults to SQLite)
export DATABASE_URL="postgresql://user:pass@host:port/db"
```

**Environment Variables:**
```bash
# Required for live data (can run in demo mode without)
export POLYGON_API_KEY="your_key_here"
```

## Key Implementation Notes

**Performance Targets:**
- p99 end-to-end prediction latency: ≤200ms after 3:45 PM ET
- GC pauses: <20ms during critical period (3:45-4:00 PM ET)
- Memory: No heap drift during a session (ΔRSS < 100 MB)

**Critical Code Paths:**
- GEX computation: `app/core/gex.py` - Highly performance-sensitive, maintain invariants
- Gamma scheduler: `gamma_scheduler.py` - Runs in background, collects snapshots
- Prediction engine: `app/core/predictions.py` - Uses calibrated coefficients from DB
- WebSocket streaming: `app/ingest/` - Backend-only, NOT used in Streamlit UI

**Testing Strategy:**
- Unit tests for GEX calculation invariants (critical)
- Integration tests for API endpoints
- Validation scripts for gamma setup and data pipelines
- No test fixtures committed (use recorded-minute data if needed)

**Common Pitfalls:**
- Never mix Streamlit session state with FastAPI global state
- Always validate GEX invariant: `assert total_gex >= abs(net_gex)`
- Use correct Polygon ticker format: `I:SPX` for index data
- Handle market hours correctly (check `is_market_open()` before live data calls)
- Snapshot files are append-only NDJSON - never overwrite

## Resources

- [GAMMA_SETUP.md](../GAMMA_SETUP.md) - Gamma snapshot automation setup
- [IMPLEMENTATION_SUMMARY.md](../IMPLEMENTATION_SUMMARY.md) - Recent implementation details
- [CODE_EXPORT.md](../CODE_EXPORT.md) - Full codebase documentation
- [Polygon API Docs](https://polygon.io/docs) - Market data API reference
