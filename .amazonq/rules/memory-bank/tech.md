# MarketPinPredictor — Technology Stack

## Languages
- Python 3.10+ (primary)
- PowerShell (startup scripts, integration tests, supervisor)
- C++ / CUDA (vendored PyTorch source in `pytorch/`)
- TypeScript / Next.js (campaign-concept-studio sub-project, separate)

## Core Frameworks

### Web / API
- FastAPI >= 0.121.1 (backend REST API, port 8001)
- Uvicorn >= 0.38.0 (ASGI server, standard extras)
- Pydantic >= 2.12.4 (data validation, settings)
- pydantic-settings >= 2.11.0 (`.env` loading)
- Streamlit >= 1.43.0 (dashboard UI)
- slowapi >= 0.1.9 (rate limiting)

### ML / CUDA
- torch == 2.10.0+cu128 (CUDA 12.8, Blackwell RTX 50 series)
- torchvision == 0.25.0+cu128
- torchaudio == 2.10.0+cu128
- numpy >= 1.26.4
- pandas >= 2.3.3
- scikit-learn >= 1.8.0
- xgboost >= 2.1.4

### Market Data
- databento >= 0.84.0 (primary live OPRA stream)
- polygon-api-client == 1.16.3 (legacy fallback)
- python-dotenv >= 1.0.0

### Database
- SQLAlchemy >= 2.0.0 (ORM)
- SQLite (runtime databases: market_data.db, forecast_research.db, shadow_research.db)

### Utilities
- aiohttp >= 3.13.3 (async HTTP)
- websockets >= 16.0 (WebSocket client)
- tenacity >= 9.1.2 (retry logic)
- pytz >= 2024.1 (timezone handling)
- zoneinfo (stdlib, America/New_York)
- scipy >= 1.10.0 (statistical utilities)
- plotly >= 5.0.0 (charting)
- pypdf >= 6.16.2 (PDF handling)

### Agent / MCP
- mcp == 2.2.0 (Model Context Protocol server for Codex/agent bridge)
- openai >= 2.0.0 (AI analyst via GPT)

### Testing
- pytest >= 7.4.4
- PYTHONPATH configured via `pyproject.toml` (`pythonpath = ["."]`)

## Development Environment

### Virtual Environment
```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements_backend.txt --extra-index-url https://download.pytorch.org/whl/cu128
```

### Run Commands
```powershell
# Backend API
.venv\Scripts\python -m uvicorn app.api.main:app --port 8001

# Streamlit UI
.venv\Scripts\python -m streamlit run app.py

# Tests
.venv\Scripts\python -m pytest

# Standalone forecast
.venv\Scripts\python databento_forecast.py
.venv\Scripts\python databento_forecast.py --symbols SPX SPY QQQ --horizon eod

# Train ETF model
.venv\Scripts\python train_etf_daily_model.py

# Start full stack (PowerShell)
.\start_databento_app.ps1
.\start_market_day.ps1
```

### Key Configuration (`.env`)
| Variable | Purpose |
|---|---|
| `DATABENTO_API_KEY` | Live OPRA stream authentication |
| `DATABENTO_SYMBOLS` | Comma-separated symbols (default: SPX,NDX,VIX) |
| `DATABENTO_REQUIRED_SYMBOLS` | Minimum required (default: SPX,NDX) |
| `DATABASE_URL` | SQLite path (default: sqlite:///./data/market_data.db) |
| `MARKETPIN_GOVERNANCE_ACCESS_TOKEN` | Governance API token (≥16 chars) |
| `OPENAI_API_KEY` | AI analyst (optional) |
| `FORCE_CPU` | Override CUDA detection |
| `DATABENTO_SUBSCRIPTION_PROFILE` | near-term-shadow or similar |
| `DATABENTO_PRIMARY_MAX_STRIKE_PAIRS` | Max strike pairs (default: 600) |

## Model Artifacts

| File | Description |
|---|---|
| `models/etf_price_predictor_best.pt` | Primary ETF price PyTorch model |
| `models/etf_price_predictor_meta.json` | Feature schema and model metadata |
| `models/etf_rich_predictor_best.pt` | Rich-feature variant |
| `models/eod_calibration.json` | EOD calibration parameters |
| `cuda_price_predictor_best.pt` | Root-level CUDA predictor (legacy) |

## Database Schema (SQLite)

### `data/market_data.db`
Primary runtime database. Tables include:
- Market structure observations (GEX, pin levels per symbol/date)
- ORB reference samples (5-second cadence, provenance-complete)
- ORB reference progress decisions (immutable sidecars)
- Prediction passports and authority records
- Monitor scan ledger and outbox tables

### `data/forecast_research.db`
Forecast research experiments and shadow formula validation results.

### `data/shadow_research.db`
Shadow formula comparison data.

## CI / Tooling
- GitHub Actions (`.github/workflows/`)
- `.codex/` — Codex agent environment configuration
- `.streamlit/config.toml` — Streamlit theme/server config
- `pyproject.toml` — uv package manager, pytest config
- `uv.lock` — Locked dependency manifest

## GPU Requirements
- NVIDIA GPU with CUDA 12.8 support (Blackwell RTX 50 series preferred)
- Falls back to CPU if CUDA unavailable (`FORCE_CPU=true` or no GPU)
- `inference_device()` in `backend/ai_predictor.py` handles detection
- PyTorch tensors use `torch.float64` for financial precision
