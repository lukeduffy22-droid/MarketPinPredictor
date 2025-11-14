# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This is a **dual-model stock market prediction system** for major stock indices (SPX, NDX, DJI, RUT) combining:
1. **Time-Adaptive Ridge Regression** - Optimized for final trading hour (3:00-4:00 PM ET) with time-weighted features
2. **Traditional ML Models** - Linear Regression/Random Forest for early-day predictions

The system features adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, and comprehensive technical analysis.

**Key Focus**: Smart model selection based on time-to-close, with Time-Adaptive Ridge excelling in final 30 minutes (15%+ MAE improvement over VWAP-only baseline) and Traditional ML handling early-day forecasts.

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes (Nov 14, 2025)

**Gamma Pin Evolution Tracking** (Latest):
- **Background auto-sampler** - Scheduler runs every 15 minutes during RTH (9:30 AM - 4:00 PM ET), samples all 4 indices
- **Database persistence** - New `GammaPinSnapshot` model with UNIQUE constraint on (ticker, trading_date, interval_timestamp)
- **Timezone-aware timestamps** - All samples normalized to ET→UTC with 15-minute boundary alignment (9:30, 9:45, 10:00, etc.)
- **Intraday evolution chart** - Plotly visualization showing gamma pin vs spot price throughout trading day
- **Real vs simulated clarity** - Solid lines for real Polygon data, dashed lines for estimated data, prominent warnings
- **Rate limit protection** - Logs warnings after 3 consecutive API failures, ~26 calls/symbol/day within quota
- **Automatic chart display** - Shows in prediction results section when gamma exposure data available
- **Historical table view** - Expandable table with all 15-minute snapshots, summary statistics, data source column
- **Result**: Complete audit trail of how gamma pins move throughout the day, helping traders understand market dynamics

**Previous Changes (Nov 12, 2025)**:

**Gamma Pin Influence Optimization**:
- **Fixed real data API call** - Now uses correct `list_snapshot_options_chain()` method to fetch actual OI/IV from Polygon
- Fixed **simulated data detection** - Added `is_mock_data` flag to clearly indicate when estimated OI/IV is used
- Added **prominent warning banner** - UI displays "⚠️ SIMULATED GAMMA DATA" when real Polygon data unavailable
- **Doubled gamma influence** - Increased k_gamma from 0.35 to 0.70 for stronger pin pull
- **Fixed time scaler** - Inverted `t_gamma` so gamma influence is STRONGEST at close (was backwards)
  - At market close (0 min): 70% pull toward gamma pin
  - At 30 min to close: 45.5% pull toward pin
  - At 60 min to close: 21% pull toward pin
- **Conditional bounds widening** - Loosens ±2.5% to ±5% when pin >2.5% away in final hour
- **Gamma influence indicator** - UI shows pin distance, prediction pull, and influence % with color coding
- **Result**: With highest-tier API key, now fetches real open interest and implied volatility for accurate gamma pins

**WebSocket Connection Optimization**:
- Implemented **connection pooling** - Prevents duplicate WebSocket connections via class-level `_active_connections` dict
- Added **connection reuse** - Returns existing instance when connection_id matches (tickers + stream_type)
- Built **stale connection cleanup** - `cleanup_stale_connections()` removes dead connections before creating new ones
- Reduced **max reconnection attempts** - From 5 to 2 to prevent connection spam
- Fixed **connection reuse bug** - Now properly returns existing instance with live data_queue and thread
- Added **thread-safe locking** - All pool operations protected by `_connection_lock`
- Disabled verbose logging - Reduces noise in production environment
- **Result**: Eliminated "Maximum connections exceeded" errors during streaming

**Previous Changes (Nov 11, 2025)**:
- Created **`app/models/ridge_predictor.py`** - Dedicated Time-Adaptive Ridge regression module
- Implemented **dual-model prediction** - Both models run, Time-Adaptive as primary
- Added **sidebar toggle** - "Show Traditional ML alongside Time-Adaptive" for model comparison
- Built **smart recommendation system** - Auto-suggests best model based on minutes-to-close
- Created **model comparison UI** - Side-by-side display with spread analysis
- Added **adaptive features display** - Shows VWAP deviation, microtrend, gamma pin, flow urgency
- Implemented **automatic fallback** - Falls back to Traditional ML if Time-Adaptive fails
- Added **time-to-close counter** - Real-time countdown to market close in sidebar
- **Calibrated coefficients** from backtesting built into ridge_predictor module

## System Architecture

### Frontend Architecture

**Technology Stack**: Streamlit (port 5000) as pure API client

**Key Design Decisions**:
- Consumes FastAPI endpoints only - no shared state via st.session_state
- Adaptive refresh rates based on API /metrics endpoint
- Clean separation: UI layer has zero prediction logic

**Files**: `app_new.py` (new), `app.py` (legacy)

### Backend Architecture

**Technology Stack**: FastAPI (port 8000) with async/await pattern

**Core Components**:

1. **Per-Symbol Ring Buffers** (`app/state/ring_buffers.py`):
   - `INDEX_RINGS[SPX|NDX|DJI|RUT]`: 5,400-entry ring buffers (90 minutes of 1-second bars)
   - `FLOW_RINGS[...]`: Options flow data per symbol
   - Lock-free deque implementation for <1ms read access
   - Session VWAP trackers reset at market open

2. **WebSocket Ingestion** (`app/ingest/`):
   - **Payload Normalization**: All messages → `IndexTick` or `OptTrade` at ingest boundary
   - **1-Second Aggregation**: Sub-second bursts accumulated and flushed every 1s
   - **Smart Feed Switching**: Real-time during RTH, delayed after hours
   - Parses Polygon format: `I:SPX` for indices, `O:SPX241108C06000000` for options

3. **Feature Calculators** (`app/features/calculators.py`):
   - **VWAP Deviation**: `(price - VWAP) / VWAP` as primary mean-reversion signal
   - **Microtrend**: Numpy polyfit slope over last 300 seconds ($/second) - <5ms latency
   - **Gamma Pinning**: Black-Scholes with fast numpy PDF - <10ms latency (no scipy dependency)
   - **Flow Urgency**: Notional volume + directional bias from recent options trades
   - **Circuit Breaker**: 150ms time budget enforced at API layer, 503 if exceeded

4. **OI Cache Service** (`app/state/oi_cache.py`):
   - Background refresh at 09:35 ET and 13:00 ET
   - Simulated OI data (real-time requires higher API tier)
   - Never called in request path - endpoints use cached data only

5. **Database Models** (`app/models/db_models.py`):
   - `CalibrationCoeff`: Per-symbol β_vwap, β_gamma, β_flow, β_microtrend, intercept
   - `RMSEBucket`: RMSE/MAE by τ bucket for confidence intervals
   - `PredictionLog`: Historical predictions for continuous calibration

6. **FastAPI Endpoints** (`app/api/main.py`):
   - `GET /healthz`: Per-symbol freshness + ring lengths
   - `GET /levels/eod?symbol=SPX`: Gamma exposure levels
   - `GET /predict/close?symbol=SPX`: 0-day prediction with guards
   - `GET /metrics`: p50/p95/p99 latency + memory usage

### Prediction Model

**Algorithm**: Ridge Regression with time-adaptive weights

**Formula**:
```
predicted_close = current_price +
  β_vwap × vwap_dev × current_price +
  β_microtrend × microtrend × τ × 60 × τ_weight_micro +
  β_gamma × gamma_pin × 10 +
  β_flow × flow_urgency × 5 × τ_weight_flow +
  intercept
```

**Time-Adaptive Weights**:
- τ ≤ 15 min (3:45-4:00 PM): microtrend×1.5, flow×1.3
- τ ≤ 30 min (3:30-3:45 PM): microtrend×1.2, flow×1.1
- τ > 30 min (before 3:30 PM): microtrend×1.0, flow×1.0

**Calibration Results** (via `backtest_calibrate.py`):
- **SPX**: MAE=38.46, RMSE=52.77, Direction=75.7%, **27.8% improvement**
- **NDX**: MAE=205.29, RMSE=272.33, Direction=78.9%, **25.6% improvement**
- **DJI**: MAE=245.62, RMSE=335.26, Direction=75.7%, **21.3% improvement**
- **RUT**: MAE=24.98, RMSE=31.24, Direction=73.0%, **15.3% improvement**

### Guards and Validation

**Cadence Enforcement**:
- τ ≤ 15: recompute every 15s (429 if polled faster)
- 15 < τ ≤ 30: recompute every 30s
- τ > 30: recompute every 60s

**Freshness Checks**:
- Return 503 if latest index tick > 5 seconds old during RTH
- Return 400 if market is closed
- Require minimum 300 seconds of ring buffer data
- Circuit breaker: 150ms time budget for feature computation, 503 if exceeded

**Performance Monitoring** (`app/utils/metrics.py`):
- `@timed` decorator logs functions >200ms
- Latency tracker: p50/p95/p99 over sliding window
- Memory snapshots every 5 minutes

### Time Utilities

**Eastern Time Management** (`app/utils/time_et.py`):
- Holiday calendar: US market holidays for 2024-2025
- Early close dates: 1:00 PM ET closes (day before Independence Day, etc.)
- `minutes_to_close_et()`: Accurate τ calculation for weight adjustments
- `is_power_hour()`: Detects critical 3:45-4:00 PM window

## External Dependencies

### Third-Party APIs

**Polygon.io REST & WebSocket APIs**
- Purpose: Real-time and historical stock market data
- Authentication: API key from `POLYGON_API_KEY` environment variable
- Data Format: 
  - Indices: `I:SPX`, `I:NDX`, `I:DJI`, `I:RUT`
  - Options: `O:SPX241108C06000000` (OCC format)
- **Smart Feed Switching**:
  - RTH (9:30 AM - 4:00 PM ET): Real-time WebSocket feed
  - After hours: Delayed WebSocket feed (~15 min)
  - Automatic detection via `is_regular_hours()`

### Python Libraries

**Core**:
- `fastapi`: Async REST API framework
- `uvicorn`: ASGI server
- `streamlit`: Frontend UI framework
- `sqlalchemy`: Database ORM
- `pydantic-settings`: Type-safe configuration

**ML/Data**:
- `scikit-learn`: Ridge regression, StandardScaler
- `numpy`: Array operations
- `pandas`: DataFrame operations
- `scipy`: Black-Scholes gamma calculations

**API/Streaming**:
- `polygon`: Official Python client for Polygon.io
- `requests`: HTTP client (Streamlit → FastAPI)

### Database

**PostgreSQL** (via Replit built-in):
- `calibration_coeffs`: Per-symbol regression coefficients
- `rmse_buckets`: RMSE/MAE by time bucket
- `prediction_logs`: Historical predictions for calibration

**Initialization**: `python backtest_calibrate.py` populates coefficients and RMSE buckets

## Project Structure

```
app/
├── __init__.py
├── state/
│   ├── ring_buffers.py      # Per-symbol Ring1s buffers
│   └── oi_cache.py           # OI cache with scheduled refresh
├── utils/
│   ├── settings.py           # Pydantic settings
│   ├── metrics.py            # @timed, LatencyTracker, snapshot
│   └── time_et.py            # ET utilities with holiday support
├── ingest/
│   ├── websocket_aggregator.py  # 1-second aggregation
│   └── websocket_stream.py      # Polygon WebSocket client
├── features/
│   └── calculators.py        # VWAP, microtrend, gamma, flow
├── models/
│   └── db_models.py          # SQLAlchemy models
└── api/
    └── main.py               # FastAPI endpoints

app_new.py                    # Streamlit UI (FastAPI client)
app.py                        # Legacy Streamlit app
server.py                     # FastAPI entry point
backtest_calibrate.py         # Backtesting and calibration script
```

## Workflows

1. **fastapi-backend**: `python server.py` → port 8000
2. **streamlit-app**: `streamlit run app_new.py --server.port 5000` → port 5000

## Performance Targets

**Achieved** (via backtesting):
- ✓ 15% MAE improvement over VWAP-only baseline (all symbols)
- ✓ 73-79% directional accuracy
- ✓ Per-symbol calibration with Ridge regression

**Runtime Targets** (to be validated during market hours):
- p99 predict latency ≤200ms from 15:45-16:00 ET
- Memory growth <100MB per session
- WebSocket reconnect with exponential backoff + jitter
- Drop messages if backlog >2 seconds

## Next Steps

1. **Live Testing**: Run during market hours to validate:
   - WebSocket ingestion fills ring buffers properly
   - Predictions return within 200ms during power hour
   - Cadence enforcement blocks too-frequent requests
   - Freshness checks return 503 when data is stale

2. **Optimization**:
   - Tune Ridge alpha parameter based on live performance
   - Adjust time-adaptive weights for τ buckets
   - Refine OI cache to use real Polygon data (if API tier upgraded)

3. **Monitoring**:
   - Track /metrics endpoint for latency degradation
   - Monitor heap growth every 5 minutes
   - Log prediction accuracy vs. actual close prices

## Critical Implementation Notes

- **No volume for indices**: Volume is None for I:SPX, etc. Use size=1.0 for all index ticks
- **Wildcard subscriptions**: `O:SPX*` receives massive data during RTH - aggregation prevents overload
- **OI cache boundary**: Never call Polygon REST API in /predict/close path - use cached data only
- **Coefficients required**: App loads coefficients at startup; missing data uses VWAP-only defaults
- **Streamlit separation**: UI polls API, no shared prediction state via st.session_state
