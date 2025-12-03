# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This project is a dual-model stock market prediction system for major stock indices (SPX, NDX, DJI, RUT). It combines a Time-Adaptive Ridge Regression model, optimized for the final trading hour, with traditional machine learning models for earlier predictions. Key features include adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, comprehensive technical analysis, 1-hour Opening Range Breakout (ORB) tracking, AI market event scanning, and an AI-powered prediction enhancement layer. The system aims to provide accurate price predictions, with a focus on improving accuracy in the last 30 minutes of trading.

## User Preferences

Preferred communication style: Simple, everyday language.

## Recent Changes (December 2025)

### Replit WebSocket Limitation & 1-Second REST Polling Fix (Dec 3, 2025)
- **Issue**: Replit environment blocks WebSocket connections to `socket.polygon.io` (DNS resolution fails with `gaierror: [Errno -2]`)
- **Root Cause**: Replit's network infrastructure doesn't allow long-lived WebSocket connections to certain external hosts
- **Solution**: Implemented aggressive 1-second REST API polling using premium Polygon subscription (unlimited API calls)
- **Implementation** (`app/ingest/rest_fallback.py`):
  - `poll_polygon_rest()` polls every 1 second for near-real-time data
  - Uses `client.get_snapshot_indices()` for batch fetching all 4 indices
  - Logs every 10 polls to reduce noise
- **Health Endpoint Updates**:
  - `data_age_seconds`: Shows how old the latest tick is (should be 0-1 seconds)
  - `buffer_length`: Number of seconds of data in ring buffer
  - `mode`: Shows "REST" or "WebSocket"
- **Freshness Threshold**: 5 seconds (compatible with 1-second polling)
- **Note**: This is a Replit-specific workaround. If deploying elsewhere with WebSocket support, the system will use real WebSocket streaming.

## Recent Changes (November 2025)

### 1-Hour Opening Range Breakout (ORB) Tracking
- **ORB Tracker Module** (`app/state/orb_tracker.py`): Captures high/low during 9:30-10:30 AM ET for each trading day
- **ORB Features in ML Model**: Position in range, breakout direction (bullish/bearish/inside), range width percentage
- **ML Integration**: ORB breakout signals now influence Time-Adaptive Ridge predictions
- **Confidence Boost**: When ORB direction aligns with other signals (VWAP, microtrend), confidence increases
- **API Endpoints**: `/orb/{symbol}` and `/orb` for accessing ORB data

### AI Market Event Scanner
- **Event Scanner Module** (`app/services/market_event_scanner.py`): Scans for macro/micro events affecting indices
- **Event Categories**: Fed announcements (FOMC, rate decisions), economic data (CPI, jobs, GDP), earnings, geopolitical, market structure (options expiration)
- **Calendar Integration**: Automatically detects monthly OPEX, weekly options expiration, NFP report days
- **AI Analysis**: Uses OpenAI to analyze news context and identify market-moving events
- **Risk Level Assessment**: Low/Normal/Elevated/High based on event count and impact
- **Sidebar Display**: Shows top 3 events with impact level and expected direction
- **AI Prompt Integration**: Events are passed to AI prediction enhancement for context-aware adjustments
- **API Endpoints**: `/market-events` and `/market-events/summary`

### Enhanced AI Prediction Analysis
- AI now receives ORB data (high/low, position in range, breakout status)
- AI now receives market events context for event-aware prediction adjustments
- Combined with historical accuracy data for comprehensive prediction critique

### GEX Calculation Fix (Based on External Feedback)
- **Fixed Total GEX vs Net GEX**: Now correctly calculates aggregate sums across ALL strikes
  - `total_gex` / `net_gex`: Pin-level values (backward compatible)
  - `aggregate_total_gex` / `aggregate_net_gex`: NEW fields with sum across all strikes (gross and net)
  - Multi-expiry gamma also updated with `expiry_total_gex` / `expiry_net_gex` fields

### Half-Day Session Fix (Nov 28, 2025)
- **Fixed Time Countdown Bug**: Previously showed wrong minutes remaining on early close days
- **Root Cause**: `ridge_predictor.get_minutes_to_close()` was hardcoded to 4 PM, now delegates to `time_et.minutes_to_close_et()`
- **Prediction Formula Fix**: `predict()` function now uses `close_time_et()` for correct close time
- **Half-Day Detection**: UI now prominently displays when market closes early (1 PM ET)
- **Close Time Display**: Shows actual close time in countdown (e.g., "Market closes in 2h 15m (01:00 PM ET)")
- **Holiday-Aware**: Uses `app/utils/time_et.py` for accurate holiday/early close detection

### Regime-Aware Predictions (Nov 28, 2025)
- **Regime Flags**: Model now detects and adjusts for special session types:
  - `regime_half_day`: Early close sessions (1 PM ET) - reduces VWAP influence, increases gamma weight
  - `regime_holiday_adjacent`: Day before/after holidays - reduces microtrend weight, increases flow weight
  - `regime_eom`: End of month (last 3 days) - allows more drift, reduces mean-reversion
  - `regime_eow`: End of week (Thu/Fri) - tracked for pattern analysis
  - `regime_vix`: VIX level placeholder for volatility regime
- **Gamma Snapshot Features**: Added distance_to_pin, net_gamma_level, aggregate GEX for better short-horizon predictions
- **Coefficient Adjustments**: Model coefficients now adapt based on regime flags

### MAE-by-Regime Error Tracking (Nov 28, 2025)
- **PredictionLog Schema**: Added regime flag columns for historical analysis
- **get_mae_by_regime()**: New function to compute MAE/bias by session type
- **API Endpoint**: `/prediction-accuracy/by-regime` returns MAE statistics by regime type
- **Bias Detection**: Helps identify systematic under/over-prediction in specific session types

### Enhanced CSV Export
- **New Columns**: IndexSymbol, SessionDate, Distance_Points, Distance_Pct, Pin_Drift_Per_Hour
- **Raw Values**: Export includes raw numeric values for analysis (not formatted strings)
- **Download Button**: Added CSV download button in gamma history table

### Adaptive Gamma Sampling
- **Intelligent Intervals**: Sampling frequency increases as market close approaches
  - Regular: 15-minute intervals
  - Last hour: 5-minute intervals
  - Last 30 minutes: 3-minute intervals
  - Last 15 minutes: 2-minute intervals
- **Early Close Aware**: Uses proper close time for half-day sessions

### Prediction Accuracy Improvements
- **Timeframe-Adaptive Bounds**: Predictions now use adaptive limits based on timeframe:
  - 1-day: ±3% maximum move
  - 5-day: ±5% maximum move  
  - 1-week: ±8% maximum move
- **VIX Volatility Override**: When VIX > 35 (crash/panic), allows 2x normal range; VIX > 25 allows 1.5x
- **Gamma Pin Validation**: Rejects gamma pins more than 15% from current spot price (prevents showing unrealistic values like $2800 for SPX at $6800)

### AI Enhancement Layer Improvements
- **Historical Accuracy Feedback**: AI now receives historical model accuracy data including:
  - Average accuracy percentage from past predictions
  - Prediction bias (bullish/bearish/neutral)
  - Consistency rating (highly_consistent/moderately_consistent/variable)
  - Last 3 predictions with predicted/actual/accuracy details
- This allows AI to calibrate adjustments based on past model performance

### Database Functions Added
- `get_historical_accuracy_for_ai()` - Fetches rich historical accuracy data for AI context
- `get_predictions_needing_actuals()` - Find predictions missing actual EOD prices
- `batch_update_prediction_actuals()` - Batch update predictions with actuals

## System Architecture

### Frontend Architecture

The frontend uses Streamlit (port 5000) as a pure API client, consuming only FastAPI endpoints. It features adaptive refresh rates and strictly separates the UI from prediction logic.

### Backend Architecture

The backend is built with FastAPI (port 8000) using an async/await pattern.

**Core Components**:
-   **Per-Symbol Ring Buffers**: Stores 90 minutes of 1-second index bars and options flow data for rapid access.
-   **WebSocket Ingestion**: Normalizes incoming messages, aggregates sub-second data into 1-second bars, and intelligently switches between real-time and delayed feeds.
-   **Feature Calculators**: Computes critical features such as VWAP Deviation, Microtrend, Gamma Pinning (Black-Scholes), and Flow Urgency. Includes a 150ms circuit breaker.
-   **AI Enhancement Layer**: Integrates a swappable AI provider architecture (e.g., OpenAI) to act as a "critic and corrector" on base model predictions. It analyzes live gamma, VWAP, microtrend, options flow data, AND historical accuracy feedback to suggest confidence-weighted adjustments and provides natural language explanations.
-   **Multi-Expiry Gamma Analysis**: Analyzes gamma across 0-7 DTE expirations with time-weighted aggregation to identify unified gamma walls and an aggregate pin strike for enhanced EOD predictions.
-   **Advanced Gamma-Based EOD Prediction**: Incorporates Wall-Weighted Magnet (WWM), Pin Stability Index (PSI), Zero-Gamma Magnet, and Volatility-Adjusted Close Predictor (VACP) for highly accurate end-of-day predictions.
-   **OI Cache Service**: Refreshes Open Interest (OI) data in the background.
-   **Database Models**: SQLAlchemy models for `CalibrationCoeff`, `RMSEBucket`, `PredictionLog`, and `Prediction` with accuracy tracking.
-   **FastAPI Endpoints**: Provides health checks, gamma exposure levels, and prediction endpoints.

### Prediction Model

The primary prediction model is Ridge Regression with time-adaptive weights, incorporating `vwap_dev`, `microtrend`, `gamma_pin`, and `flow_urgency`. Weights adjust based on minutes to market close.

### Guards and Validation

The system enforces prediction cadence, performs freshness checks on market data, requires minimum data availability, includes a circuit breaker for feature computation, validates gamma pins within ±15% of spot price, and applies timeframe-adaptive prediction bounds with VIX volatility overrides.

### Time Utilities

Comprehensive Eastern Time management for `minutes_to_close_et()`, market holidays, and early close dates.

## External Dependencies

### Third-Party APIs

-   **Polygon.io REST & WebSocket APIs**: For real-time and historical stock market data for indices and options. Features smart feed switching.
-   **OpenAI (via Replit AI Integrations)**: Used for the AI prediction enhancement layer.

### Python Libraries

-   **Core**: `fastapi`, `uvicorn`, `streamlit`, `sqlalchemy`, `pydantic-settings`.
-   **ML/Data**: `scikit-learn`, `numpy`, `pandas`, `scipy`.
-   **API/Streaming**: `polygon`, `requests`.
-   **Time Management**: `pytz`.

### Database

-   **PostgreSQL**: For persistent storage of model parameters, performance metrics, and historical predictions.