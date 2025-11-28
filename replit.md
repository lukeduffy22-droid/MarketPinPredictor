# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This project is a dual-model stock market prediction system for major stock indices (SPX, NDX, DJI, RUT). It combines a Time-Adaptive Ridge Regression model, optimized for the final trading hour, with traditional machine learning models for earlier predictions. Key features include adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, comprehensive technical analysis, 1-hour Opening Range Breakout (ORB) tracking, AI market event scanning, and an AI-powered prediction enhancement layer. The system aims to provide accurate price predictions, with a focus on improving accuracy in the last 30 minutes of trading.

## User Preferences

Preferred communication style: Simple, everyday language.

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