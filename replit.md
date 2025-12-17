# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This project is a dual-model stock market prediction system for major stock indices (SPX, NDX, DJI, RUT). It combines a Time-Adaptive Ridge Regression model, optimized for the final trading hour, with traditional machine learning models for earlier predictions. The system features adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, comprehensive technical analysis, 1-hour Opening Range Breakout (ORB) tracking, AI market event scanning, and an AI-powered prediction enhancement layer. Its primary goal is to provide accurate price predictions, with a particular focus on improving accuracy in the last 30 minutes of trading.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture

The frontend uses Streamlit as a pure API client, consuming only FastAPI endpoints. It features adaptive refresh rates and strictly separates the UI from prediction logic.

### Backend Architecture

The backend is built with FastAPI using an async/await pattern.

**Core Components**:
-   **Per-Symbol Ring Buffers**: Stores 90 minutes of 1-second index bars and options flow data.
-   **WebSocket Ingestion**: Normalizes incoming messages, aggregates data into 1-second bars, and intelligently switches between real-time and delayed feeds. Includes a market-close data freeze system for audit compliance.
-   **Feature Calculators**: Computes critical features such as VWAP Deviation, Microtrend, Gamma Pinning (Black-Scholes), and Flow Urgency, with a 150ms circuit breaker.
-   **AI Enhancement Layer**: Integrates a swappable AI provider architecture to act as a "critic and corrector" on base model predictions. It analyzes live gamma, VWAP, microtrend, options flow data, historical accuracy feedback, ORB data, and market event context to suggest confidence-weighted adjustments and provides natural language explanations.
-   **Multi-Expiry Gamma Analysis**: Analyzes gamma across 0-7 DTE expirations with time-weighted aggregation to identify unified gamma walls and an aggregate pin strike for enhanced EOD predictions. Includes canonical GEX definitions and an audit observability layer for traceability.
-   **Advanced Gamma-Based EOD Prediction**: Incorporates Wall-Weighted Magnet (WWM), Pin Stability Index (PSI), Zero-Gamma Magnet, and Volatility-Adjusted Close Predictor (VACP) for highly accurate end-of-day predictions.
-   **OI Cache Service**: Refreshes Open Interest (OI) data in the background.
-   **Database Models**: SQLAlchemy models for `CalibrationCoeff`, `RMSEBucket`, `PredictionLog`, and `Prediction` with accuracy tracking, including regime-aware error tracking.
-   **FastAPI Endpoints**: Provides health checks, gamma exposure levels, prediction endpoints, market event summaries, ORB data, and accuracy statistics.

### Prediction Model

The primary prediction model is Ridge Regression with time-adaptive weights, incorporating `vwap_dev`, `microtrend`, `gamma_pin`, and `flow_urgency`. Weights adjust based on minutes to market close, with regime-aware adjustments for special session types (e.g., half-day, holiday-adjacent, end-of-month). Prediction accuracy improvements include timeframe-adaptive bounds and VIX volatility overrides.

### Guards and Validation

The system enforces prediction cadence, performs freshness checks on market data, requires minimum data availability, includes a circuit breaker for feature computation, validates gamma pins within ±15% of spot price, applies timeframe-adaptive prediction bounds with VIX volatility overrides, and includes a market-close data freeze.

### Time Utilities

Comprehensive Eastern Time management for `minutes_to_close_et()`, market holidays, and early close dates. Adaptive gamma sampling frequency increases as market close approaches.

## External Dependencies

### Third-Party APIs

-   **Polygon.io REST & WebSocket APIs**: For real-time and historical stock market data for indices and options.
-   **OpenAI (via Replit AI Integrations)**: Used for the AI prediction enhancement layer.

### Python Libraries

-   **Core**: `fastapi`, `uvicorn`, `streamlit`, `sqlalchemy`, `pydantic-settings`.
-   **ML/Data**: `scikit-learn`, `numpy`, `pandas`, `scipy`.
-   **API/Streaming**: `polygon`, `requests`, `websockets`.
-   **Time Management**: `pytz`.

### Database

-   **PostgreSQL**: For persistent storage of model parameters, performance metrics, and historical predictions.

## Recent Changes (December 2025)

### Historical Gamma Validation System (Dec 17, 2025)
- **Purpose**: Validate measurement system by comparing historical gamma pins against actual closes
- **New Modules**:
  - `tools/historical_gamma.py`: Historical gamma builder using canonical GEX functions and Polygon historical data
  - `tools/historical_validation.py`: Validation pipeline with error metrics (MAE, bias, direction accuracy)
- **New API Endpoints**:
  - `POST /historical/build/{symbol}?date=YYYY-MM-DD`: Build single historical snapshot
  - `POST /historical/batch/{symbol}?days=30`: Build batch of historical snapshots
  - `GET /historical/validate/{symbol}?days=30`: Get validation metrics
  - `GET /historical/snapshot/{symbol}/{date}`: Get saved historical snapshot
- **Validation Metrics**: MAE (points/%), bias, direction accuracy, error distribution
- **Historical Snapshots**: Stored at `./logs/historical/{symbol}/{YYYY-MM-DD}.json`

### Market-Close Data Freeze System (Dec 17, 2025)
- **Purpose**: Ensure data integrity for audit compliance by preventing live data ingestion after market close
- **New Modules**:
  - `app/utils/market_time.py`: Market time utilities with `now_et()`, `market_is_open()`, `market_is_closed()`, `is_freeze_enforced()`, `get_freeze_status()`
  - `app/core/accuracy_ledger.py`: CSV-based accuracy tracking with `record_accuracy()`, `get_accuracy_stats()`
- **Freeze Enforcement**: All feeds (WebSocket, Options WebSocket, REST) check freeze status continuously
- **New API Endpoints**: `/accuracy/freeze-status`, `/accuracy/record/{symbol}`, `/accuracy/stats/{symbol}`, `/accuracy/ledger`
- **Accuracy Ledger**: Stored at `logs/accuracy_ledger.csv`