# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This project is a dual-model stock market prediction system for major stock indices (SPX, NDX, DJI, RUT). It combines a Time-Adaptive Ridge Regression model, optimized for the final trading hour, with traditional machine learning models for earlier predictions. The system features adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, comprehensive technical analysis, 1-hour Opening Range Breakout (ORB) tracking, and an AI-powered prediction enhancement layer. Its primary goal is to provide accurate price predictions, with a particular focus on improving accuracy in the last 30 minutes of trading, and leveraging AI for confidence-weighted adjustments based on live market context.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture

The frontend uses Streamlit as a pure API client, consuming only FastAPI endpoints. It features adaptive refresh rates and strictly separates the UI from prediction logic.

### Backend Architecture

The backend is built with FastAPI using an async/await pattern.

**Core Components**:
-   **Per-Symbol Ring Buffers**: Stores 90 minutes of 1-second index bars and options flow data.
-   **WebSocket Ingestion**: Normalizes incoming messages, aggregates data into 1-second bars, intelligently switches between real-time and delayed feeds, and includes a market-close data freeze system for audit compliance. WebSocket connections are managed via a singleton pattern to prevent duplicate connections.
-   **Feature Calculators**: Computes critical features such as VWAP Deviation, Microtrend, Gamma Pinning (Black-Scholes), and Flow Urgency, with a 150ms circuit breaker.
-   **AI Enhancement Layer**: Integrates a swappable AI provider architecture to act as a "critic and corrector" on base model predictions. It analyzes live gamma, VWAP, microtrend, options flow data, historical accuracy feedback, ORB data, and market event context to suggest confidence-weighted adjustments and provides natural language explanations.
-   **Multi-Expiry Gamma Analysis**: Analyzes gamma across 0-7 DTE expirations with time-weighted aggregation to identify unified gamma walls and an aggregate pin strike for enhanced EOD predictions. This includes canonical GEX definitions, an audit observability layer for traceability, and diagnostic metrics for system health (e.g., skew, volatility regime, truncation, confidence, dispersion). It also tracks pin drift per hour for intraday signal.
-   **Advanced Gamma-Based EOD Prediction**: Incorporates Wall-Weighted Magnet (WWM), Pin Stability Index (PSI), Zero-Gamma Magnet, and Volatility-Adjusted Close Predictor (VACP) for highly accurate end-of-day predictions.
-   **Max Pain Calculation**: Independent calculation of max pain strike (where option writers have minimum payout). Uses only strike prices and open interest - no Greeks involved. Displayed alongside gamma pin in UI for comprehensive options-based price targets.
-   **OI Cache Service**: Refreshes Open Interest (OI) data in the background.
-   **Database Models**: SQLAlchemy models for `CalibrationCoeff`, `RMSEBucket`, `PredictionLog`, and `Prediction` with accuracy tracking, including regime-aware error tracking.
-   **FastAPI Endpoints**: Provides health checks, gamma exposure levels, prediction endpoints, market event summaries, ORB data, and accuracy statistics.
-   **Unified Canonical Gamma Pipeline**: A single source of truth for snapshot generation (`build_audit_snapshot()`) ensuring consistent GEX calculations, including pre-gate diagnostic fields explaining sanity check failures.
-   **Historical Gamma Validation System**: Allows building and validating historical gamma pins against actual closes for model efficacy.
-   **NDJSON Snapshot Export**: Append-only export of audit snapshots to `exports/{symbol}/{YYYY-MM-DD}.ndjson` for full-day observability. Controlled by `EXPORT_SNAPSHOTS` feature flag. Exports occur immediately after persist, even for invalid snapshots.

### Prediction Model

The primary prediction model is Ridge Regression with time-adaptive weights, incorporating `vwap_dev`, `microtrend`, `gamma_pin`, and `flow_urgency`. Weights adjust based on minutes to market close, with regime-aware adjustments for special session types. Prediction accuracy improvements include timeframe-adaptive bounds and VIX volatility overrides.

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