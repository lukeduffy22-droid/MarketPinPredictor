# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This project is a dual-model stock market prediction system for major stock indices (SPX, NDX, DJI, RUT). It combines a Time-Adaptive Ridge Regression model, optimized for the final trading hour, with traditional machine learning models for earlier predictions. Key features include adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, comprehensive technical analysis, and an AI-powered prediction enhancement layer. The system aims to provide accurate price predictions, with a focus on improving accuracy in the last 30 minutes of trading.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture

The frontend uses Streamlit (port 5000) as a pure API client, consuming only FastAPI endpoints. It features adaptive refresh rates and strictly separates the UI from prediction logic.

### Backend Architecture

The backend is built with FastAPI (port 8000) using an async/await pattern.

**Core Components**:
-   **Per-Symbol Ring Buffers**: Stores 90 minutes of 1-second index bars and options flow data for rapid access.
-   **WebSocket Ingestion**: Normalizes incoming messages, aggregates sub-second data into 1-second bars, and intelligently switches between real-time and delayed feeds.
-   **Feature Calculators**: Computes critical features such as VWAP Deviation, Microtrend, Gamma Pinning (Black-Scholes), and Flow Urgency. Includes a 150ms circuit breaker.
-   **AI Enhancement Layer**: Integrates a swappable AI provider architecture (e.g., OpenAI) to act as a "critic and corrector" on base model predictions. It analyzes live gamma, VWAP, microtrend, and options flow data to suggest confidence-weighted adjustments and provides natural language explanations.
-   **Multi-Expiry Gamma Analysis**: Analyzes gamma across 0-7 DTE expirations with time-weighted aggregation to identify unified gamma walls and an aggregate pin strike for enhanced EOD predictions.
-   **Advanced Gamma-Based EOD Prediction**: Incorporates Wall-Weighted Magnet (WWM), Pin Stability Index (PSI), Zero-Gamma Magnet, and Volatility-Adjusted Close Predictor (VACP) for highly accurate end-of-day predictions.
-   **OI Cache Service**: Refreshes Open Interest (OI) data in the background.
-   **Database Models**: SQLAlchemy models for `CalibrationCoeff`, `RMSEBucket`, and `PredictionLog`.
-   **FastAPI Endpoints**: Provides health checks, gamma exposure levels, and prediction endpoints.

### Prediction Model

The primary prediction model is Ridge Regression with time-adaptive weights, incorporating `vwap_dev`, `microtrend`, `gamma_pin`, and `flow_urgency`. Weights adjust based on minutes to market close.

### Guards and Validation

The system enforces prediction cadence, performs freshness checks on market data, requires minimum data availability, and includes a circuit breaker for feature computation.

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