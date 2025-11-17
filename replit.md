# Stock Index Price Predictor - Dual-Model Prediction System

## Overview

This project is a dual-model stock market prediction system designed for major stock indices (SPX, NDX, DJI, RUT). It integrates a Time-Adaptive Ridge Regression model, optimized for the final trading hour, with traditional machine learning models (Linear Regression/Random Forest) for earlier predictions. The system features adaptive model selection, gamma exposure analytics, real-time WebSocket streaming, and comprehensive technical analysis. Its primary goal is to provide accurate price predictions, particularly excelling in the last 30 minutes of trading with a 15%+ MAE improvement over a VWAP-only baseline.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture

The frontend uses Streamlit (port 5000) as a pure API client, consuming only FastAPI endpoints. It features adaptive refresh rates based on the `/metrics` API endpoint and strictly separates the UI from prediction logic.

### Backend Architecture

The backend is built with FastAPI (port 8000) using an async/await pattern.

**Core Components**:
-   **Per-Symbol Ring Buffers**: Stores 90 minutes of 1-second index bars and options flow data in lock-free deque implementations for rapid access.
-   **WebSocket Ingestion**: Normalizes incoming messages into `IndexTick` or `OptTrade`, aggregates sub-second data into 1-second bars, and intelligently switches between real-time and delayed feeds.
-   **Feature Calculators**: Computes critical features such as VWAP Deviation, Microtrend (Numpy polyfit slope), Gamma Pinning (Black-Scholes with fast Numpy PDF), and Flow Urgency. It includes a 150ms circuit breaker for API latency.
-   **OI Cache Service**: Refreshes Open Interest (OI) data in the background, ensuring endpoints use cached data.
-   **Database Models**: SQLAlchemy models for `CalibrationCoeff`, `RMSEBucket`, and `PredictionLog` to store model parameters, performance metrics, and historical predictions.
-   **FastAPI Endpoints**: Provides health checks, gamma exposure levels, and prediction endpoints (`/predict/close`).

### Prediction Model

The primary prediction model is Ridge Regression with time-adaptive weights, incorporating `vwap_dev`, `microtrend`, `gamma_pin`, and `flow_urgency`. Time-adaptive weights adjust the influence of `microtrend` and `flow_urgency` based on minutes to market close, increasing their impact closer to the final bell. The model has demonstrated significant MAE improvements and high directional accuracy in backtesting across all indices.

### Guards and Validation

The system enforces prediction cadence based on time-to-close, performs freshness checks on market data, and requires minimum data availability in ring buffers. A circuit breaker limits feature computation to 150ms. Performance monitoring tracks latency and memory usage.

### Time Utilities

Comprehensive Eastern Time management ensures accurate calculation of `minutes_to_close_et()`, handles market holidays, and identifies early close dates and the critical "power hour" (3:45-4:00 PM ET).

## External Dependencies

### Third-Party APIs

-   **Polygon.io REST & WebSocket APIs**: Used for real-time and historical stock market data for indices and options. It features smart feed switching between real-time during market hours and delayed after hours.

### Python Libraries

-   **Core**: `fastapi`, `uvicorn`, `streamlit`, `sqlalchemy`, `pydantic-settings`.
-   **ML/Data**: `scikit-learn` (Ridge regression), `numpy`, `pandas`, `scipy` (Black-Scholes).
-   **API/Streaming**: `polygon` (official client), `requests`.

### Database

-   **PostgreSQL**: Utilized for persistent storage of `calibration_coeffs`, `rmse_buckets`, and `prediction_logs`. Initialized via `backtest_calibrate.py`.