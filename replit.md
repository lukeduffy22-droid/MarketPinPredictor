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
- **Key Functions**:
  - `build_today_snapshot(symbol)`: Build gamma snapshot for TODAY using live OI data
  - `build_historical_snapshot(symbol, date)`: Load stored snapshot or build for today
  - `validate_historical_snapshots(symbol, days)`: Compare stored pins to actual closes
- **New API Endpoints**:
  - `POST /historical/build-today/{symbol}`: Build today's snapshot (uses live OI data)
  - `POST /historical/build/{symbol}?date=YYYY-MM-DD`: Build/load historical snapshot
  - `POST /historical/batch/{symbol}?days=30`: Build batch of historical snapshots
  - `GET /historical/validate/{symbol}?days=30`: Get validation metrics
  - `GET /historical/snapshot/{symbol}/{date}`: Get saved historical snapshot
- **Validation Metrics**: MAE (points/%), bias, direction accuracy, error distribution
- **Historical Snapshots**: Stored at `./logs/historical/{symbol}/{YYYY-MM-DD}.json`
- **IMPORTANT**: Historical OI data is NOT available from Polygon standard endpoints. Snapshots must be pre-built daily using `/historical/build-today/{symbol}` to build a validation dataset.

### Market-Close Data Freeze System (Dec 17, 2025)
- **Purpose**: Ensure data integrity for audit compliance by preventing live data ingestion after market close
- **New Modules**:
  - `app/utils/market_time.py`: Market time utilities with `now_et()`, `market_is_open()`, `market_is_closed()`, `is_freeze_enforced()`, `get_freeze_status()`
  - `app/core/accuracy_ledger.py`: CSV-based accuracy tracking with `record_accuracy()`, `get_accuracy_stats()`
- **Freeze Enforcement**: All feeds (WebSocket, Options WebSocket, REST) check freeze status continuously
- **New API Endpoints**: `/accuracy/freeze-status`, `/accuracy/record/{symbol}`, `/accuracy/stats/{symbol}`, `/accuracy/ledger`
- **Accuracy Ledger**: Stored at `logs/accuracy_ledger.csv`

### Diagnostic Metrics Enhancement (Dec 18, 2025)
- **Purpose**: Add 6 observational-only diagnostic metrics to audit snapshots for all symbols (SPX, NDX, DJI, RUT)
- **CRITICAL RULE**: These metrics are DIAGNOSTIC ONLY - do NOT use to adjust gamma math or tune parameters
- **New Diagnostic Fields in AuditSnapshot**:
  1. **skew_metrics**: ATM call vs put IV spread (`call_iv_mean`, `put_iv_mean`, `skew`)
  2. **gamma_by_distance**: Gamma contribution per strike-distance bucket (ATM, NEAR, MID, FAR as percentages)
  3. **vol_regime**: Volatility regime classification (LOW < 15%, MEDIUM 15-25%, HIGH > 25%)
  4. **truncation**: Truncation bias metrics (`contracts_used`, `contracts_available`, `excluded_above`, `excluded_below`, `truncation_pct`)
  5. **confidence**: Self-diagnostic confidence score (0.0-1.0) with `confidence_factors` explaining deductions
  6. **dispersion_ratio**: Structural noise metric (FAR/ATM gamma ratio) - higher indicates noisier predictions
- **Implementation Files**:
  - `app/core/audit_snapshot.py`: Added diagnostic functions and fields to AuditSnapshot
  - `tools/historical_gamma.py`: Added diagnostic metrics to HistoricalGammaSnapshot
- **Hard Rules Enforced**:
  1. Do NOT invent missing historical OI - system refuses to build snapshots for past dates without pre-built data
  2. Do NOT imply this recreates dealer positioning - all disclaimers state "Dealer net sign unknown, tracking magnitude only"

### GEX Calculation Bug Fix & Pin Drift Implementation (Dec 18, 2025)
- **Purpose**: Fix two critical bugs in the audit snapshot system
- **Bug Fix A - Total_GEX == Net_GEX**:
  - **Problem**: Both fields were identical because we weren't separating call and put gamma before aggregation
  - **Solution**: Track `call_gex_total` and `put_gex_total` separately, then compute:
    - `gross_gex = sum(call_gex) + sum(put_gex)` (total gamma magnitude, always positive)
    - `net_gex = sum(call_gex) - sum(put_gex)` (signed directional exposure)
  - **New Fields in AuditSnapshot**:
    - `call_gex_total`: Aggregate call gamma exposure
    - `put_gex_total`: Aggregate put gamma exposure
    - `gross_gex`: Total gamma magnitude (replaces confusing `total_gex_abs`)
    - `net_gex`: Signed net exposure (replaces confusing `total_gex_net`)
  - **Backward Compatibility**: Legacy `total_gex_abs` and `total_gex_net` still available as aliases
- **Bug Fix B - Pin_Drift_Per_Hour = 0.0**:
  - **Problem**: Pin drift was not being calculated despite pin clearly moving intraday
  - **Solution**: Load previous snapshot and compute drift using timestamps:
    - `pin_change_points = pin_now - pin_prev` (simple point difference)
    - `pin_drift_points_per_hour = (pin_now - pin_prev) / hours_elapsed` (rate of migration)
  - **New Fields in AuditSnapshot**:
    - `pin_drift_points_per_hour`: Rate of pin migration (most important intraday signal)
    - `pin_change_points`: Simple difference from previous snapshot
    - `prev_pin_strike`: Previous snapshot's pin strike
    - `prev_snapshot_timestamp`: Previous snapshot's timestamp
  - **New Functions in historical_gamma.py**:
    - `load_last_snapshot_for_day(symbol, date)`: Load most recent intraday snapshot
    - `save_intraday_snapshot(snapshot)`: Save with timestamp in filename (YYYY-MM-DD_HHMMSS.json)
- **UI Updates (Frozen Gamma Tab)**:
  - Now displays "Gross GEX" and "Net GEX" with correct semantics
  - Shows separate "Call GEX" and "Put GEX" fields
  - Displays "Pin Drift" (pts/hr) or "Pin Change" (pts) when available
- **Modified Files**:
  - `app/core/gex.py`: Updated `AggregateGexResult` and `compute_aggregate_gex()` with call/put separation
  - `app/core/audit_snapshot.py`: Added new GEX fields and pin drift tracking
  - `tools/historical_gamma.py`: Updated snapshot generation with pin drift calculation
  - `app.py`: Updated Frozen Gamma tab UI to display corrected metrics