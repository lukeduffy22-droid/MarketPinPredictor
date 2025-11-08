# Stock Index Price Predictor

## Overview

This is an advanced Streamlit-based web application that predicts stock index prices using sophisticated technical analysis, options analytics, and machine learning. The application fetches real-time market data via the Polygon.io API (with WebSocket streaming support) and uses multiple ML models with advanced technical indicators to forecast price movements for major stock market indexes including S&P 500, Dow Jones, NASDAQ 100, and Russell 2000.

**Key Focus**: Optimized for the critical 15-minutes-before-close window (3:45-4:00 PM ET) when predictions are most valuable for end-of-day positioning.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture

**Technology Stack**: Streamlit framework for interactive web interface

**Key Design Decisions**:
- Single-page application (SPA) pattern with Streamlit's reactive model
- Wide layout configuration for optimal data visualization
- Session state management for API credentials and prediction caching
- Real-time data visualization using Plotly for interactive charts

**Rationale**: Streamlit provides rapid development for data-focused applications with built-in reactivity, eliminating the need for separate frontend/backend communication layers. The framework's session state enables persistence of user inputs and computed predictions across reruns.

### Backend Architecture

**Technology Stack**: Python-based data processing pipeline

**Core Components**:
1. **Data Acquisition**: Polygon REST client for fetching historical market data and options chains
   - Uses modern 2024 API: `list_aggs()` for price data, `list_options_contracts()` for options
   - All index data uses I:SPX format (not ETF proxies)
2. **Feature Engineering**: Technical indicator calculation module (SMA, EMA, RSI, MACD, VWAP, AMA)
3. **Prediction Engine**: scikit-learn models (Linear Regression, Random Forest) with standardized features
4. **Gamma Exposure Analysis**: Options gamma calculation using Black-Scholes for strike price pinning
   - Uses estimated OI/IV values (real-time requires higher API tier)
   - Gracefully falls back to simulated data if API unavailable
5. **Backtesting System**: Historical validation using Polygon data to measure prediction accuracy
6. **Visualization**: Plotly for interactive time-series charting and gamma exposure displays

**Design Pattern**: Pipeline architecture where data flows from API → feature engineering → prediction → visualization

**Rationale**: Linear pipeline simplifies data flow and makes the prediction process transparent. Technical indicators (moving averages, RSI) serve as features because they capture market momentum and trend patterns that inform price predictions. Gamma exposure analysis identifies key price levels where market makers hedge options, creating "pinning" effects.

### Data Processing

**Feature Engineering Strategy**:
- Multiple timeframe moving averages (5, 10, 20-day SMA)
- Exponential moving averages for trend sensitivity (5, 10-day EMA)
- RSI (14-day) for momentum and overbought/oversold conditions

**Prediction Model**:
- Algorithm: Linear Regression
- Preprocessing: StandardScaler for feature normalization
- Rationale: Linear models provide interpretable results and fast training for real-time predictions. StandardScaler ensures features with different scales (price vs. RSI) contribute equally to predictions.

**Alternatives Considered**:
- Time series models (ARIMA, Prophet): More complex, potentially better for pure forecasting but less flexible for multi-feature integration
- Deep learning (LSTM): Overkill for this use case; requires more data and computational resources

### State Management

**Session State Variables**:
- `api_key`: Stores user's Polygon API credentials securely within session
- `predictions`: Caches computed predictions to avoid redundant API calls

**Rationale**: Streamlit's session state prevents re-fetching data on every interaction, improving performance and reducing API quota consumption.

## External Dependencies

### Third-Party APIs

**Polygon.io REST API**
- Purpose: Real-time and historical stock market data
- Authentication: API key-based (stored securely in Replit Secrets)
- Data Retrieved: OHLCV (Open, High, Low, Close, Volume) data for **actual indices** using Polygon ticker format (I:SPX, I:NDX, I:DJI, I:RUT)
- **Important**: App now fetches actual index data (SPX ~$6,700) instead of ETF proxies (SPY ~$670)
- Rate Limiting: Handled through caching in session state

### Python Libraries

**Data Processing**:
- `pandas`: DataFrame operations and time-series manipulation
- `numpy`: Numerical computations

**Machine Learning**:
- `scikit-learn`: Linear regression models and data preprocessing (StandardScaler)

**Visualization**:
- `plotly`: Interactive charting with subplots support for technical analysis

**Web Framework**:
- `streamlit`: Application framework and UI components

**API Client**:
- `polygon`: Official Python client for Polygon.io API

### Backtesting System

**Purpose**: Validate prediction accuracy using historical data from Polygon API

**Implementation** (`backtesting.py`):
1. **Historical Data Fetching**: Retrieves past market data for specified date ranges
2. **Date Alignment**: Uses T-1 data to predict T, comparing against actual T EOD prices
3. **Metrics Calculation**: 
   - Direction accuracy (% of correct up/down predictions)
   - Mean absolute error percentage
   - Root mean squared error (RMSE)
   - Best/worst prediction analysis
4. **Model Optimization**: Analyzes accuracy by confidence levels to identify improvement opportunities

**Key Features**:
- Works with real Polygon data for **actual indices** (I:SPX format) and options
- Generates predicted vs. actual charts for visual validation
- Provides confidence-level breakdown (high/medium/low)
- Suggests model improvements based on backtest results

**Data Accuracy**:
- **Before**: Used ETF proxies (SPY ~$670 for S&P 500)
- **Now**: Uses actual index data (SPX ~$6,700 for S&P 500)
- All predictions, charts, and backtests now use real index values

**Rationale**: Backtesting provides empirical evidence of model performance, helping users understand prediction reliability and identify when the model performs best. Date alignment ensures predictions are forward-looking (using only past data to predict future prices).

### Data Storage

**Current Implementation**: PostgreSQL database for prediction history and alerts

**Schema**:
- `predictions`: Stores historical predictions with actual outcomes
- `alerts`: Tracks significant movement alerts
- Supports accuracy tracking and performance analysis over time

**Rationale**: Database storage enables long-term tracking of prediction accuracy, historical analysis, and model performance evaluation across different market conditions.