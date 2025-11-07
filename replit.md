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
1. **Data Acquisition**: Polygon REST client for fetching historical market data
2. **Feature Engineering**: Technical indicator calculation module (SMA, EMA, RSI)
3. **Prediction Engine**: scikit-learn linear regression with standardized features
4. **Visualization**: Plotly for interactive time-series charting

**Design Pattern**: Pipeline architecture where data flows from API → feature engineering → prediction → visualization

**Rationale**: Linear pipeline simplifies data flow and makes the prediction process transparent. Technical indicators (moving averages, RSI) serve as features because they capture market momentum and trend patterns that inform price predictions.

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
- Authentication: API key-based
- Data Retrieved: OHLCV (Open, High, Low, Close, Volume) data for index ETFs
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

### Data Storage

**Current Implementation**: In-memory storage using Streamlit session state

**Rationale**: For a prediction tool with user-specific sessions, in-memory storage is sufficient. No persistent storage required as predictions are generated on-demand.

**Future Consideration**: If historical prediction tracking or multi-user analytics are needed, a database layer (PostgreSQL with time-series optimizations) could be added.