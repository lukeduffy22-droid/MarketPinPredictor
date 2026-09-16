# MarketPinPredictor — Product Overview

## Purpose
A CUDA-accelerated market prediction system that forecasts end-of-day (EOD) index closes from live Databento OPRA market data, gamma-exposure (GEX) pin analysis, and trained PyTorch models. Designed for personal trading research and educational use.

## Core Value Proposition
- Real-time options market microstructure analysis (GEX, gamma pin, max pain, zero gamma)
- Deterministic, explainable EOD price forecasts with provenance-tracked feature snapshots
- Auditable closing-tape pipeline with model calibration and promotion governance
- CUDA-accelerated inference on NVIDIA Blackwell (RTX 50 series) GPUs

## Key Features

### Live Market Data
- Databento OPRA live stream ingestion for SPX, NDX, VIX (configurable)
- Put/call parity-derived reference prices (not official exchange OHLC)
- Opening Range Breakout (ORB) capture with 5/15/30/60-minute windows
- Subscription lifecycle management with provenance epoch tracking

### Prediction Engine
- Databento Quant Ensemble: weighted combination of gamma_pin, zero_gamma, max_pain, likely_close
- VIX volatility pressure adjustment (capped ±0.08% of spot)
- Historical context adjustment from realized volatility
- Confidence scoring (35–95%) based on data quality heuristics
- Feature snapshot SHA-256 for exact replay of any stored prediction

### Closing Tape Pipeline
- Verified close artifact ingestion and governance
- Model calibration and promotion approval workflow
- Historical backfill and bulk import from Databento
- SQLite-backed audit trail with immutable decision sidecars

### Market Structure Analysis
- GEX (Gamma Exposure) calculation per strike and expiration
- Pin level tracking: gamma_pin, max_pain, zero_gamma, pin_lead_ratio
- Intraday pin drift monitoring with change-count tracking
- ORB provenance alignment across subscription epochs

### Monitoring & Operations
- Market monitor with cadence, session rollover, and universe prestage checks
- Opening acceptance preflight with timeout guards
- Data quality outbox and notification outbox monitors
- MCP server bridge for agent/Codex integration

## Target Users
- Individual quantitative traders researching index options dynamics
- Researchers studying gamma exposure and options market microstructure
- Developers building on top of Databento OPRA data pipelines

## Supported Symbols
Primary: SPX, NDX, VIX
Canary candidates: XSP, XND, RUT, MRUT, OEX, DJX, RUI, XAU, HGX, OSX, UTY

## Deployment Model
- FastAPI backend (port 8001) + Streamlit UI
- Local Windows workstation with CUDA GPU
- SQLite databases for market data, forecasts, and shadow research
- `.env`-based configuration (never hardcoded credentials)
