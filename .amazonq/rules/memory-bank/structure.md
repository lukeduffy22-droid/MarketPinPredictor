# MarketPinPredictor — Project Structure

## Top-Level Layout

```
MarketPinPredictor/
├── app/                    # FastAPI application package (primary backend)
├── backend/                # Core domain logic and services
├── tests/                  # ~200+ pytest test files
├── tools/                  # CLI operator scripts and MCP server
├── data/                   # SQLite DBs, parquet, closing prices, caches
├── models/                 # Trained PyTorch model artifacts (.pt, .json)
├── config/                 # Market universe and calendar JSON configs
├── docs/                   # Architecture docs, incident logs, handoffs
├── exports/                # Per-symbol prediction exports and snapshots
├── logs/                   # Runtime, audit, capture attempt logs
├── research/               # Jupyter notebooks and shadow formula validation
├── pytorch/                # Full PyTorch source tree (vendored)
├── vision/                 # TorchVision source tree (vendored)
├── app.py                  # Streamlit dashboard entry point
├── databento_forecast.py   # Standalone EOD + next-week forecast CLI
├── train_etf_daily_model.py # ETF price model training script
├── pyproject.toml          # Project metadata and uv dependency config
├── requirements_backend.txt # Pinned production deps (CUDA cu128)
└── .env / .env.example     # Runtime configuration
```

## `app/` — FastAPI Application Package

```
app/
├── api/
│   └── main.py             # FastAPI app, all route registrations
├── core/
│   ├── gex.py              # GEX calculation engine
│   ├── audit_persistence.py
│   ├── audit_snapshot.py
│   ├── accuracy_ledger.py
│   └── sanity_checks.py
├── features/
│   └── calculators.py      # Feature engineering for ML models
├── ingest/
│   ├── websocket_stream.py
│   ├── options_websocket_stream.py
│   ├── websocket_aggregator.py
│   └── rest_fallback.py
├── models/
│   ├── db_models.py        # SQLAlchemy ORM models
│   └── ridge_predictor.py  # Ridge regression nowcast
├── services/               # View/service layer (advisor, live data, ORB, etc.)
├── state/
│   ├── ring_buffers.py     # Per-symbol rolling data buffers
│   ├── oi_cache.py         # Open interest cache
│   └── orb_tracker.py     # Opening range breakout state
└── utils/
    ├── close_predictor.py  # EOD close prediction utilities
    ├── market_calendar.py  # Trading calendar helpers
    ├── market_time.py      # ET/UTC time utilities
    ├── settings.py         # Pydantic settings
    └── snapshot_history.py # Snapshot retention
```

## `backend/` — Core Domain Logic

```
backend/
├── closing_tape/           # Full closing-tape governance pipeline (40+ modules)
│   ├── governance.py       # Governance rules and approval workflow
│   ├── pipeline.py         # End-to-end pipeline orchestration
│   ├── calibration.py      # Model calibration
│   ├── promotion.py        # Model promotion approval
│   ├── live_recorder.py    # Live session recording
│   ├── live_shadow.py      # Shadow formula validation
│   ├── historical_*.py     # Backfill, bulk, import modules
│   ├── sqlite_io.py        # SQLite read/write layer
│   └── training.py         # Model training within the tape
├── ai_predictor.py         # Databento Quant Ensemble prediction engine
├── market_structure.py     # GEX/pin analysis + ORB journal (MarketStructureJournal)
├── databento_streamer.py   # Live OPRA stream + lifecycle snapshots
├── workstation.py          # Passport/provenance contract
├── database.py             # SQLite persistence layer (market_data.db)
├── config.py               # Backend configuration
├── inference.py            # Model inference utilities
├── prediction_passport.py  # Prediction provenance passport
├── prediction_authority.py # Authority contract for predictions
├── monitor_*.py            # 8 monitor modules (cadence, rollover, etc.)
└── passport_analyst.py     # AI-assisted passport analysis
```

## `tests/` — Test Suite

~200 test files organized by domain:
- `test_closing_tape_*.py` — closing tape pipeline (majority of tests)
- `test_databento_*.py` — streamer, universe, health, publication
- `test_market_*.py` — structure, calendar, session, universe
- `test_monitor_*.py` — all monitor modules
- `test_prediction_*.py` — authority, passport, persistence
- `test_app_*.py` — Streamlit UI contracts
- `*.Tests.ps1` — PowerShell integration tests for startup/supervisor

## `tools/` — Operator CLI Scripts

Key tools:
- `marketpin_mcp_server.py` — MCP server for Codex/agent bridge
- `finalize_closing_tape.py` — EOD tape finalization
- `evaluate_closing_tape_models.py` — Model evaluation
- `fetch_verified_close_bundle.py` — Verified close artifact fetch
- `ingest_verified_closes.py` — Close ingestion
- `prepare_databento_universe_cache.py` — Universe cache prep
- `audit_closing_tape_readiness.py` — Pre-market readiness audit

## `data/` — Runtime Data

```
data/
├── market_data.db          # Primary SQLite DB (live metrics, ORB, structure)
├── forecast_research.db    # Forecast research experiments
├── shadow_research.db      # Shadow formula validation
├── closing_tape/           # Closing tape artifacts
├── databento_cache/        # Databento API response cache
├── databento_history/      # Historical DBN files
├── verified_close_sources/ # Verified close source artifacts
└── parquet/                # Parquet exports
```

## `models/` — Model Artifacts

```
models/
├── etf_price_predictor_best.pt     # Primary ETF price model
├── etf_price_predictor_meta.json   # Model metadata/schema
├── etf_rich_predictor_best.pt      # Rich-feature ETF model
├── etf_rich_predictor_meta.json
└── eod_calibration.json            # EOD calibration parameters
```

## Architectural Patterns

### Provenance-First Design
Every prediction, observation, and model artifact carries a provenance chain:
- `subscription_epoch_id` (SHA-256 hex) — identifies the live subscription session
- `subscription_generation` — monotonic counter within an epoch
- `universe_sha256` — hash of the selected option contract universe
- `feature_hash` / `model_artifact_sha256` — deterministic replay identity

### Fail-Closed Validation
All data paths use explicit validation gates. Invalid or fallback-provenance data is rejected before persistence. Functions return structured dicts with `recorded: bool` and `reason: str` rather than raising exceptions for expected failures.

### Append-Only Journals
Market structure observations and ORB reference samples are append-only. No backfilling from later prices. Immutable decision sidecars record the final `progress_eligible` status.

### Dependency Injection for Testability
`MarketStructureJournal` and closing-tape components accept injected `saver`/`loader` callables, enabling pure in-memory unit tests without database setup.

### Layered Architecture
```
Streamlit UI (app.py)
    ↓ HTTP
FastAPI (app/api/main.py)
    ↓
Services (app/services/)
    ↓
Backend domain (backend/)
    ↓
Database (backend/database.py → data/market_data.db)
```
