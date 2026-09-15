# Market Pin Predictor

Market Pin Predictor is a local Streamlit and FastAPI application for recording Databento OPRA evidence, analyzing index-option market structure, and producing evidence-gated research forecasts.

The repository deliberately keeps two kinds of information separate:

- **Observed:** immutable option trades and their pre-trade national best bid/offer, definitions, event/receive timestamps, corrections, flags, and daily open-interest statistics.
- **Inferred:** trade-side heuristics, positioning estimates, gamma interpretation, and model forecasts. These remain labeled estimates because OPRA does not publish aggressor side or participant holdings.

## Current architecture

```text
Databento OPRA
    -> backend/databento_streamer.py
    -> backend/closing_tape/        capture, replay, integrity, evaluation
    -> backend/app.py               FastAPI contracts and lifecycle gates
    -> app.py                       Streamlit dashboard
```

Important entrypoints and contracts:

- `start_databento_app.ps1` — canonical Windows launcher for the complete local stack
- `backend/app.py` — canonical FastAPI application
- `server.py` — direct launcher for `backend.app:app`
- `app.py` — Streamlit dashboard
- `backend/prediction_authority.py` — fail-closed production authority
- `backend/prediction_passport.py` — immutable forecast identity and provenance
- `app/core/gex.py` — GEX formulas and sign conventions

## Local setup

Create `.venv`, install the backend requirements, and copy `.env.example` to a local `.env`. Never commit `.env` or provider credentials.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements_backend.txt --extra-index-url https://download.pytorch.org/whl/cu128
Copy-Item .env.example .env
```

The pinned CUDA packages target the workstation configuration. CI intentionally installs a lighter CPU-only dependency set for contract tests.

## Run

For a normal live session, use the guarded launcher rather than starting components separately:

```powershell
.\start_databento_app.ps1
```

For backend-only development:

```powershell
.\.venv\Scripts\python.exe server.py
```

An open port or HTTP 200 is not sufficient live-readiness evidence. The lifecycle and provenance endpoints must also show advancing, fresh, current-generation data.

## Test

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider
.\.venv\Scripts\python.exe tools\check_publication_boundary.py --base origin/friday-1/9
```

Use targeted tests while editing, then run the relevant broader offline suite. Tests must use temporary databases and must never mutate the configured live database.

## Repository boundary

GitHub holds source, tests, configuration templates, and reviewed documentation. Live databases, DBN captures, exports, logs, generated datasets, model weights, caches, environments, and bundled third-party repositories remain local and are ignored. See [.github/copilot-instructions.md](.github/copilot-instructions.md) for contributor and Copilot constraints.

Forecasts and side classifications are research estimates unless the recorded evidence has passed the existing maturity, evaluation, calibration, and promotion gates. This project does not place trades and is not investment advice.
