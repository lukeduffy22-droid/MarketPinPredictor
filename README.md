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

## Market Events

Open **Market Events** in the Streamlit dashboard. The canonical backend serves the same snapshot at `GET /market-events`. The screen shows when a release was published and first observed, actuals, comparable estimates when verified, source coverage, and how the covered markets moved. Dashboard times use America/Chicago. The retained local history is in `data/market_events/events.db`. Set `MARKETPIN_MARKET_EVENTS=0` to disable this component.

| Coverage | Source and timing |
| --- | --- |
| CPI, PCE price index, Fed decisions and Board speeches/testimony | BLS, BEA and Federal Reserve announcements, polled every two minutes after the preceding refresh finishes. Release figures preserve the measurement period and units. |
| Major tech | Apple, NVIDIA and AMD corporate newsrooms provide keyless coverage when the release has a verified publication time. Setting `SEC_USER_AGENT` to the application name and a real contact email enables Item 2.02 filing checks for AAPL, MSFT, NVDA, GOOGL, AMZN, META, TSLA, AVGO, AMD, ORCL, NFLX and INTC. Filing acceptance has its own timestamp. |
| Expectations | Optional `TRADINGECONOMICS_API_KEY` captures future CPI/PCE survey values and eligible Fed target upper-bound expectations. A comparison requires an observation before publication for the same reporting period or meeting, metric and units. Optional `FINNHUB_API_KEY` supplies separately labeled adjusted earnings figures for matched releases. Access depends on provider entitlement. |
| Index levels | SPX, NDX, RUT, DJI, VIX and configured Databento symbols. SOX adds semiconductor context. RVX and VXN add small-cap and Nasdaq volatility. FRED supplies dated closes for seven indices. An entitled Massive key supplies cash-index snapshots. Databento option-implied levels carry their own timestamp and provenance. |
| Market reaction | During live sampling, comparable observations bracket publication with a pre-event sample up to 90 seconds earlier and a post-event sample 5 to 6½ minutes later. Outside regular equity hours, eligible Massive stock minute bars can supply separate SPY, QQQ and IWM fund returns. Missing samples, entitlements or market sessions can leave the window empty. |

The source panel identifies configuration gaps and outages. A daily close retains its session date. A previous release remains visible through an outage. The panel marks CPI, PCE, confirmed major issuer results and selected Fed releases as material. Fed commentary also qualifies when an eligible equity benchmark moves at least 0.35% or cash VIX moves at least 0.5 points across publication. These are monitoring thresholds. The open panel shows newly published material releases. External scheduled notifications run on the separate monitoring schedule. Market Events is informational and has no path to a trade or forecast promotion.

The scheduled `start_market_day.ps1` path requests RUT. For the manual all-index launch the existing command is `.\start_databento_app.ps1 -EnableRutCanary`. Both paths retain W32Time, subscription and coverage checks. Forward-expiry observations may support opening-range context. A same-day closing signal requires full-size PM-settled RUTW pairs. Traditional AM-settled RUT, mini contracts and forward expirations cannot establish that signal. A core-only launch can show RUT as unavailable.

A workstation acceptance run should check an advancing current-generation RUT quote, the same-day RUTW pair count, a captured opening bucket, a healthy source poll, and a timestamped window around an actual release. CI exercises parsers, safety gates and UI behavior with fixtures.
