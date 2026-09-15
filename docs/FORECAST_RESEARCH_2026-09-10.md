# EOD, multi-session and directional research

Implemented September 10, 2026. New algorithms remain research-only; no production forecast or promotion rule was replaced.

## Available today

- Research page: http://127.0.0.1:8502
- Research API: http://127.0.0.1:8001/v1/research/forecasts
- Each symbol has an independent horizon from today's close through 10 trading sessions ahead. Holidays and early closes use the reviewed exchange calendar. Three sessions after September 10 targets September 15.
- The service samples retained evidence once per minute, checks current runtime identity and symbol validity before and after reading, then saves frozen candidates in `data/forecast_research.db`. The production `data/market_data.db` is opened query-only.
- GET requests read cached research; they do not create predictions, start subscriptions or write either journal. Stopped collection and clock rollback suppress numerical output.
- The main Streamlit dashboard contains the same panel in source. Its running process disables hot reload, so the separate page makes the feature available during this session. No existing production process was restarted.

The canonical `start_databento_app.ps1` now starts the independent research API after core service readiness. `MARKETPIN_FORECAST_RESEARCH_AUTOSTART=0` disables this auxiliary startup. To start only research and its companion page:

```powershell
& .\tools\start_forecast_research.ps1 -CompanionDashboard
```

The standalone source database defaults to the same `data/market_data.db` used by the canonical launcher. An explicit `--source-database` override supports another retained database. This matters because the shell's legacy `.env` points at `market.db`, while today's runtime startup log confirms `data/market_data.db`.

## Methods being evaluated

| Method | Inputs and behavior | Current authority |
|---|---|---|
| Last-price baseline | Latest valid same-symbol reference | Research comparison baseline |
| Damped momentum | Continuous 15–30 minute observations; bounded return extrapolation with time remaining | Unvalidated candidate |
| Sample-mean reversion | Same observation window, mean reversion scaled by time remaining | Unvalidated candidate; no fabricated VWAP |
| Positive-GEX pin context | Fresh, valid same-session options context and positive net GEX | Unvalidated candidate; never reused as a future-date pin |
| Equal candidate blend | Fixed average of eligible baseline/candidates | Research-only, not fitted or promoted |
| Multi-session shrunk return | Current reference plus verified unadjusted daily closes; at least 60 complete horizon-return samples and a current prior close | Abstains until sufficient data; chronological purged evaluation |
| Directional shift | Established prior trend, two separated opposing confirmation windows, volatility/noise threshold and family agreement | Descriptive research detector; no probability or trading instruction |

Directional confirmation uses distinct observations and cannot advance merely because the user refreshes. Reconnects, provenance changes, invalid observations and gaps reset the evidence. SPX/SPY, NDX/QQQ, DJI/DIA and RUT/IWM each contribute at most one family vote. Genuine underlying traded-volume VWAP is optional; OPRA quote counts, option volume and ETF bar closes cannot masquerade as underlying VWAP.

Daily training and evaluation honor both event time and when the record became available. Late imports cannot manufacture historical model knowledge. Conflicting closes, large unadjusted split-like discontinuities, overlapping evaluation horizons and modified forecast payloads are rejected. No calibrated intervals or forecast probabilities are claimed.

## Expanded coverage

The research catalog contains 25 instruments:

- Index families: SPX/SPY, NDX/QQQ, DJI/DIA, RUT/IWM; VIX is separate volatility context.
- Market breadth and size: RSP, VTI, MDY.
- Sectors and semiconductors: XLK, XLF, XLY, XLP, XLE, XLI, XLV, XLU, SMH.
- Rates and credit: TLT, IEF, HYG, LQD.

`DIJ` and `DJIA` normalize to `DJI`. ETF values retain their own units. The current adapter does not provide a direct DJI cash index feed; no scaled DIA value is substituted.

DIA/IWM option roots, selector support and underlying validation were added. The expanded OPRA profile is `config/market_universe_expanded.staged.json`. It retains the 3,600-contract total and 1,800-contract family limits and allocates existing SPX/NDX/core selections before optional ETF additions. The profile has not been applied to today's live subscription.

The separate EQUS collector is disabled by default, uses completed minute bars, bounds requests and storage, preserves observation revisions, and backs off for 30 minutes after a structural entitlement denial. Today's existing validator returned `403 license_not_found_unauthorized` for SPY/QQQ/IWM: a live EQUS.MINI entitlement is unavailable. That evidence does not prove whether expanded ETF OPRA options would succeed; they still require staged activation and coverage checks. No paid entitlement or historical backfill was purchased.

[Databento's EQUS.MINI specification](https://databento.com/docs/venues-and-datasets/equs-mini) describes its component-venue equity feed. [DIA's issuer](https://www.ssga.com/us/en/individual/etfs/state-street-spdr-dow-jones-industrial-average-etf-trust-dia) and [IWM's issuer](https://www.ishares.com/us/products/239710/ishares-russell-2000-etf) document the respective index relationships.

## Accuracy findings

At 12:59 CT the existing five-minute shadow journal produced these *matched-observation* comparisons. They measure five-minute reference predictions, not EOD close accuracy:

| Symbol | Matched observations | Baseline MAE | Existing pin-context candidate MAE | Independent sessions |
|---|---:|---:|---:|---:|
| NDX | 180 | 17.653670 | 17.199131 | 3 |
| SPX | 231 | 2.564551 | 3.857455 | 2 |

NDX's candidate slightly reduced mean absolute error while increasing RMSE; SPX's candidate worsened both. These results do not justify universal coefficients or promotion. The journal lacks the required independent sessions for purged walk-forward acceptance. Legacy process-identity omissions and timestamp-ordering failures are excluded from valid evidence.

The new EOD/multi-session journal is collecting prospective forecasts. Available verified daily closes reach only September 2, while retained market-structure sessions begin September 4. Consequently there are no matching matured EOD labels for this new evaluation, and only six verified dates for SPX/NDX. Numerical multi-session forecasts correctly abstain. Accuracy improvement is **not yet verified**.

Re-run the read-only evaluation after new verified closes are ingested:

```powershell
& .\.venv\Scripts\python.exe tools\evaluate_forecast_research.py --output output\forecast_research\accuracy_evaluation.json
```

## Diagnosed defects and fixes

- Passport insertion inherited a long SQLite busy timeout. A reproduced contention test now exercises two bounded 250ms lock waits, fresh write transactions, immutable idempotency and publication guards. The historical lock owner remains unidentified. This source fix awaits the next production backend launch.
- Corrected July 2, 2026 to a regular close and added reviewed 2027–28 early closes from the [NYSE calendar](https://www.nyse.com/trade/hours-calendars).
- Prevented stale/disabled ETF history from displacing independently valid OPRA history, and invalid runtime state from reusing older observations.
- Closed SQLite connections explicitly; transaction context managers alone do not close connections.
- Corrected the independent research service's source-database binding and verified its loaded source fingerprint.
- Expired dashboard results hide stale numerical targets on subsequent rendering; the page uses manual refresh.
- Fixed rolling-window boundaries for irregular timestamps by including a preceding real observation within the allowed gap. Confirmation still requires at least five minutes of actual elapsed evidence; no prices are interpolated.

## Verification and remaining activation

The combined forecast, shift, symbol, equity, passport, streamer, universe, provider and calendar regression run passed **395 tests**. Subsequent UI/launcher/integration checks passed **34 tests**. PowerShell syntax validation passed. Browser verification exercised independent SPX EOD and NDX three-session selection and displayed explicit coverage/abstention reasons.

At 13:04 CT, the main feed advanced 1,328 messages over five seconds; SPX and NDX were fresh, valid and on generation 2. Persistence advanced relative to the 12:41 baseline and SQLite WAL quick-check returned `ok`. RUT/VIX remained invalid. Earlier reconnection and lock errors mean this healthy sample is not proof of uninterrupted full-session validity.

Only the new research processes were started/restarted. Expanded OPRA subscriptions, the passport publication fix and the main-dashboard panel await the next normal production launch. The EQUS basket additionally requires entitlement. Multi-session numerical forecasts require adequate verified history; model promotion requires held-out evidence.

Evidence files are in `output/forecast_research/`: runtime and live-session verification JSON, the existing shadow evaluation, new forecast accuracy evaluation, and the regression test output.
