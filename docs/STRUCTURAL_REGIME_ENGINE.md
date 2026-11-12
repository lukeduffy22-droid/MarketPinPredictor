# Structural Regime Prediction v1

Implements the supplied rule-based 0DTE specification as a separate research
estimate for SPX, NDX and RUT. It does not replace the existing model lean,
issue a decision-grade passport, or change promotion/validation thresholds.

## Contract and choices

`backend/structural_regime.py` defines immutable `Snapshot`, `Regime` and
`Prediction` dataclasses, four pure distance functions, `classify_regime`,
`predict_close`, and `score_confidence`. `EngineConfig` exposes initial
thresholds for explicit offline experiments. Live v1 uses the frozen defaults;
changing those defaults or formulas requires a new model version.

- Resolve the supplied overlapping ranges with **neutral precedence**:
  negative below -25 points; neutral from -25 through +25; shallow positive
  above +25 through +50; deep positive above +50.
- Threshold distances use index points, exactly as supplied. They have not
  been normalized or empirically tuned across symbols.
- `band_width` remains signed (`pin - pain`). Range construction sorts the
  anchors and uses `abs(band_width)` for expansion, including reversed anchors.
- Apply the required hard clamp only to deep positive / below pin / above pain.
  Other formulas remain unclamped, including neutral soft bands. Shallow
  positive uses the anchor band when between anchors, otherwise expands each
  side by 25%. Deep positive below pain expands the lower bound by 25%.
- The confidence thresholds unspecified in the supplied text are explicitly
  set to medium **25,000** and high **50,000** in the app's native
  `raw_gamma_x_open_interest_x_100` units. Strength scores are 0.4/0.7/0.9.
  These are initial research choices, not historical quantiles.
- `time_of_day` is the cash-session fraction. OPENING is the first 60 minutes;
  CLOSING is the final 60 minutes; other observations are MIDDAY. The existing
  reviewed calendar supplies holidays and early-close boundaries.
- Volatility validity requires a validated, fresh, nonfallback VIX observation
  from the same process epoch/generation, available by the primary observation.
  Unavailable volatility lowers the heuristic score without fabricating VIX.
- The confidence label is low below 0.5, medium from 0.5 to below 0.75, high
  at or above 0.75. It is **not a calibrated probability**. Bands are structural
  reference ranges, **not prediction intervals**.
- The existing zero-gamma input is a **strike-curve crossing**, not a portfolio
  gamma-versus-spot flip. Regime names follow the requested rule labels; they
  do not establish dealer gamma positions, hedging flows or profitability.

For the supplied SPX example, the implemented weighted sum is
`0.40*7735.76 + 0.35*7719.55 + 0.15*7750 + 0.10*7700 = 7728.6465`.
The reference band is 7700–7750 and the MIDDAY / invalid-volatility heuristic
score is 0.68. The sample JSON's 7719.55 is the input model lean, not this
formula's output. All values in this example are illustrative, not live proof.

## Integration

The lifecycle coordinator builds the estimate after the existing model runs,
using the same observation. It requires valid source calculation, a committed
calculation ID, exact runtime identity, fresh nonfuture timestamps, finite
inputs, an eligible model lean, confirmed same-day expiration, and an open
cash session. Existing passport and runtime publication checks still apply.

Results are nested as `prediction.structural_regime_prediction` in dashboard,
workstation and event responses. Prediction and close-overlay responses also
carry `structural_regime_prediction`. `GET /structural-regime/{symbol}` exposes
the same lifecycle result with explicit abstention reasons. Reads never run
inference or write observations. Raw quote/GEX payloads retain their existing
forecast-free contract. A stale, invalid, cross-generation or unpublished
parent forecast cannot expose a structural numeric estimate. The structural
estimate also expires at its target cash close.

The Streamlit prediction card shows the structural close, band, heuristic
score and label, regime, input evidence, volatility validity and source time.
It always identifies this new, unvalidated model as research-only, even when
other models eventually have decision-grade evidence.

## Prospective journal and completed sessions

The live coordinator appends eligible observations to
`data/structural_regime.db`, separate from observed market tapes and production
prediction tables. Creation is lazy. Tests and other coordinator callers can
inject a journal; application startup configures the standard path.

Each observation retains the full snapshot, regime, prediction, source time,
target time, calculation and runtime identities, model version, thresholds,
and a deterministic prediction ID. Duplicate writes are ignored. SQLite
triggers reject UPDATE and DELETE. No historical predictions are backfilled;
observations must be recorded before their session close. Write failures are
logged and shown in the UI without changing the parent forecast.

After official source artifacts have been verified and the close bundle has
committed, `tools/ingest_verified_closes.py` appends a comparison for the final
prospectively recorded observation of each symbol/model version. It retains
the prediction, actual official close, official source/reference/hash,
observation time, signed/absolute error and structural-band membership.
Corrections append new receipts; earlier receipts stay intact. Reimports are
idempotent. `--history-only` does not perform these comparisons.

An absent official close leaves a session pending. Manual/unverified closes
cannot label this journal. A comparison failure is reported by the importer;
the verified close has already committed and the import can be retried.
These diagnostics do not train, tune, promote, or assert model accuracy.

Future calibration must split by complete session, preserve prediction-time
availability, compare persistence and the existing lean, and evaluate on
untouched sessions before changing live defaults or describing scores as
probabilities. No calibration evidence is claimed by this implementation.

## Verification and activation

Focused tests cover formulas, thresholds, clamping, reversed bands, validation,
holiday/early-close behavior, prospective journaling, correction receipts,
runtime publication and API freshness, frontend mapping, and real Streamlit
AppTest rendering. Tests use temporary databases and stubbed providers.

Run with an absolute temporary SQLite `DATABASE_URL` and the repository
`.venv/Scripts/python.exe -m pytest` against the structural tests plus
workstation, API purity, live-data-client and verified-close import suites.
The repository's database configuration rejects relative SQLite URLs.

Verified on 2026-09-25 with an absolute temporary SQLite target:

```text
.venv/Scripts/python.exe -m pytest tests/test_structural_regime.py tests/test_structural_regime_integration.py tests/test_workstation_state.py tests/test_workstation_api_purity.py tests/test_live_data_client.py tests/test_verified_close_import.py tests/test_verified_close_history_only.py tests/test_app_prediction_export.py tests/test_frontend_databento_only_dependencies.py -q --disable-warnings --maxfail=5
274 passed in 13.84s
```

`git diff --check` also passed for the edited tracked files. Existing unrelated
working-tree changes were retained; this work has not been committed.

Source/test verification does not prove that a running service loaded these
changes. This implementation does not start, stop or restart services, change
subscriptions, or make provider requests. Normal authorized application
startup loads the lifecycle producer and enables prospective capture.
