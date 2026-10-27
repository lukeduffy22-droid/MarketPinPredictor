# Observed indicator connection — October 1, 2026

The Databento dashboard can now consume completed, retained ETF minute bars
through a separate read-only research endpoint. Previously the live adapter
always supplied an empty indicator frame. This change does not enable a
collector, purchase a license, change a subscription, or alter forecast models.

## Implemented

- `GET /v1/research/indicator-bars?symbol=SPX` on the existing research service
  reads the adjacent `equity_context.db` in SQLite read-only/query-only mode.
  Missing stores stay missing. Reads have row and execution-time bounds.
- The existing equity collector's stored instrument, source, revision hash,
  observation time, OHLCV values, completed-minute boundaries and same-session
  continuity are checked. Current display requires a fresh full-session history
  from the opening minute and at least 35 bars. Failure is explicit.
- The live fetch and actual lifecycle-batch dashboard paths attach verified
  indicators without changing the parent forecast or its identity.
- Display-only indicators: rolling RSI 14, MACD 12/26 with signal 9, SMA 20,
  momentum 10, KAMA 10/2/30, and session typical-price volume weighting. The
  latter is labeled a minute-bar approximation, not trade VWAP. Warm-up values
  are not manufactured; current displayed values must be finite.
- SPX/SPY, NDX/QQQ, RUT/IWM and DJI/DIA relationships are explicitly labeled
  ETF context. Prices and volumes are never converted to cash-index units.
  The context chart does not overlay an index forecast or invoke combined
  index technical-indicator AI. Source time is revalidated on each render.
- CSV context rows retain the ETF source identity separately from forecast
  identity, identify derived indicators, and omit cross-instrument targets.

## Evidence

**OBSERVED (offline): 154 tests passed.** The suite exercises retained SQLite
bars through the real FastAPI endpoint, frontend transport/adapter, independent
indicator arithmetic, Streamlit rendering, expiry, invalid sources, gaps,
duplicates, future timestamps, missing stores, request failures, forecast
identity and export labeling. Temporary fixture data only; no provider calls.

Receipts: `output/indicator_connection_20261001/pytest-final.xml`,
`TESTED_SHA256SUMS.json`, `tested-source/`, and `index-baseline/`.
The baseline is the existing Git index; prior staged work was preserved.

## Activation and remaining data prerequisite

The running research service and dashboard must reload these files. The
prepared `output/indicator_connection_20261001/Activate-Indicators.ps1` verifies
source hashes, actual process ownership, research endpoint/database identity
and the existing supervisor lock. It restarts only research and dashboard,
uses the dashboard's existing exact-PID guard, and checks that the backend PID
is unchanged. Each attempt writes a new timestamped activation receipt.

The initial non-admin check refused activation because Windows hid the
existing elevated process owners. A Windows Administrator confirmation was
requested. Successful activation must be established by an `ACTIVATED` receipt
and endpoint readback; source/test success alone does not establish it.

At 12:55 PM Central, Windows reported that the elevation operation was canceled.
The activation script did not start, no activation receipt reports success,
and no service restart was performed by this change. Activation remains pending.

**NOT YET VERIFIED:** live indicators from an authorized captured bar stream.
No retained `equity_context.db` was found during this check. Current October 1
SPX/NDX audits separately report the EQUS.MINI live-license denial. After access
and collection are resolved, this connection can display qualifying bars.
Neither that licensing issue nor the separate missing calculation-ID defect
is repaired by this change.

## Activation observed at 2:54 PM Central

On the user's subsequent request to load the fix, the guarded research and
dashboard activation completed. The separate readback receipt is
`output/indicator_connection_20261001/20261001T195453090Z.activation-readback.json`
with status `ACTIVATED_OBSERVED`.

- **OBSERVED:** all six runtime files match the tested SHA-256 manifest.
  Research PID 10992 reports the expected loaded source hash
  `dee8c1a0184fe31d9a464671caf43683285ec616a10f898a59c91c6613768de3`,
  unchanged source database identity, and query-only access. Its completed
  research captures advanced from 1 to 4 after activation.
- **OBSERVED:** the guarded dashboard restart passed its source preflight,
  replaced PID 29916 with PID 41628, and returned a successful completion log.
  The browser was reloaded and rendered the new page at 2:53:18 PM Central
  without a visible page exception.
- **OBSERVED:** backend PID 52440, subscription epoch and generation remained
  unchanged across activation. Received messages advanced from 131,965 at
  19:45:22Z to 281,947 at the final readback. This establishes continued feed
  receipt, not forecast eligibility or a complete-day capture.
- **OBSERVED:** the new SPX/SPY and NDX/QQQ indicator endpoints return
  `RETAINED_BAR_DATABASE_MISSING`. The connection is active; usable live bars
  and displayed live indicators remain **NOT YET VERIFIED**.
- **OBSERVED:** the independent backend prediction readiness flag remained
  false at final readback. This deployment does not establish prediction
  readiness or repair the other previously reported problems.

The first activation helper did not finish; a hidden stop confirmation was
the inferred cause. Its exact PID was verified and stopped before retrying
with a noninteractive helper and explicit stop authorization. A recovery
attempt initially rejected a local-time/UTC comparison; the comparison was
corrected to UTC without weakening the ownership check. The dashboard's
guarded child later logged successful completion and exited, while its outer
wrapper remained waiting. Independent ownership/hash/endpoint checks verified
activation, and only that exact outer wrapper was stopped. All attempt
receipts and progress records were retained; no service descendants were
stopped during wrapper cleanup.

### Subsequent parallel ORB deployment

After the indicator activation readback, a different supervisor invocation
(`04ee6b09083f4f3e99e7d7e2bf530460`, caller `CodexNdxOrbRepair`) stopped the
backend at 2:55:20 PM Central and successfully launched PID 27484 at
2:56:48 PM. At 2:57 PM its live endpoint reported connected and progressing
Databento data with a new subscription epoch; prediction readiness still
reported false. Research PID 10992 and dashboard PID 41628 continued running.
The indicator activation's unchanged-backend proof is limited to its own
2:54 PM readback and must not be extended across this separate restart.
The timestamped `*.post-parallel-restart.json` receipt retains the final check
and the separate supervisor's completion log.
