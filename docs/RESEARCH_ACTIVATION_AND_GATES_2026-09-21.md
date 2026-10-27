# Research activation and readiness criteria

Date: 2026-09-21, America/Chicago. This is an activation addendum to
`FRONTEND_DATA_REPAIR_PHASES_1_2_2026-09-21.md`; that document records the earlier
offline-only work. The user subsequently approved the scoped research launch,
dashboard deployment, and necessary supporting work.

Update at **23:14 CT**: the approved dashboard deployment is now verified. See
the completion receipt below; the earlier access blocker is retained as history.

## What should count as passing

The 60-return floor in `backend/forecast_horizons.py` is a minimum input-history
requirement for positive-horizon research forecasts. It is not a requirement for
an individual collection cycle or operating session to succeed, and meeting it
does not establish predictive accuracy. Horizon 0 does not require daily-return
history. Report these independently:

| Assessment | Required evidence | Current result |
| --- | --- | --- |
| Research service operation | Verified process and loaded source identity; worker completions/timestamps advance; supported requests return the expected contract; no fabricated journal rows | OBSERVED after close, including more than two collection intervals |
| Market-hours core operation | Current-generation required symbols, fresh valid observations, advancing transport and persistence, applicable opening evidence | NOT YET VERIFIED by this after-close activation; historical shortages in optional research do not redefine this gate |
| Forecast availability | Per symbol and horizon: current valid input, exact instrument/source provenance, current continuous daily history, at least 60 qualified horizon returns, and all remaining evaluator checks | BLOCKED for positive horizons by retained history; all horizons correctly abstain outside regular hours |
| Model promotion | Separate preregistered, point-in-time, held-out validation against baselines and the applicable governance requirements | Unchanged; no promotion or accuracy claim |

Assess symbols and horizons individually. Do not require every optional symbol to
produce a forecast before an otherwise valid required-symbol session can pass.
Likewise, an all-abstain collection cycle can demonstrate working infrastructure
without demonstrating a forecast's eligibility or usefulness.

The value 60 is the current configured floor, not an empirically established
universal optimum. It is reasonable to review it through a separate, versioned
offline experiment. Specify candidate floors before inspecting results; use
chronological holdouts, prevent overlapping horizon outcomes from leaking across
splits, and compare error, interval coverage, baseline skill, and abstention by
symbol/horizon. A lower floor should require evidence of acceptable performance,
not merely a larger number of populated cards. No threshold or formula changed
in this activation.

## Why waiting alone will not repair history

The refreshed query-only inventory is in
`output/research_activation_20260921/history-inventory.json`.

| Symbols | Qualifying closes each | Latest session | Qualified returns at horizons 1 / 5 / 10 |
| --- | ---: | --- | --- |
| SPX, NDX, RUT, VIX, SPY | 6 | 2026-09-02 | 4 / 0 / 0 |
| Other 20 tracked symbols | 0 | None | 0 / 0 / 0 |

All positive horizons also report `daily_close_history_not_current`. Reducing
60 to 20, 10, or 4 would leave that freshness problem and the zero longer-horizon
samples unresolved. New sessions help only if qualifying closes actually enter
the verified ledger.

The practical next step is the provenance-preserving history import described in
the phase-1/2 report, together with observed daily ingestion receipts. Existing
licensed historical artifacts can supply prior sessions; there is no requirement
to wait for 60 new live sessions just to populate this input history. At least
61, 65, and 70 continuous qualifying closes are needed to form 60 overlapping
returns at horizons 1, 5, and 10 respectively, through the previous required
session. Overlapping returns are not 60 independent validation observations.

The existing import source contract covers SPX, NDX, RUT, VIX, and SPY. The other
instruments require their own reviewed source contract; ETF and cash-index prices
cannot substitute for each other. No qualifying historical source bundle was
found in the checked local source folder or connected Dropbox/Basic Memory
searches. Those limited searches do not establish that no such artifacts exist
elsewhere. No provider retrieval, new entitlement, historical import, or
production-data modification was performed by this activation.

Existing verified-close selection/settlement tasks were observed as configured;
successful settlement had not yet been observed in the pre-17:05 CT check. Their
configuration alone is not proof that tomorrow's history will advance. They were
not changed or manually triggered here.

## Activation changes and evidence

- Launched only `tools/start_forecast_research.ps1`, using the repository's
  canonical `.venv`. Research owns `127.0.0.1:8001`, PID **55192**, instance
  `d855ac0816c14ec1ae865524401bf5b8`. The runner has no provider collector and
  reads the production source database query-only.
- First completion: **16:41:50.530 CT**, count **1**. Subsequent verified
  completion: **16:50:52.368 CT**, count **10**, interval 60 seconds, worker alive,
  no worker error. Both loaded fingerprints match a fresh runner `--check`.
- Every horizon **0-10** returned the expected API schema for all **25 symbols**.
  All abstained after hours. The separate research journal retained **832** rows
  in the checked samples; no invented eligible forecast was appended.
- Added the versioned local setting `config/forecast_research_runtime.json` and
  its loader in `tools/forecast_research_supervisor.psm1`. This enables the
  existing watchdog's independent research check without adding a scheduler or
  changing machine environment settings. Explicit
  `MARKETPIN_FORECAST_RESEARCH_AUTOSTART=0` still takes precedence.
- Manual supervised reconciliation returned `READY` without launching a second
  process. The existing scheduled watchdog independently logged
  `research_supervision ... status=READY code=RESEARCH_OWNED_ENDPOINT` at
  **16:51:45 CT**, invocation `694b2fd9bd3b4a568a6886b49dc9cb02`.
- Backend PID **42724** and dashboard PID **60352** remain unchanged. Backend
  epoch `53b6321d32c9a27844089a1a81e7bb3c15ca3a0041269996dd5f8c9877a151d1`,
  generation **1**, and message count **1,176,222** are unchanged; reconnect count
  remains zero. SQLite remains WAL with `quick_check=ok`; all six checked
  persistence tables retain their counts and latest timestamps. This is expected
  after close and is not proof of market-hours persistence advancement.
- The in-app browser rendered research rows for SPX, NDX, SPY, and QQQ without
  the generic service-outage message after selecting **Load forecast research**.
  The displayed capture was `2026-09-21T21:42:50.699253+00:00`. The old seven-column
  table remains loaded, so this proves service restoration but not deployment of
  the new history/error/selection UI.

Evidence directory: `output/research_activation_20260921/`. Key receipts:
`research-launch.json`, `supervised-reconcile.json`, `progression.json`,
`expected-identity.json`, `forecasts-h0.json` through `forecasts-h10.json`,
`scheduled-supervision.log`, `baseline.json`, `after.json`, and `verification.json`.

## Tests and preservation

The additional supervision-setting coverage passes **19/19 tests** under both
PowerShell 7 and Windows PowerShell 5.1. Reports: `supervisor.xml` and
`supervisor-winps.xml` in the activation evidence directory. Tests cover persisted
activation, explicit-disable precedence, rejected string truthiness/unknown
schema, and the prior ownership, serialization, failure, and backoff cases.
Application process/HTTP operations are mocked in these tests.

The earlier offline results remain separately recorded: **137 Python/API/AppTest
tests**, **163 core supervisor tests**, and the earlier 16 research supervisor
tests under each PowerShell version. Python/UI code was not changed again during
activation, so those suites were not rerun just to repeat existing evidence.

No staging, commits, resets, or branch changes were performed. The original
staged diff is unchanged. Of the 24 originally dirty files, only the mutable
`data/ui_preferences/advisor_selection.json` differs from the earlier offline
baseline; it was left in place. Other concurrent diagnostic work was preserved.
The exact comparison is in `preservation.json`. No original application records
or tapes were rewritten or removed.

## Dashboard deployment blocker and next steps

**Research: loaded and observed. Dashboard changes: offline verified, deployment
still pending.** Windows denies this session's process query access to dashboard
PID 60352 and its parent 57092 (`OpenProcess` error 5); CIM/process inspection
does not provide their executable/command line. Receipt: `ownership-access.json`.
The existing SYSTEM watchdog can verify them, but this session cannot safely
perform the guarded stop. No restart was attempted and no ownership guard was
weakened.

The operator skill requires: "Refuse to stop an owner that is not verified as a
MarketPinPredictor process under the resolved repository."
Source: `C:/Users/LukeD/.codex/skills/marketpin-live-session-operator/SKILL.md`.

Resume from an elevated Windows session. The user's deployment approval remains
valid; another permission decision is unnecessary. Recheck the current listener
instead of reusing the recorded PID, then use the existing dashboard-only guard:

```powershell
Set-Location -LiteralPath 'C:\Cprojectsgpu_app\MarketPinPredictor'
$activationRoot = (Get-Location).Path
Import-Module .\market_app_supervisor.psm1 -Force
$dashboardOwners = @(Get-MarketAppListenerProcessIds -Port 8501)
if ($dashboardOwners.Count -ne 1) { throw 'Expected exactly one dashboard owner.' }
if (-not (Test-MarketAppVerifiedProcess -ProcessId $dashboardOwners[0] `
    -ProjectRoot $activationRoot -RequiredCommandMarkers @('streamlit', 'app.py'))) {
    throw 'Dashboard owner could not be verified; no restart authorized for an unknown owner.'
}
& .\ensure_market_app.ps1 -RestartDashboard `
    -ExpectedDashboardPid $dashboardOwners[0] -Caller CodexResearchActivation
```

After that guarded restart, verify the new panel's requested horizon, target
session, source timestamps, history counts/reasons, and selection binding with
mixed 0/1/5/10 selections. Confirm core backend and research identities remain
unchanged. During the next regular session, separately verify required-symbol
freshness and persistence advancement. History acquisition/ingestion and any
threshold experiment remain separate evidence work, not prerequisites for
restarting the already-tested dashboard.

## Dashboard deployment completed at 23:09 CT

The user ran the prepared helper from Administrator PowerShell. It verified
ownership, then the canonical dashboard-only guard completed successfully at
**23:09:32 CT** (2026-09-22 04:09:32 UTC), invocation
`f15b07675b4c42a79f0d8ff46678303c`. Dashboard PID changed **60352 -> 13140**.
Backend PID **42724** and research PID **55192** stayed unchanged. Core epoch,
generation, message count, reconnect count, and last error were unchanged between
the checks during and after this restart. Research completions advanced **386 ->
390** with the same instance identity. The post-restart inspection reports
`warming/off-hours`, no issues/warnings, SQLite WAL and `quick_check=ok`; all six
checked persistence tables retain their prior counts/timestamps. No market-hours
progression is claimed at this hour.

The browser was reloaded once from its stale 16:43 snapshot. The new calendar
caption, submitted-selection timestamp, history columns, source columns, and
instrument units all rendered. Submitted mixed horizons were SPX=0, NDX=1,
SPY=5, QQQ=10. The displayed targets were September 22, September 23,
September 29, and October 6 respectively. It was already September 22 in the
exchange's New York timezone. All results correctly abstained outside regular
hours. The positive horizons displayed `daily_close_history_not_current` and
`insufficient_verified_daily_history`, with qualified/required counts **4/60**,
**0/60**, and **0/60**. The generic service-outage warning was absent.

Removing QQQ from the research selection hid the old results and displayed
"Selections changed. Submit Load forecast research to replace the previous
results." This verifies the deployed selection binding, in addition to the
earlier offline AppTest coverage. The original four-market selection was then
restored. No model/formula/provider/subscription changes were made.

Receipts are under `output/dashboard_activation_20260921/`:
`guarded-restart.log`, `during-restart.json`, `post-restart-inspection.json`,
`postconditions.json`, and `deployment-verification.json`. Application source
hashes and the staged diff match the prepared pre-deployment baseline. This was
runtime/browser validation of the previously tested source, not another code
deployment or full test-suite run.

The one-off helper had a completion-reporting defect: `Start-Process -Wait`
waited for the long-running dashboard descendant even after the guard exited.
Its original `restart-result.json` can therefore remain at
`RESTARTING_DASHBOARD`; use the independent completion receipts above. The helper
has been corrected to wait for the guard process alone. This did not require
another application restart. If its original PowerShell invocation is still
waiting, Ctrl+C cancels that wait; do not rerun the restart command. Windows
documents the process-tree behavior in
[Start-Process](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.management/start-process?view=powershell-5.1#-wait).

Remaining work: verified historical-close acquisition/ingestion, evidence that
daily close ingestion advances, and next-session live freshness/persistence
checks. The 60-return input threshold and all model-promotion gates remain
unchanged. Dashboard activation itself is complete.
