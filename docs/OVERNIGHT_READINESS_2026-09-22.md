# Overnight readiness — September 22, 2026

Assessment started at 00:20 CT, before the September 22 cash session. This is
operational hardening of the deployed phase 1–2 repair and the existing close-task
launcher. Formulas, the 60-qualified-return threshold, subscriptions, and retained
market records are unchanged. All Python checks used the repository `.venv`.

## Changes completed

- `tools/forecast_research_supervisor.psm1` now uses the core supervisor's
  `netstat` listener lookup instead of CIM-backed `Get-NetTCPConnection`. The
  ownership and endpoint-identity checks remain in place. This removes a known
  stall-prone listener lookup; it does not establish the cause of every earlier
  watchdog timeout or prove that all timeout paths have been eliminated.
- `tools/run_verified_close_postclose.ps1` captures native stderr and the actual
  exit code before restoring PowerShell's terminating-error preference. Windows
  PowerShell 5.1 previously could terminate on redirected stderr before writing
  the task's diagnostic log. Failures remain failures; they now reach the logging
  path. Existing selection, settlement, and ingestion behavior is unchanged.
- Added a listener-delegation regression check and three isolated Python-process
  fixtures covering failure, warning, and successful output. Tests execute only
  extracted launcher functions, never a settlement or provider command.

These are per-invocation launcher changes. They require no backend, dashboard, or
research-worker restart. The settlement launcher is ready for its next scheduled
invocation; a real settlement run was not triggered to test it.

## Verification

- PowerShell 7: **23 passed, 0 failed**.
- Windows PowerShell 5.1: **23 passed, 0 failed**.
- Canonical `.venv` pytest: **21 passed** across database URL/target, diagnostic
  automation, and post-close scoring tests. Fixture databases and mocked
  reviewers/fetchers were used; no real AI or market-provider requests occurred.
- The updated research supervisor returned `READY / RESEARCH_OWNED_ENDPOINT`
  in **4.8 seconds**, verifying and retaining the existing worker.
- Between 00:20 and 00:32 CT, research completed captures advanced **457 -> 468**.
  PID **55192**, instance `d855ac0816c14ec1ae865524401bf5b8`, loaded source hash,
  and source-database identity stayed unchanged. Worker reported `WAITING`, alive,
  with no error. All-abstain captures are valid progression evidence, not proof
  that forecasts are eligible.
- Backend epoch, generation, message count, reconnect count, and last error were
  unchanged. Backend PID remains **42724**; no application restart was performed
  in this overnight pass. After-hours inactivity is expected.
- The read-only operator inspection at **00:31:50 CT** reports
  `warming/off-hours`, reachable backend/dashboard, and no inspection issues or
  warnings. SQLite reports WAL mode and `quick_check=ok`. Retained invalid/stale
  market calculations remain ineligible; this is not a regular-session health
  or prediction-readiness claim.
- The staged diff is unchanged. All baseline application/source files outside
  the three intentionally edited existing files retain their hashes. The new
  launcher test and this report were added without staging or committing.
- `git diff --check` passed. The earlier 137 Python/API/Streamlit and 163 core
  Pester results remain prior evidence; those unchanged suites were not rerun.

Receipts: `output/overnight_readiness_20260922/baseline.json`, `powershell7.xml`,
`powershell51.xml`, `python.xml`, `research-supervision.json`,
`preservation-and-runtime.json`, `inspection.json`, `close-tasks.json`, and
`ownership.json`.

## Outstanding evidence and activation

The September 21 settlement task returned exit 1 and left no settlement log; its
source-artifact directory was empty. The stderr handling defect explains how a
failure can lose its receipt, but the underlying settlement failure cannot be
identified from that evidence. No settlement retry, close import, history rewrite,
or threshold reduction was performed. Acquisition and ingestion of verified
closes remain necessary to resolve the historical-data shortage.

The research worker is active and the dashboard deployment completed at 23:09 CT
on September 21, as documented in `RESEARCH_ACTIVATION_AND_GATES_2026-09-21.md`.
The phase 1–2 launcher changes above are available immediately on invocation.

Earlier, separate backend configuration and diagnostic-automation changes still
need a verified backend activation. The backend fingerprint reports the older
configuration module. Its fingerprint does not enumerate diagnostic automation,
so that module's loaded revision is not independently established. The existing
September 21 diagnostic job remains `FAILED / JSONDecodeError`, with one attempt;
activation must preserve it, not reset or retry it.

At 00:33 CT, this Codex command session was not elevated and could not verify the
backend owner's executable. The operator skill requires: "Refuse to stop an owner
that is not verified as a MarketPinPredictor process under the resolved
repository." Source:
`C:/Users/LukeD/.codex/skills/marketpin-live-session-operator/SKILL.md`.
No ownership guard was weakened. The canonical restart can also contact Databento
for universe discovery, so clarification of the original no-provider restriction
was requested before that separate activation.

If that scope is authorized, use an elevated session, fresh process ownership
checks, and the canonical backend-only guard. Preserve the currently enabled RUT
canary and contract cap rather than taking the launcher's three-symbol default.
Validate an eligible retained universe before stopping the current backend, then
verify loaded code, local endpoint readiness, unchanged dashboard/research owners,
and absence of unexpected errors. At the next regular session, verify current
generation, required-symbol freshness, and persistence advancement separately;
overnight tests cannot prove those live postconditions.
