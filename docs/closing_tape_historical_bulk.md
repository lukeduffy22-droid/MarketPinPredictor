# Closing-tape historical bulk automation

The bulk runner connects the cost-capped Databento planner/downloader to the
immutable historical importer and the existing readiness audit. It does not
modify or reuse the live recorder, and it does not train or promote a model.

## 1. Review a dry run

Dry run is the default. It performs provider metadata, condition, cost, and
billable-size checks, but makes no time-series request and writes no journal.

```powershell
.\.venv\Scripts\python.exe tools\run_closing_tape_historical_bulk.py `
  --start 2026-06-01 `
  --end 2026-08-31
```

Save and review these output fields:

- `batch_plan_sha256`
- `estimated_cost_usd`
- `estimated_billable_bytes`
- every day's `provider_condition`, request windows, and `plan_sha256`
- `blocked_dates` and `readiness_before`

An already eligible date is retained and skipped. A provider-unavailable date,
conflicting existing bundle, or ambiguous duplicate capture blocks execution.
Date ranges omit configured weekends and US cash-market holidays; an explicitly
requested non-session date is rejected instead of being silently shifted.

## 2. Execute the reviewed batch

Execution requires the exact batch hash plus explicit aggregate ceilings. The
ceilings are local preflight checks; they are not Databento billing controls.

```powershell
.\.venv\Scripts\python.exe tools\run_closing_tape_historical_bulk.py `
  --start 2026-06-01 `
  --end 2026-08-31 `
  --execute `
  --approve-batch-plan-sha256 <reviewed-hash> `
  --max-estimated-cost-usd <reviewed-ceiling> `
  --max-estimated-billable-bytes <reviewed-ceiling>
```

Use `--download-only` with `--execute` to acquire and verify the immutable DBN
bundles without importing them. The default execution path downloads or reuses
each three-component bundle, re-verifies its hashes and decoded evidence,
imports it idempotently, and reruns capture/label/model readiness.

The resumable journal is written atomically beneath
`data/backtest_pipeline/historical_bulk/`. Resume never trusts the journal by
itself: the physical manifest and bundle are rediscovered and rehashed, and the
importer rechecks its content-addressed completion record.

Bulk acquisition has no active-capture override. If the live closing-tape
recorder is running, acquisition/import refuses to proceed.

## Evidence and anti-leakage boundary

Open interest in `contract-surface-v4` is selected from immutable observations
using the Databento receive timestamp. For a contract minute, an observation is
eligible only when `ts_recv_ns <= minute_utc + 1 minute`, matched to the same
session, feed, raw symbol, and source hash. The latest eligible DELETE produces
missing OI; `ts_ref`, `ts_event`, a later mutable projection, and missing receive
timestamps cannot backfill an earlier feature.

The v4 surface keeps the maximum OI availability timestamp for local audit.
Previously frozen v3 surface/model artifacts intentionally fail the current
feature-contract check and must be rebuilt from corrected evidence.

The bulk result distinguishes:

1. immutable acquisition/import status;
2. capture eligibility;
3. full-five-family verified-close label status;
4. overall model-training readiness.

Missing labels remain an explicit work queue. Surface construction, CUDA
evaluation, paper promotion, and production promotion are separate fail-closed
steps and are never inferred from a successful download.
