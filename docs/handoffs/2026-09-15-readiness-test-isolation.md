# Copilot handoff: isolate closing-tape readiness tests

## Preservation scope

Source checkout: `C:\Cprojectsgpu_app\MarketPinPredictor`.
Original branch: `copilot/vscode-mtwynpc2-ellx`.
Source baseline: `fd0b73f99f38a0db49e559cadda5b862f6307ec1`.
Preservation branch: `feature/2026-09-readiness-test-db-isolation`.

This independent root commit intentionally contains only the readiness test file and this handoff. It does not inherit the baseline's database, model artifacts, runtime files, credentials, or unrelated source features. It is not a runnable application checkout and must not be merged wholesale. The original checkout, branch, index, and all existing records remain intact.

## Completed change

The autouse `_isolate_configured_market_database` fixture sets `DATABASE_URL` to each test's temporary `data/market_data.db`. Pytest monkeypatch restores the environment afterwards. This prevents an inherited application database setting from determining the readiness fixtures' database location. No production gate, model, or validation threshold is changed.

On 2026-09-15, in the source checkout with bytecode writing disabled:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
.\.venv\Scripts\python.exe -B -m pytest tests/test_closing_tape_readiness.py -q -p no:cacheprovider
```

Result: **12 passed in 1.47s**. These are offline tests against the existing application baseline, not a live-runtime or forecast-accuracy certification.

## Integration instructions

Compare the destination readiness tests with the original baseline, then port the seven-line autouse fixture only, preserving any newer tests and fixtures. A whole-file copy or root-commit cherry-pick can cause add/add conflicts or overwrite newer work. Dependencies are the existing `backend.closing_tape` readiness, catalog, config and contracts modules and their normal test environment. Re-run the focused command in the integration checkout. No runtime restart, migration, model training or feed activation is needed for this test-only change.

Do not merge into `friday-1/9`, force-push, alter other worktrees, approve/delete PRs, or clean generated records. This document is the shared handoff; no separate message has been sent to Copilot.

## Progress that must not be overlooked

The broader September work is already preserved locally in baseline `fd0b73f99`, whose title understates its scope. It includes advisor workflow/persistence, diagnostic assistant, closing-price workflow, capture diagnostics, validation-method separation, snapshot-cache evidence tooling, closing-tape gates/reconciliation, forecast research changes and associated tests/docs. It also contains unrelated configuration and generated artifacts, so do not publish that commit wholesale. Local presence is not proof of safe remote backup or runtime activation.

Preserve those features separately in later source-only integration batches after reviewing their dependencies. Start from `docs/FAILURE_AND_CORRECTION_REGISTER.md`, `docs/VALIDATION_METHODS_AND_LEGACY_AUDIT.md`, `docs/AI_AND_CLOSING_PRICES.md`, and `docs/snapshot_cache_evidence_gate.md` in the original checkout. Runtime activation was previously unresolved; VIX policy suitability and ETF evaluation leakage remain open. This preservation task does not resolve those issues.

At inspection, the only uncommitted test change was this fixture; nested `pytorch` and `vision` repositories were dirty and deliberately untouched. No loose production-source or dependency/configuration change was present. Generated/runtime artifacts, `.env`, credentials, environments, databases/WAL/SHM, bytecode, exports, logs, screenshots, temporary directories, and model binaries are excluded from this preservation commit. The original test change remains uncommitted on the original branch because its index and branch are deliberately not advanced by this independent snapshot.
