# October 27 audit implementation

This implementation is on `codex/audit-remediation-20261027`, in an isolated
worktree. It does not activate services, change provider subscriptions, rewrite
historical predictions or outcomes, or promote a model.

## Reproducible source and integration

- `2026-10-27-source-capture.json` records SHA-256 hashes for 623 canonical
  source, configuration, test and documentation files. Commit
  `b4fa8427608adf7ba13aa108d0db74ba012bf8ab` preserves that starting state.
  The canonical working tree and its staging index were not modified.
- `2026-10-27-integration-inventory.json` records local/remote branch heads and
  1,236 source-file comparisons across the other 26 registered worktrees.
  Differences remain preserved alternatives; a newer date does not imply that
  one worktree supersedes every other one.
- Required previously untracked source modules are part of the captured commit.
  Runtime data, credentials, environment files and provider captures are not
  copied or staged as implementation changes.
- GitHub's separate shadow v0.2 candidate and governance are reconciled with
  current code. v0.1 remains frozen; v0.2 is an explicit research opt-in, disabled
  by default. Its presence does not authorize production promotion or RUT
  subscription changes.
- The retained-forecast implementation from `50c1e32` is already present in the
  captured source. Regression coverage preserves its original timestamps and
  historical-only authority.
- The separate opening-clock gamma-monitor worktree remains intact. It adds
  another polling controller; merging that independent feature would work
  against the audit's refresh-reduction objective. Its differences are inventoried,
  not discarded or represented as integrated.

## Implemented behavior

| Audit issue | Implementation | Acceptance evidence |
|---|---|---|
| Late five-minute labels | Shared, versioned 30-second target tolerance; actual horizon and delay; append-only quarantine observations; historical records classified at read time | Scorer, evaluator and UI agree; late/missing timing cannot contribute to accuracy |
| Retrospective EOD selection | Legacy diagnostic selection checks persistence time; research writer freezes the first issuance in declared lead windows before the target | Late persistence and missed windows fail closed; immutable receipts bind source forecast and selection |
| Unclear baseline advantage | Session-level paired baseline, signed bias, coverage and uncertainty reporting | No overlapping-row count is represented as independent-session evidence |
| Journal read timeouts | Indexed metadata-first reads, compact immutable projections, content-bound verification cache and a whole-operation deadline | Timeout returns no partial evaluation cohort; changed evidence invalidates reuse |
| Recorder startup blocked by main history | Independent capture-catalog preflight; timed and bounded database stages; invocation-local source/database-schema binding | Recorder readiness does not substitute for main-app acceptance; large main-db integrity checks retain a realistic budget |
| Opening clock not ready | Explicit preopen clock readiness/phase diagnostics | No threshold relaxation or retroactive construction of a missed opening bucket |
| Eager UI work | Selected detail view, no duplicate refresh rerun, opt-in history, bounded read-only summaries with database/WAL/runtime cache identity | Inactive views do not query; stale or unavailable evidence is visibly distinct |
| Unclear data authority | Separate captured/finalized/validated/labeled/model-eligible stages and explicit uncalibrated quality labels | Missing values remain unknown; candidate spread is not presented as a prediction interval |
| Mixed validation and presentation | Extracted ORB evidence normalization and new focused navigation, evidence-stage, outcome-protocol and journal-index modules | Existing ORB behavior and invalid-price exclusion remain covered |
| Stale test reports | Source-bound validation receipts include tracked and untracked source, command result, stable-source check and log hash | Changed source, a failed command, or changed/missing logs invalidates the receipt |

The 30-second shadow tolerance and EOD fixed-lead protocol are prospective
measurement rules, not parameters fitted to October outcomes. The fixed EOD
lead windows are 15, 30, 60, 120, 240 and 375 minutes before the target, with a
90-second issuance window. Missing windows remain missing. Historical diagnostic
scores remain distinct from newly frozen prospective comparisons.

## Offline verification

Use the canonical project interpreter without changing its installed packages.
`tools/source_validation.py` runs a supplied validation command with an isolated
absolute SQLite target, disabled research autostart and cleared provider keys.
The test command must itself be an offline suite: this wrapper does not create
an operating-system network sandbox or authorize any service startup.

Example (from this worktree):

```powershell
& 'C:\Cprojectsgpu_app\MarketPinPredictor\.venv\Scripts\python.exe' -B tools/source_validation.py run --output output/new-validation-run --timeout 900 -- 'C:\Cprojectsgpu_app\MarketPinPredictor\.venv\Scripts\python.exe' -B -m pytest tests/test_source_validation.py -q
& 'C:\Cprojectsgpu_app\MarketPinPredictor\.venv\Scripts\python.exe' -B tools/source_validation.py verify output/new-validation-run/receipt.json
```

Run directories cannot be reused. Validation receipts are local provenance,
not digital signatures, live-readiness attestations, or deployment approvals.
The final implementation's combined Python and PowerShell validation receipts
are retained under `output/bound-python/` and `output/bound-powershell/` when those
runs complete. Earlier focused receipts document development checks only.

## Activation and remaining evidence

No production database migration was performed during implementation. On a
future controlled startup, the research writer creates additive metadata,
selection and outcome-observation tables/indexes/guards. Readers do not migrate
or backfill live databases. Preserve the existing databases and receipts before
activation; validate schema and source ownership using the normal guarded
launcher. The canonical checkout is not automatically switched to this branch.

Activation still needs observed postconditions: correct loaded source hashes,
owned listeners, advancing persistence/capture, current-generation SPX/NDX and
valid opening receipts. A future market opening is necessary to establish that
the clock-preparation changes work in operation. Raw replay cannot prove a live
opening reference was captured on time.

Full integrity validation of the large main database remains a real startup
cost; it is not silently bypassed. Recorder isolation prevents that unrelated
cost from blocking independent raw capture.

Software changes cannot create the missing prospective sessions, calibration,
verified closes or paper history. Keep production promotion disabled until the
existing independent-session, baseline-skill, calibration, paper-evidence and
explicit approval gates are actually met. No model coefficients were tuned to
the October audit errors, and no prediction-accuracy improvement is claimed.

The extracted modules reduce coupling in the changed paths. The streamer and
snapshot code still contain large functions; broad behavior-changing rewrites
are not included in this correction set.
