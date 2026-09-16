# MP-2026-09-14-OPEN: missing opening reference buckets

Status: OPEN. Observed during read-only diagnosis beginning 2026-09-14 14:16 CT. A prevention checklist and durable incident record have now been added; no capture-code correction or successful future-opening verification is claimed.

## Confirmed facts

Expected opening hour: 08:30–09:30 CT, 720 five-second buckets. Read-only SQLite samples joined to final eligibility decisions exactly matched the user's table:

| Symbol | Eligible samples | First bucket CT | Missing opening buckets | Maximum subsequent gap |
|---|---:|---|---:|---:|
| SPX | 717 | 08:30:15 | 3 | 5 seconds |
| NDX | 707 | 08:31:05 | 13 | 5 seconds |
| RUT | 701 | 08:31:35 | 19 | 5 seconds |
| VIX | 717 | 08:30:15 | 3 | 5 seconds |

All continue through 09:29:55 CT. The exact opening bucket is absent for every symbol. This explains partial status for all ORB windows even with 97–99% row coverage. These are derived parity-reference samples, not counts of raw OPRA messages. VIX is forward-like option context, not spot-breakout evidence.

Logs show:

1. 07:55:29 CT: initial live subscription established.
2. First 08:30:00 bucket attempts: PROCESSING_CLOCK_NOT_SYNCHRONIZED.
3. Retried first-bucket attempts: HANDOFF_NOT_ACTIVE.
4. NDX/RUT subsequently: COMPLETE_PAIR_MINIMUM_NOT_MET.

The source clock classifier distinguishes unknown/insufficient samples from unsynchronized timing, but this rejection reason alone does not. A Windows clock fault is not proven. The specific readiness gates are confirmed; the deeper clock/handoff transition remains unresolved.

## Correction record

Implemented now: documentation, evidence preservation, and explicit premarket/opening acceptance checks in [the checklist](../PREMARKET_EVIDENCE_CHECKLIST.md).

Pending engineering: reproduce the opening transition in isolated replay; establish adequate clock/handoff/definition/pair evidence without weakening gates; preserve raw inputs independently of feature acceptance; retain detailed per-bucket failures; determine whether genuine opening input was present and recoverable.

Verification required: exact first bucket and sustained progression for required symbols in a future eligible session, with zero unexplained gaps and stable provenance. If required exchange quotes are unavailable, retain an honest partial status. No system can manufacture a legitimate opening observation from absent quotes.

Related open issues: ORB bounded read timeout (HTTP 503 / RUNTIME_READ_TIMEOUT) observed in API and running UI; Advisor source changes not activated in the inspected running dashboard. Neither is established as the cause of this opening gap.

Evidence: [bucket summary](evidence/2026-09-14-opening-summary.json), [opening log excerpts](evidence/2026-09-14-opening-failures.txt). Full original receipts remain in the Codex task outputs for open-the-latest-marketpin-build-plan under 2026-09-14.

## 2026-09-16 source reproduction and evidence correction

Validation method: deterministic synthetic reproduction in the canonical checkout,
starting at commit `ad98b00d9`, using the real clock classifier, handoff transition,
reference calculation, and an isolated WAL SQLite journal. The two linked evidence
files above were absent from this checkout during this work. This reproduces the
documented rejection sequence; it is not a raw September 14 feed replay and does
not establish why quotes/readiness arrived late that morning.

Before the patch, four SPX/NDX regression cases failed because attempts exposed
only the rejection reason, without clock/handoff/pair evidence. Unknown clock
state (zero samples) was indistinguishable from measured skew in the failure
record. Repeated handoff failures could disappear from logs under warning
suppression and eventually from the bounded in-memory history.

The correction preserves the existing acceptance gates and within-bucket retry
behavior. Each attempt now carries clock sample counts, skew thresholds and
status, required/missing handoff families and observed ages. When those gates
allow evaluation, it also retains snapshot identity, universe/mapping coverage,
available quotes, complete-pair counts and quote rejection counts. Later gates
are explicitly unevaluated if an earlier gate fails. Scheduler timeouts whose
worker details are unavailable say `not_observed`, not that their inputs passed.

Every failed attempt during 08:30–09:30 CT is appended without warning suppression
to `logs/opening_reference_attempts/YYYY-MM-DD.ndjson`, including bucket, epoch,
generation, attempt timing and reason. These receipts are marked
`research_only_opening_diagnostic`, `usable_for_prediction=false` and
`validation_is_valid=false`; they never enter the ORB sample tables. A failed
receipt write exposes `failure_evidence_recorded=false` and an error log containing
the evidence. Successful samples still require the existing immutable eligibility
decision. No historical rows, formula, thresholds, model or promotion policy were
changed, and no services were restarted.

Focused tests in `tests/test_opening_transition_stress.py` exercise September 14
08:30 CT for each of SPX and NDX: unknown clock -> warming handoff -> eligible
exact-first-bucket commit; thin pairs -> zero accepted rows; and measured skew ->
zero accepted rows. Additional cases verify unsuppressed receipts, missing timeout
details, and explicit disk-write failure. Tests use temporary receipts/databases.

Observed validation: all 200 tests across opening-transition stress, Databento
streamer, ORB end-to-end rehearsal and market-structure ORB passed. The combined
run with Databento-only-mode tests reported 219 passed and one bounded health-read
timeout (`test_health_status_reports_databento_provider`, `RUNTIME_READ_TIMEOUT`).
The entire Databento-only-mode file then passed independently (20 passed). The
combined-run timing failure remains recorded; no health timeout was relaxed.

Reproduce with the repository venv and a temporary configured database:

```powershell
$env:DATABASE_URL = 'sqlite:///' + (Join-Path $env:TEMP ('marketpin-opening-' + [guid]::NewGuid().ToString('N') + '.db')).Replace('\', '/')
& .\.venv\Scripts\python.exe -m pytest tests\test_opening_transition_stress.py -q
```

Scope limit: this patch makes attempted opening failures diagnosable; it cannot
recover absent exchange inputs or reconstruct a bucket after its deadline. A
process that never dispatches an attempt cannot emit an attempt receipt. The
historical incident remains open pending independently observed exact-first-bucket
and sustained-opening acceptance in a future eligible live session.
