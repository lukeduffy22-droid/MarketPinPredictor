# September 16 live-session engineering checkpoint

Source checkout: `C:\Cprojectsgpu_app\MarketPinPredictor`, branch
`codex/explain-repository-structure`, base `755b75a41`. Existing export, universe
sidecar and monitor edits were preserved; they are not all work from this task.

## Observed progress

- Opening failure diagnostics are committed in `b7b62468f` and observed in today's
  running process and `logs/opening_reference_attempts/2026-09-16.ndjson`.
- Copilot's incident packaging and concurrent ORB projection are in `755b75a41`.
  A successful `/health` fingerprint comparison at approximately 12:47 CT found
  the current ORB router matching its startup hash. Commit time alone had suggested
  it might not be active; the fingerprint supersedes that inference.
- Live `/health/live` and ORB requests returned intermittent 503
  `RUNTIME_READ_TIMEOUT` at 12:38–12:39 CT. One health sample showed SPX/NDX roughly
  92 seconds stale. A 12:48 CT sample subsequently showed advancing traffic,
  active handoff, calculation readiness and current persistence, with 13 cumulative
  queue-full warnings. This is recovery in a sample, not sustained acceptance.
- The 12:35 watchdog receipt deferred closing-tape launch for primary/ORB readiness.
  No new research scorecard or model promotion is claimed.

## Changes completed in this task

1. Added `import-opening` to the incident CLI, using a bounded read-only adapter
   for actual opening failure NDJSON. Counts cover every input receipt; examples
   are bounded per symbol/epoch/generation/reason. Source and line hashes bind
   derived examples to preserved originals. Unsupported rechecks remain explicit.
2. Imported today's 26,850,263-byte receipt file: 8,845 failures, 35 examples, zero
   contradictions among supported checks. NDX had 1,876
   `PROVIDER_TIMESTAMP_OUTSIDE_REGULAR_SESSION` attempts and one
   `COMPLETE_PAIR_MINIMUM_NOT_MET` attempt; SPX had 26 timestamp-session failures.
   These are failed attempts, not missing-bucket counts or valid-quote counts.
3. Reproduced an ORB candidate-selection defect for SPX and NDX: one old event,
   stale receive timestamp, or future receive timestamp could poison an otherwise
   eligible set because provider-time checks happened after pair ranking.
   Provider-time filtering now precedes ranking, with rejection counts preserved.
   Existing freshness thresholds, session bounds, mapping/generation checks,
   minimum-pair requirements and post-persistence eligibility decisions remain.
   This fixes the reproduced selection behavior; retained summaries alone cannot
   establish how many historical failures had enough alternative eligible pairs.
4. Updated the existing ORB end-to-end rehearsal to bind the router's shared clock
   to its simulated journal date. Otherwise the concurrent projection reads the
   real current date against September 8 fixtures and falsely reports zero rows.

Validation: 195 focused tests passed, covering quote selection, opening stress,
streamer regressions, ORB end-to-end rehearsal, incident import/replay, and bounded
health concurrency. Tests use temporary databases. Research-only authority and
immutable historical records were preserved.

Local evidence directory (not Git):
`C:\Users\LukeD\Documents\Codex\marketpin-live-progress-20260916`.
The original source SHA-256 is
`5c9090d7c7804051f567b10bb5e841285c739adc0edb5e00cf3ae1bea059c818`.

## Activation still required

The importer was executed successfully during live hours. The new sampler code
has not been loaded into backend PID 632. The repository's read-only
`Test-MarketAppVerifiedProcess` returned false for that PID from this session;
no restart or ownership-guard bypass was attempted. Other source edits continue
in the shared checkout, so activation must cover their reviewed state as well.

From an appropriately privileged session, re-resolve the listener, verify its
canonical ownership, review the complete source delta, and use the guarded
`ensure_market_app.ps1 -RestartBackend -ExpectedBackendPid <verified-current-pid>`
path. Then verify the new loaded hashes, new epoch, progressing transport and
SPX/NDX persistence, eligible ORB samples, bounded API latency, and overload deltas
across multiple persistence intervals. Preserve the earlier epoch and failures.
An intraday restart cannot repair this morning's missing opening buckets.

## 13:33 CT restart and recovery correction

The manual command omitted `-EnableRutCanary`, selecting the default three-symbol
cache rather than the four-symbol profile used by the scheduled tasks and original
backend. The verified stop completed, but replacement startup failed with
`cache_only_result_invalid`. The watchdog then validated the existing four-symbol
current-day cache and launched a replacement at 13:36:09 CT. At 13:37:20 CT the
backend responded with an active handoff, 2,337 received messages, a new epoch,
and zero loaded-source hash mismatches; calculation readiness was still false.

For this configured deployment, a guarded manual restart must preserve
`-EnableRutCanary`. Verify the currently configured task arguments before future
recovery; do not assume the launcher's default profile matches the running one.
Do not repeat the earlier command without that flag. Recovery alone does not
erase the interruption or prove sustained readiness.

At 13:38:57 CT the replacement had received 85,100 messages (up from 2,337),
SPX and NDX were valid/current-epoch and 2.5/3.5 seconds old, and their persisted
calculation timestamps had advanced to 13:38:36/13:38:43 CT. Health calls completed
in 78/47 ms. The ORB collection still returned HTTP 503 in 1.015 seconds; one
new-epoch queue-full warning remained, with zero reported skipped records and
reconnect attempts. VIX remained invalid for insufficient primary strikes.
Backend recovery and activation are observed; ORB API and overload acceptance
remain open. The prior epoch's warnings and restart interruption remain evidence.
