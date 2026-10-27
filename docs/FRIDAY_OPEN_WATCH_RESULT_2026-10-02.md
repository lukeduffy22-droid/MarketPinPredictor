# Friday opening watch result — October 2, 2026

**Result: live acceptance failed.** The deployed repairs passed offline regression tests, but the morning session did not establish reliable SPX/NDX forecasts, complete opening ranges, or recorder capture. All times below are America/Chicago (CDT).

The final endpoint/database observation was **09:53:38**, after the requested 09:50 checkpoint. The scheduled 09:20 heartbeat executed late, with its first observation at 09:46; evidence filenames retain the scheduled slot but their embedded timestamps are authoritative. A final bounded supervisor-log read at 10:01 confirmed the same recorder deferral through 10:00. This was sampled monitoring, not continuous coverage.

## What the morning checks established

| Check | Observed result |
| --- | --- |
| Current-day contracts | The 07:00 and 07:15 preparation tasks succeeded. The runtime adopted the October 2 universe: 2,690 selected contracts from 22,188 source rows. Cache SHA-256: `fdd9b3c32d5d08ff08eb689916a21cac7e17ca046ef1f1313081786734ebd8da`. |
| Source and services | Tested source hashes still matched at 09:46. Backend, dashboard and research endpoints responded; the final reads took 15–63 ms. This establishes responsiveness, not prediction readiness. |
| Provider transport | Messages increased to 300,675, but the final runtime was generation 28 with 84 reconnect attempts, 72 slow-client/skipped-record warnings and 31 queue-full warnings. Receive-to-process lag briefly improved to 0.68 seconds while compute backpressure remained active. Warning counters do not quantify lost records. |
| SPX and NDX predictions | Both prediction endpoints returned HTTP 503: no passport-backed lifecycle prediction available. SPX was about 280 seconds old and NDX about 262 seconds old; both calculations belonged to generation 24 while the active generation was 28. The prediction pipeline remained false. |
| Exact opening bucket | All four symbols rejected the exact 08:30:00 bucket with `PROCESSING_CLOCK_NOT_SYNCHRONIZED`. The authoritative `orb_cbbo_reference_samples` table had no exact first-bucket rows. Initial retained rows began at 08:30:20 for SPX/NDX/VIX and 08:30:25 for RUT. Later samples cannot repair the missing opening observation. |
| Opening-range coverage | First-bucket, 5-, 15-, 30- and 60-minute checks all failed. At the final read, 60-minute capture ratios were SPX 13.33%, NDX 11.94%, VIX 1.39% and RUT 10.97%, with missing-first-bucket, incomplete-coverage and runtime/range-identity reasons. These ranges must remain partial/unverified. |
| Persistence | Bounded read-only observations showed intermittent advancement over intervals longer than two minutes. The final latest calculation/audit was at 09:50:29, latest saved prediction at 09:49:17 in generation 24, and latest market row at 09:50:29. Earlier valid saved rows do not establish current forecast readiness. |
| Recorder | No October 2 recorder directory, status or DBN existed at 09:53. The 09:35, 09:45 and 09:50 watchdog runs deferred launch because the primary capture was unsafe; the 09:55 and 10:00 log entries repeated this result. No byte/record advancement, callback-drop count, queue behavior or full-session throughput could be verified. Capture is not complete. |
| Indicators and optional symbols | The historical-poll collector reported `ENTITLEMENT_UNAVAILABLE` with zero accepted bars, separately from the previously denied direct-live EQUS.MINI connection. This does not imply all historical access is denied. RUT and VIX independently failed chain breadth at the final read (22 and 2 primary-expiration strikes, respectively; required minimum 30). |
| Earlier timeout/Arrow symptoms | Sampled runtime reads were fast and did not reproduce `RUNTIME_READ_TIMEOUT`. Dashboard stderr remained empty. Repeated provider gateway timeouts and feed pressure persisted; these are separate from the repaired bounded-read/UI issues. |

The provider clock synchronized after messages arrived but reset during reconnects. Host-clock shadow diagnostics are separate, do not control live admission, and their uncertainty estimate is not measured absolute clock offset.

## Recovery boundaries and unresolved work

The existing scheduled supervisor replaced the backend at 07:46:52, before cash open, after inactive-handoff readiness checks. Backend listener 53028 remained thereafter; dashboard 31796 and research 56568 stayed in place. The preopen recovery trigger needs review: those handoff failures alone did not prove the old backend had failed. Later recovery was deferred with `protected_opening_boundary_reached`.

Current OS queries could not read the elevated backend's executable or command line, even with sandbox escalation. PID/parent/creation-time receipts and loaded fingerprints supported continuity, but full ownership could not be independently rechecked. No manual restart, process kill, extra provider connection, subscription change, replay, model promotion, threshold/formula change or market-data repair was performed by this watch. Existing dirty code, DB/WAL, tapes, opening gaps and immutable receipts were preserved. The final log shows a transient research-supervision failure at 09:50, followed by `READY / RESEARCH_OWNED_ENDPOINT` at 09:55 and 10:00.

Remaining actions are:

1. Diagnose and repair the recurring ingestion/reconnect/queue-pressure path offline, then activate through the canonical guarded process during an appropriate window after verifying ownership and coordinating with the supervisor. A stable, fresh, valid, current-generation SPX/NDX calculation and passport-backed forecast must be observed before declaring recovery.
2. Keep the recorder's primary-readiness gate intact. When normal scheduling can safely launch it, verify its owned process, fresh status, growing bytes/records, callback drops and queue behavior over a bounded interval. The overnight recorder pause fixes remain unproven under live capture load.
3. Resolve the indicator collector's current-session access failure without assuming older historical retrieval establishes entitlement; then observe fresh accepted bars. No entitlement/subscription change was made.
4. Review preopen handoff recovery and clock readiness for the next session. Preserve today's missing exact bucket; do not backfill it or relabel partial ranges complete.
5. The 06:30 official-close catch-up failed DNS resolution for `cdn.cboe.com`. DNS later resolved, but this watch did not retry retrieval. The October 1 settlement receipt remains retained. The 32 older ledger rows still require original-artifact recovery or reviewed append-only adjudication.

## Evidence and watch closure

Evidence is under `output/session_repair_20261002/`: `watch-0720-*` through `watch-0920-*`, `watch-0835-cbbo-opening.json`, `watch-0835-first-bucket-attempts.json`, `watch-late-recorder-gate.json`, `watch-late-ownership-recorder.json`, `watch-final-0950.json`, `watch-final-supervisor.json` and `watch-final-summary.json`. The legacy `orb_reference_samples` table is not the source for current CBBO opening evidence.

The bounded Codex heartbeat `verify-friday-marketpin-opening-and-recorder` was **deleted successfully after the final check**, including though blockers remain. The existing Windows watchdog was left unchanged and was observed running through 10:00. No further watch was created. Offline test results and activation receipts remain in `SESSION_ERROR_REPAIRS_2026-10-02.md`; this result supersedes its earlier pending-live-verification status.
