# November 6 opening readiness: unresolved capture cause, retained diagnostics repair

Status: **the cause of delayed primary provider timing / NDX availability is not
established, and exact opening capture is not fixed or verified.** A separate,
reproduced evidence-loss defect is repaired offline in this remediation worktree.
No canonical source, historical record, configuration, database, WAL, service or
subscription was changed. No provider request or service restart was required.

The canonical runtime subscribed at 07:53:18 CT. November 6 retained 18 first-bucket
attempts for each of SPX and NDX: 13 rejected insufficient provider timing, followed
by five rejected handoff. Timing became synchronized with 446 samples around
08:30:03.96 CT; NDX still lacked a committed fresh observation through the last
attempt around 08:30:04.99. Both core instruments missed the exact first bucket.
Shared lifetime counters in the earliest generation-1 audits show zero unmapped
quote drops, timestamp-order failures, crossed quotes or negative receive lag.
The positive invalid-price counts cannot be assigned to NDX or the first five
seconds. Later cached calculation inputs overwrite earlier quotes.

The full retained 4,493,896-record DBN tape was scanned and hash verified read-only.
It is TCBBO from a separate recorder started at 09:35 CT and replaying provider
records from 08:30. It is not the primary CBBO-1s SDK receipt stream. Its absence
of NDX trades before 08:30:15.343 CT cannot prove absence of earlier CBBO updates.
Databento documents CBBO as interval sampling of both quote updates and trades;
see https://databento.com/docs/schemas/bbo . No historical quote was backfilled or
relabeled as an original opening observation.

Native DBN fixtures containing 2,114 mapping messages and 100 CBBO records exercised
the actual reader, clock collection and handoff in approximately 70 ms under a
profiler. This is a synthetic mechanism benchmark, not a replay of original host
arrivals or proof that valid opening NDX quotes existed. Clock collection already
precedes mapping/price admission; previous-day last-sale metadata is already
permitted by the versioned optional-last-sale policy. Neither is a new causal fix.

The proven evidence-loss defect is that the generation-local first receipt,
mapping, cache-admission and first rejection diagnostics were exposed in current
health but omitted from early opening-failure receipts. Reconnection replaces
that observer, so later health cannot recover generation 1. The repair copies
its existing diagnostic snapshot into opening gate evidence, verifies its epoch
and generation, and includes explicitly shared, cumulative, non-atomic transport
counters. Existing failed-attempt persistence then retains these observations.
There is no new per-quote I/O or change to admission, clock authority, subscriptions,
freshness, pair requirements, generation checks, first-bucket deadline or coverage.

These milestones are the first ever in a connection and may describe replay;
their replay flags and semantics remain explicit. This repair does not create a
raw primary tape, socket-arrival times, per-family event counts or first-nonreplay
milestones. A missing or rejected diagnostic does not grant capture authority.

Regression tests must demonstrate the former missing fields, receipt retention
after reconnect, identity isolation, shared counter scope, unchanged unknown
clock rejection through 49 samples, synchronized timing still rejecting absent
NDX handoff, and no late first-bucket backfill. Existing reader, CBBO, transition,
handoff and ORB tests provide the surrounding offline verification. Exact commands,
source-bound test receipt, before-images, full executed evidence scripts and the
final result are saved outside application data under:

`C:/Users/LukeD/.codex/visualizations/2026/10/27/01a17f25-1b0c-7f63-b077-449c2bb3ff81/opening-readiness-remediation-20261106/`

The missing primary first-five-second receive/mapping/rejection sequence prevents
a causal November 6 provider-versus-local-delay conclusion. It cannot be recovered
from later latest-cache snapshots or a different-schema replay. Future acceptance
requires an authorized activation, genuine current-generation timing and valid
SPX/NDX inputs inside the unchanged first bucket, retained receipts, and sustained
window coverage. After-hours inactivity is expected and supplies no failure or
opening-success evidence. Keep this incident open until those observations exist.
