# ORB read profiling and covering index

The live ORB route has a one-second fail-closed deadline. On September 16,
2026, read-only profiles at the same fixed cutoff measured SPX at 2.145/1.556
seconds and NDX at 1.295/0.981 seconds. These are standalone cProfile timings,
not endpoint latency or a statistically meaningful latency distribution.
The first profile spent 1.751 seconds (SPX) and 1.139 seconds (NDX) in the
reference loader. SQLite metadata fetching dominated; the old query plan
used the symbol/date/time index, table lookups, and a temporary tie-break sort.

`idx_orb_reference_snapshot_metadata` covers the metadata query's filters,
timestamp/sample-ID ordering, and selected provenance fields. It avoids
thousands of accesses to the table containing the large formula-input audit
payload. The query, as-of filters, retained opening rows, first row per
provenance identity, latest row, and final eligibility checks are unchanged.
No missing opening bucket is reconstructed. Research-only authority remains.

New tables receive the index through SQLAlchemy metadata. Existing databases
require explicit installation; startup does not launch an unbounded index build.
No backend restart is required solely to make a SQLite index available.

## Reproduce and deploy

Run from the canonical repository with its virtual environment. Store output
outside the repository; output files must not already exist.

```powershell
& .\.venv\Scripts\python.exe -B tools\profile_orb_reads.py --database data\market_data.db --output C:\path\orb-before.json
& .\.venv\Scripts\python.exe -B tools\install_orb_read_index.py --database data\market_data.db
```

The profiler enforces SQLite `mode=ro` and `query_only`, starts no feed, and saves
the cutoff, complete snapshots, stage timings, cProfile results and query plans.
It still consumes read I/O; avoid running continuous profiles during ingestion.

The installer defaults to inspection. With `--apply`, it waits at most 100 ms
for a writer lock and uses a SQLite progress callback to interrupt index creation
at its build budget. Busy/interrupted builds roll back and return `deferred`
(exit code 2). The budget is checked periodically, not a hard real-time bound
on OS I/O, commit, or rollback. Do not repeatedly retry deferred live builds.

During a maintenance window with recording writes quiescent, allow a larger
budget (maximum 30 seconds):

```powershell
& .\.venv\Scripts\python.exe -B tools\install_orb_read_index.py --database data\market_data.db --apply --budget-seconds 30
& .\.venv\Scripts\python.exe -B tools\profile_orb_reads.py --database data\market_data.db --output C:\path\orb-after.json --as-of '<exact as_of_utc from orb-before.json>'
```

Compare complete snapshots at the same cutoff, confirm `COVERING INDEX
idx_orb_reference_snapshot_metadata` and no metadata temporary sort, then check
live single-symbol and combined endpoint latency. A faster read must still
return unavailable for incomplete opening evidence. Also verify feed advancement
and current-generation persistence; HTTP 200 alone does not establish readiness.

## September 16 deployment result

The one-second live installation attempt was interrupted after 1.009 seconds
and rolled back. Subsequent inspection confirmed the index was absent. **Live
index benefit is not yet verified.** At 14:10 CT, health returned HTTP 200 and
messages advanced from 1,417,170 to 1,417,711 across checks. The combined ORB
endpoint still returned HTTP 503 `RUNTIME_READ_TIMEOUT` in 1.001 seconds.
No restart was performed.

Evidence is under
`C:\Users\LukeD\Documents\Codex\marketpin-live-progress-20260916`:
`orb-profile-before.json`, `orb-profile-before-repeat.json`, and
`orb-index-deferred-runtime.json`.

Validation: `tests/test_orb_read_index.py` plus
`tests/test_market_structure_orb.py`: 50 passed. Tests cover unchanged full-day
projection, provenance retention, query-plan coverage, idempotent installation,
busy-writer deferral, interruption rollback, and retained evidence preservation.

## 14:16 CT live installation (supersedes deferred status above)

After the owner requested live remediation, a date-scoped five-second attempt
also interrupted and rolled back. The full index was then installed with a
30-second budget, completing in 14.337 seconds without restarting the backend.
This was not interruption-free: the immediate health check returned 503,
and reference sample timestamps jumped from 19:16:00 UTC to 19:16:20 (SPX)
and 19:16:25 (NDX). Logs retained database-lock/deadline failures. These gaps
must remain fail-closed; they must not be backfilled. Subsequent rows resumed.

The same-cutoff standalone profile improved to SPX 0.911 seconds and NDX 0.248
seconds, with complete snapshots exactly equal to `orb-profile-before.json`.
The metadata query used the new covering index without a temporary sort.
However, live single-symbol and combined ORB calls still returned 503 around
one second. Empty-date and opening-minute requests returned 200 in 0.100/0.127
seconds. Thus query cost improved but endpoint recovery is **not verified**.
An experimental SQL window selection did not improve latency enough and was
reverted; the retained selection algorithm remains unchanged.

Before the build, health reported 1,650,951 messages; later checks advanced from
1,682,912 to 1,684,649. The epoch remained unchanged, with three queue-full
warnings, zero provider skipped records and zero reconnects before/after.
These transport counters do not erase the observed database persistence gaps.

Current focused validation: 54 passed across index installation, ORB projection,
and end-to-end rehearsal tests. The optional date-scoped installer works in
fixtures but did not finish within its live five-second budget. It is not
installed in the live database. No evidence eligibility or research boundary
changed.

Further evidence: `orb-profile-after.json`, `orb-live-after-index.json`, and
`orb-profile-sql-selection.json` (the rejected experiment).

The remaining runtime delay needs an in-process stack view. [py-spy](https://github.com/benfred/py-spy)
reads the running Python process without a restart. It is prepared in an
isolated uvx tool environment, not installed in the application environment.
Attaching from the agent session failed with Windows error 5, access denied.
Use an administrator PowerShell:

```powershell
& 'C:\Cprojectsgpu_app\MarketPinPredictor\tools\capture_live_orb_profile.ps1'
```

The script re-resolves and verifies the canonical port-8000 owner, records a
20-second nonblocking stack profile at 25 Hz including idle threads, and makes
bounded HTTP requests. It retains HTTP failures and profiler stderr outside
the repository. It does not request locals, restart the feed, or change data.
PowerShell parsing passed; elevated attachment remains unexecuted.

At 14:24:36 CT, the index remained present, messages reached 1,930,788 and
SPX/NDX reference persistence continued. Queue-full warnings had increased
from three to five; skipped-record and reconnect counters remained zero.
The cause of the additional warnings is not established. This is still a
degraded session, not a clean overload-acceptance result.
