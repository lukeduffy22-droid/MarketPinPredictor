# Offline Incident Replay

The incident exporter packages explicitly selected, sanitized JSON evidence. It does not connect to Databento, open the live database, mutate source files, or grant production authority.

Each source record must use `marketpin-incident-evidence.v1` and include an aware observation timestamp, symbol, subscription epoch/generation when known, validation method, explicit missing-evidence reasons, and named pass/fail checks. Failed checks and missing evidence deterministically produce the replay rejection reasons.

Write packages to a runtime directory outside this repository:

```powershell
.\.venv\Scripts\python.exe -B tools\incident_replay.py export `
  C:\runtime\incidents\opening.json C:\runtime\incidents\closing.json `
  --destination C:\runtime\replay\2026-09-14

.\.venv\Scripts\python.exe -B tools\incident_replay.py replay `
  C:\runtime\replay\2026-09-14
```

Export rejects credential-like fields. It copies each source byte-for-byte and records its SHA-256 and size in a canonical manifest. Replay verifies those bytes and recomputes rejection reasons using only packaged evidence.

## Timeout Regression

The focused concurrency test models four ORB symbol projections delayed by 60 ms each while health is requested concurrently. The prior serial critical path is approximately 240 ms and exceeds the test's 150 ms bounded-read budget. The concurrent projection acceptance threshold is below 150 ms, with both `/v1/orb` and `/health/live` returning HTTP 200 and unavailable symbol state remaining explicit.

Run the focused offline checks with:

```powershell
.\.venv\Scripts\python.exe -B -m pytest tests\test_incident_replay.py tests\test_health_async_isolation.py -q -p no:cacheprovider
```

These figures are deterministic test workload parameters, not measurements from the live September 14 session. Live acceptance remains required before closing the incident.

## Import actual opening receipts

`import-opening` reads the retained `orb-reference-failed-attempt-v1` NDJSON without
opening the live database or connecting to the feed. It counts every failure,
separates symbol/epoch/generation/reason groups, and packages at most two examples
per group by default. Original files remain unchanged. Each derived example binds
the original file and line with SHA-256 and labels its conversion method separately
from the producer schema and rejection reason.

```powershell
.\.venv\Scripts\python.exe -B tools\incident_replay.py import-opening `
  logs\opening_reference_attempts\2026-09-16.ndjson `
  --destination C:\runtime\incidents\opening-20260916
.\.venv\Scripts\python.exe -B tools\incident_replay.py replay `
  C:\runtime\incidents\opening-20260916\package
```

Clock counts/thresholds, handoff state, and complete-pair counts support independent
checks of the associated producer rejection. A contradictory reason adds
`PRODUCER_REASON_EVIDENCE_CONFLICT`; unsupported or missing checks retain
`MISSING_EVIDENCE:independent_gate_recheck_unavailable`. This verifies retained
diagnostics, not raw quote replay, session completeness, or model accuracy. All
outputs remain research-only and unusable for prediction. Inputs over 64 MB,
incomplete final lines, and files changing during import fail before export.
