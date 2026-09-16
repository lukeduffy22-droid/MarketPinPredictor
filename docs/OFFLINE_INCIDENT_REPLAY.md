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
