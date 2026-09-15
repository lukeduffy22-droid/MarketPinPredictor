# Deterministic snapshot cache gate

The September 14 build plan's first gate is repeatable snapshot selection across
three reruns and one cold restart. The dashboard already caches built-in tuples
and dictionaries and reconstructs `SnapshotSelection` outside `st.cache_data`.
`tools/audit_snapshot_cache.py` verifies that boundary with real Streamlit caching.

Run from the canonical checkout using the canonical environment and fixed retained
NDJSON exports (choose an explicit local date and IANA timezone):

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
& .\.venv\Scripts\python.exe -B tools\audit_snapshot_cache.py --exports-root exports --symbol SPX --local-date 2026-09-11 --timezone America/Chicago --output outputs\snapshot-cache-evidence.json
```

Exit 0 means this cache gate passed. Exit 1 means it failed; inspect the JSON's
`failure_reasons`, `runs`, and `worker_failures`. Empty or unreadable evidence does
not pass. Run the focused regression suite with:

```powershell
& .\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider tests\test_audit_snapshot_cache.py tests\test_app_snapshot_cache.py tests\test_snapshot_history.py tests\test_app_retained_context_contract.py tests\test_streamlit_launch_stability.py
```

The gate extracts the two actual cache helper definitions from `app.py`, including
their production cache settings, and runs them in an isolated `AppTest`. It does
not import the full dashboard. Initial selection plus three reruns must have one
total disk selection; a fresh Python subprocess must reproduce the same selection
after one disk selection. Each rerun reloads the snapshot module and verifies that
the reconstructed object uses the current class. Custom cached objects and
`cache_resource` substitutions fail.

Each run records the content-based `snapshot_id`, selected latest UTC timestamp,
row count, malformed timestamp count, source-file SHA-256 hashes, and provenance
counts. The ID represents the ordered selection and configuration, not an existing
forecast or passport ID. Absolute input paths, PIDs, and observation times do not
contribute to it. Candidate partitions that do not exist are also recorded, so a
new adjacent partition fails the fixed-input check. File hashes are checked before
and after every run; source hashes are checked across the audit.

Use retained files that remain fixed for the entire check. Appends or changed bytes
fail the gate even if the filesystem modification time and size stay the same.
If scanning very large files outlasts the production five-second cache TTL, warm
cache hits may not be proven and the gate fails conservatively.

All results remain `research_only=true`, `decision_grade=false`, with zero live
eligibility. Invalid and historical records retain their classifications. This
gate establishes cache reproducibility only; it does not validate live capture,
backend restart recovery, point-in-time features, model accuracy, or promotion.
Its cold restart is an isolated test process, not the live dashboard or feed.
No model tuning or database writes are performed.
