# Snapshot Export Evidence

Gamma snapshot exports are research evidence. A producer pass establishes only that the recorded calculation passed its declared local gates; it does not establish forecast accuracy, current-live eligibility, or production authority.

## Complete Archive

The end-of-day ZIP contains separate evidence groups plus `coverage-manifest.json`. The manifest reports, for SPX, NDX, VIX, and RUT:

- producer-pass record count;
- historical identity-unverified record count;
- failed or unproven record count;
- expected symbols with no retained records.

A validation-group NDJSON file is intentionally only one subgroup. Use the coverage manifest when measuring availability, failures, or abstention-like outcomes.

## Reproducibility Fields

New persisted snapshots include:

- `quote_freshness_limit_seconds`;
- `open_interest_provider_statistics_end_utc`;
- `replay_evidence_complete` and `replay_missing_evidence`;
- `transport_counter_scope=shared_databento_stream`;
- `transport_counter_semantics=cumulative_process_totals_not_symbol_counts`;
- an aware canonical `timestamp`, while `producer_timestamp_legacy` preserves the prior value.

Provider-staged universe caches retain definition and statistics cutoffs in a canonical metadata sidecar bound to the exact CSV SHA-256. Legacy or tampered sidecars do not block cache use, but their cutoff evidence is explicitly reported as missing or invalid.

Derived ZIP downloads redact machine-local `source_path` values and preserve content hashes. Original runtime records and cache files are not rewritten.
