# ADR-003: Universe Cache Provenance Sidecar

## Status

Accepted

## Decision Drivers

- Reproduce open-interest and universe evidence after a process restart.
- Preserve the existing CSV cache bytes and readers.
- Bind provider cutoff timestamps to the exact cached universe.
- Keep runtime evidence outside Git.

## Options Considered

1. Add metadata columns to every CSV row. Rejected because it duplicates session metadata and changes the established cache schema.
2. Infer provider cutoffs from the cache date or modification time. Rejected because neither proves the provider statistics cutoff.
3. Write a canonical hash-bound JSON sidecar. Selected because it preserves CSV compatibility and makes cutoff provenance independently verifiable.

## Decision

New provider-staged universe caches write a canonical metadata sidecar beside the CSV. The sidecar records its contract version, trading/source date, exact CSV SHA-256, and provider definition/statistics cutoffs. Current-day cache loading accepts legacy CSVs but labels metadata as `missing_or_invalid`; valid sidecars restore the exact cutoff fields.

The sidecar is runtime evidence under the existing ignored cache directory. It does not change model or production authority.

## Consequences

- New snapshots can retain exact provider OI cutoff evidence after restart.
- Existing caches remain usable but explicitly lack that evidence.
- A changed CSV invalidates its sidecar instead of inheriting stale provenance.
