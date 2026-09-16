# ADR-002: Bounded Concurrent ORB Projection

## Status

Accepted, pending live-session validation

## Decision Drivers

- Keep ORB and health reads within the existing one-second runtime budget during persistence.
- Never present a cached or stale result as current.
- Preserve existing freshness, pair-count, epoch, generation, and handoff gates.
- Avoid changes to the live streamer while it has a single designated owner.

## Options Considered

1. Increase API timeouts. Rejected because it hides contention and delays explicit unavailable responses.
2. Cache the last successful ORB response. Rejected because an old response could appear current after a runtime identity change.
3. Add a new bulk database projection. Deferred because it expands the persistence contract and migration/test surface.
4. Project symbols concurrently against one timestamp. Selected as the smallest change that removes serial per-symbol latency while preserving every existing read and validation rule.

## Decision

The multi-symbol live ORB endpoint uses at most four short-lived worker threads to call the existing snapshot projection. Every symbol uses the same aware `as_of_utc`. Runtime identity is checked before and after the batch, with the existing retry and fail-closed unstable-context result unchanged.

The outer bounded-runtime worker pool still limits API work and returns explicit `RUNTIME_READ_BUSY`, `RUNTIME_READ_TIMEOUT`, or `RUNTIME_READ_FAILED` responses. No timeout is increased and no result is cached.

## Consequences

- The default four-symbol endpoint no longer accumulates persistence latency serially.
- SQLite may see up to four concurrent read connections for one ORB collection.
- A live-session measurement is still required; if concurrent SQLite reads contend materially, the next option is a read-only bulk projection with one database snapshot.
