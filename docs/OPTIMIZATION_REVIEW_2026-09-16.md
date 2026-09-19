# September 16 optimization review

Checked around 22:15 America/Chicago. Source validation and observed runtime
behavior are separate. No stored market evidence was rewritten.

## Existing changes checked

- The ORB covering index is present and used by the retained-data query plan.
- Backend startup fingerprints show health worker isolation and the 15:15
  collection cutoff loaded at 17:12. This does not prove post-close delivery.
- At 22:09, concurrent full-day SPX/NDX reads returned 503 near one second while
  health endpoints returned 200 in 18-20 ms. Later individual ORB requests returned
  200 in 128-163 ms; another overlapping batch again timed out. Health isolation
  is observed off-hours; ORB concurrency remains a runtime failure in that build.
- The canonical cache-metadata writer uses explicit UTF-8 bytes to avoid Windows
  newline translation invalidating its own strict metadata check.

## Additional changes prepared

ORB endpoints now share one dedicated bounded worker. Multi-symbol projections
execute serially inside it, retaining the same runtime binding, deadline,
fail-closed responses, and no completed-result cache. Generic projections and
health use their existing separate admission pools. Saturation can still return
503; this is not an unbounded queue or a guarantee of live capacity.

An isolated ASGI check of the new source against the real database in read-only
mode returned 200 for three overlapping requests in 0.103, 0.178 and 0.349 seconds.
Every complete SPX/NDX snapshot matched its serial reference at the same cutoff.
No provider client or second backend was started for that check.

An additional ORM-to-Core rewrite was measured and discarded: alternating warm
runs showed only a small benefit (SPX median 98 to 93 ms, NDX 77 to 77 ms). The
large difference in initial profile runs was not a valid sustained-speed claim.

Derived gamma-wall tables now distinguish absent calculated sides and unverified
legacy zeros from zeros backed by calculated-contract counts. Missing DTE and
expiration counts remain unknown. Gross GEX is not inferred from absolute net
GEX. Counts cover calculated rows only and do not establish full chain coverage.
Raw producer records and analytical arithmetic remain unchanged.

Expiration tables identify their denominator as planned contracts, leave active
subscription unverified, and withhold freshness for absent profiles. These labels
do not activate deferred subscriptions or relax admission gates.

## Validation and remaining boundaries

Focused tests cover serial request admission, unchanged runtime binding, health
responsiveness, missing versus calculated zero, CSV agreement, planned counts,
and unavailable profile freshness. The broader affected regression set passed:
429 tests in 30.73 seconds, including opening, index, cache, export and post-close
checks. Changed source parses and `git diff --check` passes.

New source still requires guarded backend activation and dashboard refresh.
Live verification must measure concurrent endpoint latency, advancing current
epoch SPX/NDX evidence and persistence, and overload deltas. Exact missing opening
buckets remain missing. Post-close delivery needs an observed session crossing
15:00-15:15, with predictions frozen and collection stopping at its cutoff.

Full per-contract expected/active/fresh/IV/exclusion lineage and independent future
expiration research admission remain separate work in
`DATA_VALIDITY_AND_EXTENDED_HOURS_PLAN.md`; this review does not mark them complete.

Measurements are retained outside the repository in
`C:\Users\LukeD\Documents\Codex\marketpin-live-progress-20260916`:
`orb-health-optimization-check.json`, `orb-serialized-asgi-check.json`,
`orb-optimization-alternating.json`, and `orb-concurrency-comparison.json`.
