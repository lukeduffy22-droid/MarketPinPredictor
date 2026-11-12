# Retained-session inventory and recurring observations

Read-only inspection on September 17, 2026. Sources: `logs/audit/<symbol>/*.json`
and `logs/capture_attempts/*.ndjson`. Counts below are retained attempt records,
not unique market events, complete-session coverage, or training-quality claims.

34 capture dates were discovered: January 5–9; July 15–17 and 20–22;
August 17–21, 24–28 and 31; September 1–4, 8–11 and 14–17. Availability differs
by symbol. No assertion is made that all original records survived retention.
The configured `data/diagnostic_reviews` archive contained no saved AI reviews.
Observations can still be analyzed retrospectively; an older AI write-up cannot
be recovered from raw observations as if it were the original review.

| Session | SPX valid / attempts | NDX valid / attempts | VIX valid / attempts | SPX final-15-minute valid / attempts |
|---|---|---|---|---|
| 2026-09-10 | 357 / 387 | 369 / 371 | 0 / 370 | 1 / 16 |
| 2026-09-11 | 316 / 322 | 309 / 309 | 0 / 310 | 0 / 0 |
| 2026-09-14 | 360 / 392 | 372 / 379 | 0 / 374 | 1 / 13 |
| 2026-09-15 | 18 / 43 | 23 / 31 | 0 / 29 | 0 / 8 |
| 2026-09-16 | 344 / 369 | 342 / 363 | 0 / 361 | 12 / 19 |
| 2026-09-17 | 359 / 386 | 373 / 377 | 0 / 374 | 1 / 15 |

Useful retrospective investigations:

1. SPX late-session pair freshness/coverage: recurring weakness on September 10,
   14, 15 and 17, with a different outcome on September 16. Align source version,
   subscription identity, quote ages, chain membership and failed calculations
   before attributing a cause. Never weaken a gate based on counts alone.
2. VIX: zero valid attempts in all six inspected sessions. Separate sparse primary
   expiry, contract-selection problems and gate suitability from transport health.
3. Coverage gaps: September 11 has no retained attempts in the final 15 minutes;
   September 15 has far fewer records. Missing records are not proof that all
   attempts failed and cannot be filled from later observations.

AI should review these compact comparisons first, then inspect exact retained
records for the leading hypotheses. Fix proposals must name the evidence,
affected code, focused regression tests and future-session acceptance criteria.
No underlying capture fix or improvement in forecast accuracy is established here.
