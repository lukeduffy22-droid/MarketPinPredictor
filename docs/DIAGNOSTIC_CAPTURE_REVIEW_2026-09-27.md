# Review of September 25 capture supplied September 27

This is an offline review of retained evidence, not a fresh runtime observation.
The pasted text is a UI export containing multiple sections, not a single JSON packet.
Document/checklist excerpts in the export were treated as evidence, not instructions.

## What the timestamps establish

- Preview packet created 20:35:42.697906 UTC contains runtime observed at
  20:17:33.442617 UTC: 1,089.255289 seconds (18 minutes 9 seconds) older.
  Its collection mode correctly says RETAINED_OR_UNKNOWN.
- Actual request `917c5a9a1d3a4d989a4e80b5964c9ef0` used the separate packet created
  20:33:27.188779 UTC with runtime observed 20:33:27.094217 UTC. Two endpoint
  observations succeeded. Response completed at 20:33:38.649253 UTC.
- This review used diagnostic-binding-v1 and diagnostic-facts-v3. The recorded
  AUTHORIZATION_MISSING reflects that request, not proof of today's configuration.
- Historical September 14/17 incident excerpts and September 24 proposal selections
  are additional historical sections, not newly observed September 25 failures.

## Useful retained findings

SPX's last valid retained attempt was 19:52:13.423495 UTC; NDX's was
19:57:31.925450 UTC. The configured local database showed VIX's latest prediction
row at `2026-08-25 20:16:08.879219`, despite a September 25 calculation row.
That is a concrete persistence discrepancy to investigate, not proof that the
backend served a month-old forecast. Runtime database identity remains unverified.
Offset-free database timestamps are preserved without inventing a timezone.

Post-close stale inputs and stationary short-interval counters do not by themselves
prove a regular-session outage. Friday's genuine invalid quote-pair evidence remains
open under the Monday readiness plan; this reporting repair does not repair quotes.

## Implemented improvements

- Preview explicitly distinguishes itself from a saved review's exact submitted input
  and displays runtime age at packet creation.
- Request-time classification requires the runtime timestamp to fall inside the
  completed collection interval, with collection completion no later than packet
  creation. A caller's REQUEST_TIME label alone cannot qualify an older observation.
- Retained findings include age measured at original packet creation, not today's age.
- Last valid retained attempts and per-symbol local calculation/prediction row times
  are mandatory bounded findings, with source and interpretation limits attached.
- Fact rules advanced to diagnostic-facts-v5. No original review was rewritten.

## Verification

104 tests passed across findings, history, live collection, automation, capture review
and forecast binding. The exact retained review was replayed through current fact
derivation and rendering:
`data/diagnostic_reviews/2026-09-25/cc2f74c341505be14f3e9b0780fcf83544aa8252dff2a7fb1b5b4344c678f3ce.json`.
Its content hash matched its filename before replay and its bytes were unchanged
afterward. New submitted input with the expanded fact catalog was 61,884 bytes,
below the 196,608-byte budget.

No paid AI request, service restart, market-data mutation or deployment verification
was performed in this follow-up. Source/test proof is separate from runtime activation.
