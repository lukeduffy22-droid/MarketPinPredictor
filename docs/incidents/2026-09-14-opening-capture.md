# MP-2026-09-14-OPEN: missing opening reference buckets

Status: OPEN. Observed during read-only diagnosis beginning 2026-09-14 14:16 CT. A prevention checklist and durable incident record have now been added; no capture-code correction or successful future-opening verification is claimed.

## Confirmed facts

Expected opening hour: 08:30–09:30 CT, 720 five-second buckets. Read-only SQLite samples joined to final eligibility decisions exactly matched the user's table:

| Symbol | Eligible samples | First bucket CT | Missing opening buckets | Maximum subsequent gap |
|---|---:|---|---:|---:|
| SPX | 717 | 08:30:15 | 3 | 5 seconds |
| NDX | 707 | 08:31:05 | 13 | 5 seconds |
| RUT | 701 | 08:31:35 | 19 | 5 seconds |
| VIX | 717 | 08:30:15 | 3 | 5 seconds |

All continue through 09:29:55 CT. The exact opening bucket is absent for every symbol. This explains partial status for all ORB windows even with 97–99% row coverage. These are derived parity-reference samples, not counts of raw OPRA messages. VIX is forward-like option context, not spot-breakout evidence.

Logs show:

1. 07:55:29 CT: initial live subscription established.
2. First 08:30:00 bucket attempts: PROCESSING_CLOCK_NOT_SYNCHRONIZED.
3. Retried first-bucket attempts: HANDOFF_NOT_ACTIVE.
4. NDX/RUT subsequently: COMPLETE_PAIR_MINIMUM_NOT_MET.

The source clock classifier distinguishes unknown/insufficient samples from unsynchronized timing, but this rejection reason alone does not. A Windows clock fault is not proven. The specific readiness gates are confirmed; the deeper clock/handoff transition remains unresolved.

## Correction record

Implemented now: documentation, evidence preservation, and explicit premarket/opening acceptance checks in [the checklist](../PREMARKET_EVIDENCE_CHECKLIST.md).

Pending engineering: reproduce the opening transition in isolated replay; establish adequate clock/handoff/definition/pair evidence without weakening gates; preserve raw inputs independently of feature acceptance; retain detailed per-bucket failures; determine whether genuine opening input was present and recoverable.

Verification required: exact first bucket and sustained progression for required symbols in a future eligible session, with zero unexplained gaps and stable provenance. If required exchange quotes are unavailable, retain an honest partial status. No system can manufacture a legitimate opening observation from absent quotes.

Related open issues: ORB bounded read timeout (HTTP 503 / RUNTIME_READ_TIMEOUT) observed in API and running UI; Advisor source changes not activated in the inspected running dashboard. Neither is established as the cause of this opening gap.

Evidence: [bucket summary](evidence/2026-09-14-opening-summary.json), [opening log excerpts](evidence/2026-09-14-opening-failures.txt). Full original receipts remain in the Codex task outputs for open-the-latest-marketpin-build-plan under 2026-09-14.
