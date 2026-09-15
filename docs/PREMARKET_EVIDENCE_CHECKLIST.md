# Premarket and opening evidence checklist

Final-session requirement: also review [late-session capture](incidents/2026-09-14-close-capture.md). During the last 15 minutes, compare valid snapshots, failed attempts, queue pressure and handoff progression. At close, verify coverage independently from official-close outcome provenance. A last gamma pin is not an official close.

Operating timezone: America/Chicago. Regular-session examples below assume an 08:30 CT open; verify the exchange session/holiday schedule first. This checklist is a manual operating requirement, not an installed scheduler or an automated alert.

Read [the failure register](FAILURE_AND_CORRECTION_REGISTER.md) before each session. For every check record PASS, FAIL, or NOT YET VERIFIED with its observation timestamp and evidence path. Do not carry yesterday's PASS forward.

## Before open: target 08:15, recheck near 08:25 CT

- [ ] Verify canonical checkout and process owners; preserve dirty source, databases, WAL/SHM, and capture files.
- [ ] Verify backend and dashboard are reachable, but do not equate this with capture readiness.
- [ ] Verify current session definitions, selected contracts, subscription acknowledgement and current epoch/generation for required SPX and NDX. Record optional symbols separately.
- [ ] Inspect processing-clock status AND sample count/negative-lag diagnostics. Unknown timing evidence is not a synchronized clock; a failure reason alone is not proof of a Windows clock fault. Do not force a passing flag.
- [ ] Verify handoff state and advancing traffic where market data is actually available. Distinguish premarket inactivity from an outage; explicitly mark opening readiness unproven when dependencies await opening quotes.
- [ ] Check contemporaneous complete put/call pairs per required symbol, freshness, and definition identity. Never substitute yesterday's or later prices for missing opening quotes.
- [ ] Verify writable evidence destinations, adequate free space, recorder/persistence progression where available, and absence of current locking/queue/reconnect failures.
- [ ] Verify raw capture retention separately from feature eligibility. Do not assume rejected reference samples imply that raw inputs were retained.
- [ ] If any prerequisite is unresolved, escalate before the open and record its exact reason. Starting a process earlier is not a sufficient corrective action: on September 14, subscription began at 07:55:29 but the opening bucket still failed.

## At the opening transition

- [ ] Verify the 08:30:00 CT five-second bucket was committed and eligible for each required symbol. Record first bucket, capture time, identity, and final decision.
- [ ] If absent, retain per-attempt clock, handoff, pair-count, deadline and persistence reasons. Mark incomplete coverage immediately; do not wait for a misleading high overall percentage.
- [ ] Check first-minute progression and then 5/15/30/60-minute coverage. Confirm first/last boundaries, maximum gap and provenance alignment, not merely row counts.
- [ ] Keep incomplete windows partial. Any research reconstruction requires genuine point-in-time raw records, a reproducible procedure, and a distinct reconstruction label. It cannot retroactively become contemporaneous live capture.

## Release and UI verification

- [ ] Verify new functionality in the running dashboard, not just source/tests.
- [ ] Preserve unsaved selections before a dashboard-only restart when activation is needed and authorized. Verify ownership and use the guarded launcher. Do not restart the backend merely to activate UI changes.
- [ ] Confirm new UI behavior and that backend identity and capture progression remain intact afterward.
- [ ] Treat ORB endpoint timeout separately from feed health. Retain HTTP reason/latency; do not infer missing capture from a failed read or increase timeout thresholds without diagnosis.

## Session record

Save dated evidence under the application's operational output directory. Update the register with checks performed, failures, implemented changes, deployment proof, and remaining gaps. An incident is not closed by checking this document alone.
