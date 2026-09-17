# Failure and correction register

This is the durable operational record for MarketPin. Record observed failures separately from proposed corrections, implementation, deployment, and verified outcomes. A test pass or a documentation change does not close a live incident.

Scope: same-day predictions first, then movement up to one week. Research-only output must not self-promote. Preserve original evidence and missing-data labels.

| ID | Failure | Correction status | Evidence / next acceptance gate |
|---|---|---|---|
| MP-2026-09-17-DIAG | AI packet mixed retained runtime context, current-day capture and historical incident excerpts; last-attempt validity could be mistaken for whole-session quality | OPEN — session-aware summaries and immutable review history implemented in source; runtime activation not verified | [September 17 evidence and follow-up](incidents/2026-09-17-diagnostic-session-review.md). Recheck SPX/NDX late-session pair coverage and VIX chain suitability; capture failures remain unresolved. |
| MP-2026-09-14-CLOSE | SPX valid gamma export ends 14:45:16 CT; failed audits continue through 14:59:48 CT | OPEN — source adds recording-attempt UI and bounded blocked-cycle receipts; deployment pending | No 14:45 scheduling cutoff exists. Preserve invalid labels; verify future final-session coverage and separately sourced official close. See [close capture incident](incidents/2026-09-14-close-capture.md) |
| MP-2026-09-14-OPEN | Opening reference samples absent at 08:30 CT; clock/handoff readiness and subsequent incomplete quote pairs blocked recording | OPEN — prevention checklist added; capture correction not implemented or verified | [Incident](incidents/2026-09-14-opening-capture.md); exact opening bucket plus sustained per-symbol progression in a future eligible session |
| MP-2026-09-14-ORB-READ | Live ORB read returned HTTP 503 / RUNTIME_READ_TIMEOUT while compact health reported active transport | OPEN — read-path cause not yet isolated | Measure snapshot/query/lock latency independently of capture; verify bounded reads and feed progression |
| MP-2026-09-14-UI | Advisor changes tested on disk but old Advisor remained visible in the running dashboard | OPEN — source implemented and isolated tests passed; activation not verified | Preserve session choices, activate dashboard only when safe, verify new UI and unchanged backend identity/progression |

Mandatory reference for future premarket reviews: [Premarket and opening evidence checklist](PREMARKET_EVIDENCE_CHECKLIST.md).

Validation-method preservation and broader legacy audit: [2026-09-14 findings and acceptance gates](VALIDATION_METHODS_AND_LEGACY_AUDIT.md). Labels and separated exports are source-tested (205 tests); runtime activation remains OPEN. VIX gate suitability, ETF evaluation leakage, and conditional model-identity risks remain OPEN; no model or threshold tuning was performed.

## Record template

- Incident ID and affected session/symbols.
- Observed-at time with timezone; actual failure interval.
- Visible symptom and exact machine-readable reason.
- Evidence paths, source/runtime identity, coverage and uncertainty.
- Root cause: confirmed facts versus unresolved hypotheses.
- Proposed correction and affected components.
- Implemented change: files, revision/diff, date; or explicitly PENDING.
- Validation: command, timestamp, outcome, evidence artifact.
- Deployment: loaded runtime identity and UI/backend postconditions; or NOT VERIFIED.
- Recurrence prevention: checklist item and regression test.
- Closure: only after the specific acceptance criteria pass. Keep history when reopening; do not replace original observations.
