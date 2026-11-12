# September 17: session diagnosis and correction follow-up

Status: OPEN. Research only. No quote threshold, model, prediction authority or
recorded tape was changed. This entry records the user-supplied diagnostic packet;
it is not a fresh observation of the running application.

## Recorded evidence

Packet: `diagnostic-evidence-v1`, created `2026-09-17T20:22:58.649640+00:00`.
Runtime observed `2026-09-17T20:22:57.626249+00:00` (15:22:57 Chicago).
Epoch `3597a9818278504ecd8088f7938fab854d4fc075cea0607f877462186382aece`,
generation 1; handoff active; 7,647,701 messages; last tick age 1,374,492 ms
(about 22.9 minutes). Pipeline eligibility false. These post-close values alone
do not establish a regular-session outage or continuous capture.

| Evidence | Observation | Correction / next acceptance check |
|---|---|---|
| `capture:SPX` | 386 attempts; last 19:59:35 UTC invalid, 3 paired quotes versus minimum 5 | OPEN: inspect same-generation call/put freshness, selected contracts and pair attrition during the last 15 minutes. Prove valid final-session progression in a later eligible session; preserve failed attempts. |
| `capture:NDX` | 377 attempts; last 19:59:04 UTC invalid, coverage 0.09917355371900827 versus 0.1 | OPEN: inspect numerator/denominator and pair age at the exact calculation. The runtime value 0.045454545454545456 is a different observation, not the same measurement. Do not lower the threshold to pass. |
| `capture:VIX` | 374 attempts; last 19:59:25 UTC invalid, 1 primary-expiration strike versus minimum 30 | OPEN: distinguish legitimately sparse primary expiry from definition/subscription loss. Review symbol-specific gate suitability separately; no gate change is justified by this packet alone. |
| historical documents | September 14 incidents remain in the register | Historical context is intentionally retained; their presence is not proof that each incident recurred on September 17. |

The supplied capture summaries report last-attempt validity, not valid/invalid
totals for the whole session. They cannot establish that every generated sample
failed, that raw OPRA was retained, or that the official close was obtained.

## Follow-up from retained files, September 17 at 20:30:09 UTC

A read-only pass over the local audit and capture-attempt files produced:

| Symbol | Valid / retained attempts | Valid / attempts in final 15 minutes | Last valid (Chicago) |
|---|---|---|---|
| SPX | 359 / 386 | 1 / 15 | 14:47:42 |
| NDX | 373 / 377 | 14 / 17 | 14:58:54 |
| VIX | 0 / 374 | 0 / 14 | None |

These are retained record counts, not a continuous-coverage or raw-tape claim.
The first two symbols did not fail all day. SPX's late-session degradation and
VIX's all-session invalidity need separate investigations. The original packet's
last-attempt state concealed that distinction. Sorted retained-record hashes:

- SPX: `78c62825b9d6bbdb9d8a11a6653cdbb2a9fe195de7c873c303cdda7b84921a08`
- NDX: `2c6ae581c18e342e3de20efcc608063280d96a40714cb21b54966d8055636bd7`
- VIX: `f3085373c823a96292026c9aa78e4c261cde9646a8e3b5a869992c514d3ebf05`

## Confirmed reporting defects and implemented correction

Source review found `build_packet` selected capture files using the wall-clock
date while runtime context could be retained from an older Advisor check. The
old packet also reported only the final attempt's validation state, and AI reviews
were retained only in Streamlit session state unless downloaded.

Source changes in `app/services/diagnostic_agent.py` and `diagnostic_history.py`:

- Explicit capture-date selection; label runtime date, age and session phase.
- Label register/checklist excerpts as historical context, not new observations.
- Include whole-session validity counts, regular-session and final-15-minute
  counts, last valid attempt, failure reasons and a hash of retained input records.
- Show missing capture evidence explicitly. Never substitute another session.
- Save requested reviews in content-addressed, append-only local files under
  `data/diagnostic_reviews/<session-date>/`; expose saved reviews by date.
- Preserve proposed-only AI output. Saving a review does not execute a change,
  verify deployment, establish accuracy, or close an incident.

Validation: 17 focused diagnostic/capture tests passed, including selected-session
isolation, post-close classification, immutable archive retries/tampering, and
Streamlit save/date-change behavior. No paid AI request was made by these tests.
Implementation and offline validation are separate from deployment and future
capture proof. Runtime activation: NOT VERIFIED. Capture failures: NOT FIXED.

## Repeat for each new session

1. Read the failure register and premarket checklist. Retain the exact opening
   bucket receipt and session identity; never substitute later samples.
2. In Advisor, select **Analyze Live State** for a newly timestamped observation.
   Select **Capture session to review (Chicago date)**, then preview the packet.
   A page rerun alone does not refresh the retained runtime observation.
3. Ask AI to compare selected-session evidence with the open acceptance gates.
   Require PASS, FAIL or NOT YET VERIFIED for each; absence of evidence is not PASS.
4. Select **Save dated review to local history**, and download the change-request
   bundle for implementation work. No automatic API calls or scheduler are installed.
5. Record each accepted fix's revision, focused tests, deployment identity and
   later-session evidence here or in its own incident entry. Append dated updates;
   preserve the original report and leave unresolved gates OPEN.

Closure requires deployment proof plus the incident-specific future-session
acceptance evidence. More attempts, HTTP 200, or passing tests alone do not close
capture failures. Official-close provenance and forecast scoring remain separate.
