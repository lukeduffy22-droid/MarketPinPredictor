# Pre-open preparation and provisional ORB evaluation

November 6 follow-up, performed after hours. The prior opening-readiness report,
source snapshot and validation receipt remain preserved. This follow-up does not
close the unresolved primary CBBO timing/NDX availability incident.

## Preparation that actually ran

Retained clock/watchdog logs show 07:00 and 07:15 CT pre-stage invocations and
synchronized host-clock checks. The 07:00 source preflight passed after about
234 seconds, but universe pre-stage subsequently failed. Its SPX, NDX and VIX
fragments were saved and later reused. The 07:15 source preflight timed out at
300 seconds. The exact first failure's message is not retained in the redacted
supervisor failure receipt, so do not invent an OI or provider-error cause.

The 07:45 launch path recovered a verified current-day universe by approximately
07:50 and started backend PID 29524. Backend database initialization took about
139 seconds; the live CBBO subscription was established at 07:53:18 CT. The main
08:15 invocation preserved that PID. There is no observed generation reset
between that subscription and the opening generation-1 attempts.

The clock verifier checks host-clock health; it does not create live provider
samples. Fresh, nonreplayed pre-open provider records may satisfy the unchanged
50-sample timing gate, and fresh current-generation accepted observations for
both core families may activate handoff. Source does not forcibly clear provider
timing at cash open. Reconnection/generation change does clear it. Universe
definitions and symbol mappings are not current quote evidence. An earlier
successful preflight can reduce startup risk, but does not guarantee those live
observations inside the first five seconds. The missing historical primary
receipt/rejection trace still prevents determining their delayed availability.

## Separate scheduler defect repaired offline

The installed task XML contains fixed-offset September start boundaries. The
launcher validator converted those historical instants to local `HH:mm`, accepting
a summer wall time as proof of a recurring Central schedule. Read-only COM, CIM
and schtasks checks instead report the next Monday AutoStart at 06:00 CT and
Watchdog at 06:50 CT. Today's logs show the intended 07:00/07:15/07:45 invocations;
do not claim this discrepancy caused November 6's missing bucket.

Microsoft specifies that an explicit StartBoundary offset is used regardless of
local daylight-saving settings, whereas a floating local boundary follows local
time and DST:
https://learn.microsoft.com/en-us/windows/win32/api/taskschd/nf-taskschd-itrigger-put_startboundary

The remediation source now rejects fixed-offset weekly schedule contracts and
creates floating local wall-time boundaries for the intended Central Windows
host. Registration and the narrow legacy 07:15-trigger repair use the same
representation. Boot/logon triggers, cadence, validity gates and process controls
are unchanged. Tests exercise seasonal fixed-offset false acceptance and the
intended floating schedule without registering or running actual tasks.

The installed tasks were not changed. Activating this source does not itself
repair existing fixed-offset tasks; a separately reviewed task update and exact
readback remain necessary. Do not claim the live schedule is repaired.

## Short grace period: evaluation only

Two different mechanisms require distinct treatment:

- A bounded processing/persistence allowance can retain genuine inputs observed
  inside the nominal opening bucket. It must freeze those inputs and their
  contemporaneous validity/identity, record completion separately, and use a new
  explicit decision contract. It cannot admit later quotes as opening inputs.
  The November 6 early failures did not retain such valid opening inputs, so no
  historical benefit from extending processing time has been demonstrated.
- A delayed-start sampled range can show observed high/low as provisional context,
  with opening seconds unobserved. Keep nominal 08:30 CT boundaries and actual
  window duration; display first accepted bucket, provider reference time and
  capture/decision time separately. Neither its first price nor its range is the
  official opening print or a complete opening range.

Evaluate delayed-start limits such as 10, 15, 20 and 30 seconds against retained
windows, without selecting a production threshold from one incomplete day.
Opening lag alone is insufficient: report actual nominal-window coverage,
internal/end gaps, invalid or undecided samples and identity transitions. The
November 6 first-hour series also has later gaps, a 200-second maximum gap and
three epoch/generation/universe identity groups per core symbol. A short opening
grace does not repair those defects.

Display-only provisional context and independently labeled research annotations
can be useful, while current production ORB/structure directional gates, model
inputs, training eligibility and evaluation denominators remain unchanged. A
price above an incomplete observed high may still lie inside the unknown full
range; the size of this error cannot be measured from missing prices. Do not
stitch mixed identities into a single eligible structural breakout. No grace or
provisional signal was implemented or activated by this evaluation.

Executed checks, read-only task definitions, precise chronology, window/grace
calculations and source-bound verification are saved under:

`C:/Users/LukeD/.codex/visualizations/2026/10/27/01a17f25-1b0c-7f63-b077-449c2bb3ff81/opening-readiness-remediation-20261106/preopen-followup/`
