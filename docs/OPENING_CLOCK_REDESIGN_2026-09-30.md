# Independent clock readiness: measurement release and admission design

Prepared during the September 30 session. **This checkout is an offline candidate;
the canonical running application has not been changed.** The implemented release
is diagnostic only and disabled by default in the Python component. The later
`QUOTE_CLOCK_REPAIR_2026-09-30.md` release enables diagnostics in the guarded
launchers and fixes reader contention plus quote filtering. Independent host
clock evidence still has no opening-admission authority.

## What failed and what did not

The app's current `processing_clock_telemetry` requires 50 eligible live provider
timing records. September 30's exact 08:30:00–08:30:05 CT attempts had only 0–3.
That proves insufficient provider timing evidence, not an unsynchronized Windows
clock. The subsequent bucket had 3–9; by the third bucket sufficient clock evidence
existed but handoff was still warming. SPX's first accepted bucket was 08:30:15,
NDX 08:30:20, RUT 08:30:25, and VIX 08:31:35.

The first hour retained 717/720 SPX, 716/720 NDX, 715/720 RUT and 496/720 VIX
eligible buckets. Missing early pairs/handoff remain separate constraints. Clock
readiness alone cannot establish that any missing first bucket would have passed.
No historical record may be backfilled or relabeled using this new design.

## Recommended approach

Separate three evidence lanes:

1. Host UTC readiness: fresh local W32Time observations before the open, with
   source, successful-sync age, phase offset, delay/dispersion and process identity.
2. Elapsed-time continuity: a precise UTC clock compared with a high-resolution
   monotonic counter, to expire evidence and detect steps, suspend and stale reads.
3. Provider timing and quote eligibility: retain the existing 50-record assessment,
   generation/replay checks, quote freshness, paired quotes and handoff checks.

The intended future admission rule is a separately versioned host-bootstrap
policy for insufficient provider samples only. Any material negative provider
timing must veto bootstrap even below 50 samples. Never transform the raw provider
status from `unknown` to `synchronized`. Keep the authority and raw evidence
explicit in immutable storage, readers, receipts and health. At 50 samples the
existing provider assessment applies. All quote, handoff, exact-bucket and final
commit checks must remain independent.

## Implemented now

`backend/host_clock_shadow.py` collects local status with a one-second timeout,
without resync, configuration writes or a provider connection. The optional
observer queries every five seconds on its own thread; health and opening reads
only inspect cached memory. Failures replace prior successes. Nothing is loaded
from a disk receipt as current authority.

It rejects missing/ambiguous fields, failed synchronization, local/free-running
references, stale sync, expired observations, changed reference sources, owner
mismatch, wall-clock discontinuity, slow clock reads and excessive reported
uncertainty. The output distinguishes `candidate_ready` host evidence from the
unchanged provider status and labels its authority `diagnostic_only`.

`MARKETPIN_HOST_CLOCK_SHADOW_ENABLED=1` enables measurement after a future controlled
deployment. The default is off. There is deliberately no switch granting this
candidate permission to admit opening samples. A failed host observer cannot
block a valid provider-backed sample; a ready host cannot admit an unknown
provider-backed sample under the existing storage contract.

The standalone `python -m tools.inspect_host_clock_shadow --output <new-path>`
command takes one bounded read-only observation without importing app lifecycle,
database or provider code. It refuses to overwrite a receipt. Optional retained
provider JSON remains labeled retained, not current.

## Calibration found during the live read-only trial

This host's Python 3.11 reports 15.625 ms resolution for both `time.time` and
`time.monotonic`, implemented with GetSystemTimeAsFileTime and GetTickCount64.
The first prototype falsely flagged a roughly 13 ms discrepancy against a 10 ms
step budget. Its original failed receipt is retained. This is a prototype flaw,
not evidence of a 13 ms host clock jump or the cause of this morning's miss.

The candidate now reads UTC with GetSystemTimePreciseAsFileTime and elapsed time
with QueryPerformanceCounter (`perf_counter_ns`). A bracket longer than 1 ms
fails closed rather than confusing scheduling delay with a clock measurement.
Microsoft documents these as the respective time-of-day and interval mechanisms:
[precise UTC API](https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getsystemtimepreciseasfiletime).
[Python clock metadata](https://docs.python.org/3.11/library/time.html#time.get_clock_info)
distinguishes implementation, monotonicity and resolution.

Proposed measurement limits are 15-second evidence lifetime, successful sync no
older than two polling periods or 128 seconds (whichever is smaller), a 10 ms
step screen and a 50 ms reported uncertainty screen. The latter uses reported
dispersion + half reported root delay + absolute phase offset + a 15 ppm
post-query holdover allowance. These are engineering screening assumptions, not
certified bounds on true UTC error. Do not loosen them until measured evidence
and the intended policy justify a change. The provider's 50 ms tolerance and
50-record requirement have not changed.

[Microsoft's status-query documentation](https://learn.microsoft.com/en-us/windows-server/networking/windows-time-service/windows-time-service-tools-and-settings)
supports local read-only collection. Its [accuracy support conditions](https://learn.microsoft.com/en-us/troubleshoot/windows-server/active-directory/support-boundary-high-accuracy-time)
do not make a generic "Sync" status a guarantee of required UTC accuracy.

## Concrete next release gates

1. After the session and final tape verification, deploy only this measurement
   release using its exact before/after hashes, refreshed preflight fingerprint
   and a controlled canonical launch. Enable observation only. Keep current
   opening admission and the Windows watchdog's recovery ownership.
2. Collect pre-open evidence across Windows sync cycles and source changes;
   calibrate absolute offset independently of quote arrivals. Verify process
   restart, sleep/resume and failed-query behavior. Afternoon measurements do
   not prove tomorrow's 08:30 state.
3. Implement the versioned host-bootstrap admission contract and immutable
   reader/persistence evidence together. Bind provider evidence to the current
   epoch/generation; revalidate host freshness/continuity and conflicts before
   raw append and after persistence. Never reuse a pre-change decision after
   generation, process or clock authority changes.
4. Rehearse exact first-bucket capture with 0/2/3/49 samples, adequate quotes and
   active handoff; inject negative lag, replay, stale host evidence, wall steps,
   missing pairs, changed generations and late commits. Success requires the
   actual sample plus eligible final decision in the correct five-second bucket.
5. Verify the next real open's first bucket and all subsequent ranges. Continue
   to report per-symbol gaps; neither tests nor a successful open prove all-day
   or future-session reliability.

This sequencing makes the proposed change concrete and measurable without
replacing a market-hours gate with unvalidated time authority.
