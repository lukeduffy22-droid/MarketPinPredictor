# Repairs required after Friday's failed opening

Prepared October 2, 2026, approximately 1 PM America/Chicago. This is a proposed
implementation plan grounded in read-only inspection. No runtime, subscription,
model, clock, admission-policy or historical-data changes were made in this review.

## What changed since the morning watch

**OBSERVED:** samples at 12:57:43 and 1:00:13 PM found the same generation 53.
Messages advanced from 3,601,683 to 3,664,882. Reconnects stayed at 165, queue-full
warnings at 74 and skipped-record warnings at 174. Both SPX/NDX prediction endpoints
returned 200 with live, research-only estimates. The second live-health read
reported valid current-generation SPX/NDX inputs aged 5.04 and 9.72 seconds.
Receive-to-process lag was about 0.13 seconds at both endpoints of the interval.
These sampled results establish improvement, not continuous or future stability.

**OBSERVED:** the exact first opening attempts for all four symbols were rejected
with provider clock status `unknown`, reason `INSUFFICIENT_LIVE_TIMING_SAMPLES`,
sample count 0 of 50, and host diagnostic status `candidate_ready`. The broad
`PROCESSING_CLOCK_NOT_SYNCHRONIZED` label does not prove Windows was unsynchronized
at that instant. Host diagnostics currently have no opening-admission authority.

**OBSERVED:** applying the existing pure recorder gate to the 1 PM retained health
payload rejects it for exactly two reasons: cumulative skipped-record warnings
and cumulative pre-auth transport aborts. Current synchronized clock, runtime
identity, transport and other checks passed in that sample. Restarting to zero
those counters would conceal history and is not an acceptable repair.

**OBSERVED:** current-session EQUS.MINI historical polling is explicitly denied
with `ENTITLEMENT_UNAVAILABLE` and has zero accepted bars. Earlier historical
retrieval success does not contradict this current-session access failure.

**OBSERVED:** structural estimates still report `COMMITTED_CALCULATION_ID_MISSING`
inside otherwise available research forecasts. The structural adapter requires
a committed calculation identity; transient quote/calculation revisions do not
always carry it. The exact failing lifecycle path requires separate tracing.

## Implementation order and acceptance

### 1. Make ingestion stable under opening load

**Observed failure:** repeated queue saturation, slow-reader/skipped-record
warnings and reconnects during the morning, followed by stale generations.
**Not yet verified:** the precise hot function, lock contention or scheduling
cause. Overall host CPU does not establish capacity on the ingest thread.

- Instrument bounded reader processing time, SDK queue depth/age, quote-lock
  wait/hold time, computation duration and persistence duration. Capture a
  sanitized latency profile; do not add a second provider session to diagnose it.
- Replay retained compatible records into an isolated fixture database, including
  a burst matching measured opening traffic. No replayed record enters live
  forecasts, opening evidence or the production database.
- Repair the measured bottleneck. Keep decode and minimal quote/timing updates
  bounded. The reader and calculation loops are already separate threads; merely
  adding another thread is not a demonstrated fix. Move expensive work across a
  process boundary only if measured contention requires it, retaining generation
  and commit guards. Quote-state coalescing must never replace immutable raw tape.
- Distinguish provider connection failure from local backlog in reconnect
  decisions; do not reset timing evidence or repeatedly reconnect a connection
  merely to disguise overload. Preserve real transport-loss handling.
- Acceptance: representative burst/soak test with no introduced queue loss or
  unbounded lag, followed by stable live generation, fresh valid SPX/NDX,
  advancing committed calculations/passports and no new overload warnings over
  a predetermined observation interval. A pair of successful reads is insufficient.

The installed client explicitly uses `slow_reader_behavior="skip"` for CBBO.
[Databento documents](https://databento.com/docs/api-reference-live/client/live)
that a slow reader can lose records in this mode, while `warn` can accumulate
increasing lag. Changing that setting alone is not a throughput repair.

### 2. Fix opening-clock and preopen lifecycle decisions

- Preserve Windows time checks, but report host synchronization, provider timing
  sample sufficiency, quote availability and handoff readiness separately.
- Implement the already documented versioned host-clock bootstrap design only
  after its independent calibration and authority/persistence contract are
  satisfied. Fresh host evidence can be considered only while provider timing
  samples are insufficient; negative provider timing, replay, stale host evidence,
  wall-clock steps or generation changes must still veto admission. Do not reduce
  the existing provider sample requirement or relabel `unknown` as `synchronized`.
- Revalidate the complete identity and clock decision at final persistence, not
  just at calculation start. Fresh paired quotes and active handoff remain required;
  host clock readiness cannot create quotes that did not arrive in the bucket.
- Make supervisor readiness session-aware. Expected preopen waiting or warming
  alone must not consume the daily automatic recovery allowance. Genuine dead
  processes, disconnections and measured faults remain separate recovery reasons.
- Tests: 0/2/49/50 provider samples; missing quotes/pairs; generation rollover;
  negative lag; uncertain/stale host evidence; clock steps; preopen inactivity;
  actual stalled transport. Verify exact first-bucket capture at a subsequent
  real open. Today's missing bucket remains immutable and incomplete.

Source/design: `backend/quote_timing.py`, `backend/host_clock_shadow.py`,
`backend/databento_streamer.py`, `market_app_supervisor.psm1`,
`docs/OPENING_CLOCK_REDESIGN_2026-09-30.md`.

### 3. Add guarded recorder recovery after earlier losses

The existing absolute-zero counter gate prevents new recording after even a
recovered earlier loss. This is a capture-policy change requiring explicit,
versioned evidence semantics; deleting its checks is not the proposed repair.

- Retain cumulative loss/abort counters and the original failed-session status.
- Evaluate a bounded clean interval using counter deltas, advancing traffic,
  synchronized timing, verified process ownership, a stable generation and
  acceptable current backlog. Historical aborts must not be treated as closed
  transports without the existing teardown/ownership proof.
- Permit only a new separately identified recovery segment after those checks
  pass and capacity is proven. Record actual start time, baseline counters,
  source/process identities and all prior gaps. Preserve daily completeness=false.
- Do not enable broad intraday replay as part of this repair; it could recreate
  load and requires its own authorization and provenance. Any future approved
  historical recovery is a separate artifact, not a recovered live opening.
- Tests: old loss with clean current traffic; newly increasing loss; active queue
  pressure; unresolved transport cleanup; generation change; concurrent recorder;
  write failure; actual byte and record advancement. Never let this promote a
  forecast or overwrite an existing tape/catalog.

Source: `backend/closing_tape/start_gate.py`, `start_closing_tape.ps1`,
`backend/closing_tape/live_recorder.py` and recorder supervision.

### 4. Resolve current-session indicator access

Verify the Databento account/key's exact EQUS.MINI current-session permission and
activate the required authorized access, or explicitly select another licensed
bar source. No purchase or provider substitution is included in this review.
After access is corrected, require advancing verified current-session bars and
the existing continuity/warmup/freshness checks. Historical polls retain actual
retrieval times. ETF bars remain ETF context; no synthetic cash-index OHLCV.

### 5. Complete structural forecast identity and scoring reliability

Trace committed input capture through lifecycle publication to structural output.
Consume only the exact committed identity for the applicable symbol, generation
and revision; never reuse an older calculation ID for newer quotes. If the UI
shows the latest committed structural estimate separately, it must carry its own
timestamp, age and authority. Failed or delayed writes must remain abstentions.

Keep official-close selection and scoring append-only. Classify the morning DNS
failure separately from source-publication delay, and make any future retry
bounded, non-overlapping and limited to already-frozen selections. Do not change
forecast coefficients merely to make forecasts appear more often.

## Delivery boundary

Priorities 1–3 are the main capture-reliability work. Separate verified source,
offline tests, deployed process hashes and real session acceptance in reports.
Do not declare the issue fixed solely because tests pass, an endpoint responds,
or the afternoon is calmer than the opening burst. The first production change
should follow a measured bottleneck or confirmed decision-path defect and a
guarded activation, rather than another speculative whole-app restart.

Receipts: `output/failure_diagnosis_20261002/20261002T175745Z.json` and
`20261002T180014Z.json`. Original first-attempt evidence remains in
`logs/opening_reference_attempts/2026-10-02.ndjson`; the original morning outcome
is `docs/FRIDAY_OPEN_WATCH_RESULT_2026-10-02.md`.
