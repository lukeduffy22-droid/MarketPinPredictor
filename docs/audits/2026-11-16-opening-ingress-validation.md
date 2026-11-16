# Opening ingress diagnostics and prospective validation

## Evidence boundary

This procedure targets the source based on PR 31 head
`b7e25ed393c8b72bf8c0725cfba97f4abe9bd709`. It does not establish which source
is loaded on the workstation. The November 12 investigation established the
immediate rejection of the November 9–10 first buckets: zero shared live timing
samples against 50 required, missing SPX/NDX handoffs, and no evaluated pairs.
The historical SDK/callback/reader/cache timeline is absent. Nothing in this
procedure reconstructs it or changes those historical outcomes.

Existing offline fixtures reproduce lost initial records under the former SDK
startup ordering and preserved records under corrected ordering. They also
reproduce an available sampler dispatch opportunity at +4.80 seconds. These are
mechanism demonstrations, not proof of the historical cause or live success.

The focused repairs add startup stage observations and reason-bearing timing
admission observations. Neither grants capture authority. The existing
50-sample requirement, negative-lag ratio/tolerance, freshness, replay, epoch,
generation, mapping, handoff, pair, five-second cutoff and persistence gates
remain the acceptance rules.

The timing observer also exposes bounded phase/family/reason observation counts
in `timing_admission_counts` on the retained timeline snapshot. These count
collector decisions, including duplicate exclusions; they are not distinct
provider-record totals, clock synchronization, or independently acknowledged
durable counts. First detailed events coalesce independently of these counts.
VIX and RUT retain their diagnostic family names; a previously UNKNOWN
observation is never rewritten after mapping. Emission retains the captured
epoch/generation and uses actual admission-observation time, separately from the
reader timestamp used by the unchanged lag calculation.

Missing-price rejection diagnostics previously attempted `int(None)`, losing the
very rejection being investigated. Missing raw prices now remain explicitly
unknown in the diagnostic event. Price validation and cache admission are unchanged.

## Read-only evidence analysis

Use a sealed copy of the existing opening timeline, not an actively appended
file. The inspector reads NDJSON and optional retained observer snapshot JSON,
prints a report to stdout, and does not start the app, open a provider session,
write a database, or repair a clock.

```sh
python tools/inspect_opening_readiness.py <sealed-opening.ndjson> \
  --epoch <exact-subscription-epoch> --generation <exact-generation> \
  --cash-open 2026-11-17T08:30:00-06:00 \
  --writer-snapshot <retained-opening-timeline-snapshot.json>
```

The timestamp is an example for a future regular session, not an instruction to
schedule or activate production. Use the application's authoritative session
calendar and the cash-open boundary recorded for the selected run.

The optional snapshot must be the matching `opening_readiness_timeline` object
exposed by health/capture diagnostics, including its `writer` object,
`instrumentation_error` and `sdk_callback_registered` fields. A complete health
payload is not that object. A raw observer snapshot lacks the streamer's error
status and cannot prove complete diagnostics. Without the snapshot, diagnostic
durability is `UNKNOWN`. NDJSON file
presence establishes readable retained events, not complete trace delivery or
fsync acknowledgment of every offered event. A matching snapshot is useful only
when it acknowledges every selected enqueue order and has no pending writes,
drops, write failures, observer overflow, callback errors or instrumentation
errors, and callback registration is observed. Even then the
bounded first-stage timeline is not a complete record-by-record history.

The inspector rejects unsupported/mixed schemas, inconsistent selected opening
boundaries, conflicting duplicate event IDs, invalid timestamps and truncated
JSON. Explicit selection excludes other epochs/generations and reports their
count. It never uses them to fill missing selected-generation observations.
Shared writer overflow notices cannot be attributed to a generation and keep
the completeness verdict unknown. Identical duplicated rows are counted once.

## Future opening procedure

Prepare instrumentation and isolated offline tests before any separately
authorized production activation. Collect the following observations during an
ordinary scheduled opening. Do not reset a generation, replay records, restart a
service, change a threshold or backfill a bucket to make an opening pass.

| Check | Required observations | Pass/fail/unknown |
| --- | --- | --- |
| Source and clock | Immutable loaded source fingerprint matches the reviewed source; runtime epoch/generation, schema, session date and cash-open boundary are recorded; existing host/provider timing checks retained | A stale/unknown loaded source invalidates claims about this repair. Host clock health does not supply provider samples. Missing clock evidence makes provider-to-local wall intervals unverified. |
| SDK startup | `queue_ownership_established`, `queue_enabled_before_iterator_start`, `iterator_start_returned`, `iterator_ownership_verified`; helper monotonic timestamps and actual queue-enabled observation | PASS for observed owned-queue enable preceding start and verified iterator ownership. A startup failure/state/order violation is FAIL. Missing stages are UNKNOWN. Startup PASS does not prove delivery. |
| SDK callback | Current-generation `first_live_sdk_quote`, provider interval end, instrument/record identity, replay state, observed mapping identity and callback wall/monotonic timestamps | Report the observed interval-to-callback wall gap with clock caveat. Late callback is pre-callback delay; it cannot distinguish provider, network, decode or earlier SDK handlers. No callback evidence is UNKNOWN, not provider failure. |
| Reader | Same record key and explicit callback event ID linked to `first_reader_processed_quote`; monotonic elapsed duration | Report callback-to-reader elapsed duration without inventing a latency threshold. Queue buildup and reader scheduling remain combined. A different first record cannot be substituted. Missing/mismatched links are UNKNOWN. |
| Timing admission | `provider_timing_admission` reason, admitted flag, sample count and generation; `provider_timing_count` milestones at 1 and 50 | Existing timing gate must reach at least 50 distinct current-generation live samples and remain synchronized before capture. Replay, duplicate, stale, invalid/missing timestamp, invalid type/alignment, missing instrument or wrong generation cannot manufacture readiness. Negative samples retain existing unsafe-clock handling. |
| Cache and handoff | Same record's reader/cache admission or rejection reason; fresh SPX and NDX handoffs | A recorded cache rejection localizes the observed rejection. Cache admission alone does not establish clock, pairs or handoff. Missing terminal stage is UNKNOWN. Both core handoffs are required by the existing capture gate. |
| Qualified pairs and sampler | Existing qualified current-generation primary pair inputs; capture-gate evidence; readiness wake/submission/dispatch and deadline | Existing pair, mapping, quote, timing and handoff gates must all pass. Dispatch must target the nominal opening bucket and preserve its exact deadline. A readiness notification is advisory. |
| Capture and durability | Immutable journal raw sample plus accepted final decision for each core, bound to the selected identity and nominal first bucket; sampler outcome; diagnostic writer acknowledgments | Live opening acceptance requires the existing journal/projection checks for both SPX and NDX. Raw-only, final-pending/rejected, retired-worker-only or late outcomes do not pass. Missing diagnostic acknowledgment makes causal attribution incomplete even if capture passes. |

The nominal first bucket is `[cash_open, cash_open + 5 seconds)`. An observation
at exactly +5 seconds is outside it. A stored diagnostic outcome can be appended
after the cutoff; its append time alone cannot establish when capture committed.
Use the capture budget, final-decision identity and immutable journal evidence.

## Classification limits

- Queue startup state/order failure: direct startup observations identify the
  violated contract. A missing startup event is unknown, not proof the queue was
  disabled.
- Callback observed late: the measured boundary is after decoding and earlier
  SDK handlers. Socket arrival/decode instrumentation or provider-side receipts
  are still needed to isolate provider latency from network or SDK work.
- Callback timely, reader later: the exact record has an observed local gap.
  Queue enqueue/dequeue depth and thread scheduling evidence would be required
  to divide that gap into queue residence and reader scheduling.
- Reader observed, timing excluded: the retained admission reason identifies
  the predicate that excluded that record. Count milestones and coalesced first
  reasons are not counts of every delivered/attempted quote.
- Reader observed, cache rejected: the recorded rejection identifies the cache
  path for that record. Timing admission may still be valid because timing is
  collected before mapping and price admission.
- Fully qualified inputs observed, sampler late/absent: compare readiness,
  submission, dispatch and final outcomes. Missing dispatch diagnostics cannot
  alone prove the sampler did not run.
- Missing/saturated diagnostics: preserve an UNKNOWN cause. Lifetime counters,
  later cache snapshots, different schemas and another generation cannot fill
  the missing opening path.

The inspector always labels live opening status as unverified by an offline
diagnostic report. It exposes retained producer/sampler outcomes for review;
the existing capture journal and acceptance checks remain the authority.

## Reproducible offline verification

```sh
python -m pytest -q tests/test_opening_ingress_report.py
```

These standard-library report tests use synthetic retained events. They verify
identity selection, record/event linkage, monotonic gaps, cutoff classification,
startup ordering, timing rejection visibility, duplicate handling, writer
acknowledgment boundaries and read-only CLI behavior. They do not connect to
Databento or verify a genuine opening.

## November 16 repair validation

The final local run passed **378 tests in 21.61 seconds across 24 files**, using
Python 3.12.14, Databento 0.86.0 / DBN 0.69.0, NumPy 1.26.4, pandas 2.3.3,
SciPy 1.16.3, SQLAlchemy 2.0.44 and pytest 9.0.2. The SDK and listed numerical
dependency versions match the applicable lock entries. The new GitHub job also
checks the focused diagnostics on Python 3.11; its remote result must be read
separately. Application dependency manifests were unchanged; the dedicated CI
job pins its test dependencies to the compatible lock versions, including
PyArrow 21.0.0.

The final test stdout SHA256 is
`b7b4c0b14ede6cfb3c3b16601cae9b1573d7d4ecf59c941b5c8f2e9569eb9d5b`.
Compilation, whitespace checks and the unchanged publication-boundary checker
also passed. The final regression command was:

```sh
python -m pytest -q -p no:cacheprovider \
  tests/test_opening_timing_admission_diagnostics.py \
  tests/test_databento_iterator_startup.py \
  tests/test_opening_queue_startup_diagnostics.py \
  tests/test_opening_ingress_report.py \
  tests/test_ingestion_clock_isolation.py \
  tests/test_opening_readiness_timeline.py \
  tests/test_opening_readiness_integration.py \
  tests/test_opening_timeline_identity.py \
  tests/test_opening_supplemental_milestones.py \
  tests/test_quote_arrival_telemetry.py \
  tests/test_orb_readiness_wakeup.py \
  tests/test_orb_final_decision_lifecycle.py \
  tests/test_orb_persistence_budget.py \
  tests/test_databento_reader_publication_lock.py \
  tests/test_warming_reader_publication_lock.py \
  tests/test_quote_timing_contract.py \
  tests/test_gex.py tests/test_gex_invariants.py \
  tests/test_streamer_provider_selection.py \
  tests/test_closing_tape_readiness.py \
  tests/test_prediction_authority.py \
  tests/test_validation_method_labels.py \
  tests/test_health_loaded_code_fingerprint.py \
  tests/test_opening_acceptance_preflight.py
```

Earlier attempts are not counted as passes: initial collection was blocked by
missing test dependencies, an SDK version different from the lock, and an
incomplete Streamlit import environment. Those were corrected only in the
isolated test environment. One 189-test run had 188 passes and a failure in the
existing NDX SQLite 250 ms persistence-budget fixture. Its unchanged 47-test
persistence/lifecycle set passed on rerun, followed by the full combined pass.
No persistence timeout, acceptance predicate, or inherited test was relaxed.
Two existing dependency deprecation warnings remain in the final run.

The first remote workflow, run `43951052324`, passed publication-boundary and
offline-contract-tests, while the new opening-ingress job failed during
collection: its unpinned PyArrow resolver selected version 26.0.1, which requires
NumPy 2 despite the project's NumPy 1.26.4 policy. The dedicated test job now
pins the compatible lock versions used locally. No admission logic, threshold,
or regression assertion changed to address that environment failure.

New regressions cover missing receive timestamps through the actual reader,
all timing exclusion reasons, 49/50 samples, unsafe negative lag, immutable
UNKNOWN attribution, observer exceptions, bounded retention saturation,
generation and epoch replacement during emission, cutoff-crossing observation
timestamps, malformed-price rejection evidence, actual SDK burst ordering, and
read-only CLI behavior. Existing publication-lock isolation, wakeup,
immutable-decision, SQLite budget, source fingerprint and preflight tests are
included. These are offline results; live capture and historical upstream
causation remain unverified.
