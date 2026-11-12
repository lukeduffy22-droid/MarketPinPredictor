# November 6 full-window ORB remediation, inactive source

The scope covers later capture gaps, subscription churn, final-decision lifecycle
and useful provisional displays across 5/15/30/60-minute nominal windows. All work
is in `codex/audit-remediation-20261027`; historical evidence and prior reports
remain preserved. No services, installed schedules, databases, WAL or application
configuration were changed, and no code was activated.

## Verified retained failures and limits

The first hour has SPX625/720 and NDX622/720 final timing-eligible buckets. Three
opening grid buckets are absent per core; later losses are92SPX and95NDX. The
maximum adjacent-sample gap is200seconds. Only9/95SPX and12/98NDX missing buckets
have matching retained failure receipts. The remaining172 instrument-buckets
cannot be assigned individual historical causes from available evidence.

All five raw rows lacking final decisions match post-commit failure receipts:
NDX08:42:00,09:04:35,09:05:45,09:11:55 and SPX09:24:25CT. Four failed during
post-persist context validation, one during decision persistence. At08:42,
NDXraw persistence took0.156s, followed by3.813s waiting for publication;
SPXwaited3.953s at the shared persistence barrier. A fast raw write could therefore
strand its terminal decision and delay the peer market. Capture and decision
persistence reused the expired budget. This is demonstrated lifecycle failure,
not evidence that the process crashed.

The sampler also waited on forecast publication before selecting the next bucket.
An offline test reproduces skipped dispatch while a real publication guard is
held. This is a proven source mechanism, not proof that it caused every one of
the172 historically unreceipted missing buckets.

Generation1 transitioned to2 at08:30:22 because optional RUT monitoring treated
NDX's initial partial fresh-quote population as established core degradation,
bypassing the180-second warmup. The nearby valid NDX calculation has356/780fresh
contracts, safe measured lag and admissible pair inputs. The exact counter snapshot
at the rollback instant is not retained; its logged sole reason is
`CORE_FRESH_QUOTE_COVERAGE:NDX`. Retained nearby input replay reproduces the policy
failure. This correction addresses later churn, not the missing first five seconds.

Generation2 transitioned to3 after an observed gateway timeout at08:58:44 and
resubscription08:58:48. This recovery remains necessary under that observed
transport failure. It cannot explain the earlier08:42–08:45gap. Shared queue-full
warnings are transport evidence, never per-symbol event counts or proof that
provider quotes were absent. No original primary first-five-second CBBO arrival
trace was found; opening provider/NDX availability remains unresolved.

## Narrow implementation

1. Separate ORB metadata reads and dispatch from forecast-publication persistence.
   Universe metadata uses its own lock, with a short nested runtime identity copy.
   Runtime is released before quote acquisition to preserve existing lock order.
   Handoff diagnostic maps use a nonblocking publication-lock copy and explicitly
   report unavailable observations under contention. All real freshness, quote,
   pair, timing, epoch/generation and universe checks still apply.
2. After a committed raw sample loses its capture/decision path, attempt one
   separately bounded0.25-second audit-only negative finalization. It cannot
   append a new raw row or eligible late decision. Already-final decisions stay
   immutable. Busy storage or unavailable completion time leaves an explicit
   pending/ineligible raw witness; no timestamp is invented and no background
   retry writes after the caller returns. Historical orphan rows are untouched.
3. Honor existing startup warmup only for a positive partial fresh-quote population
   that has not established healthy coverage in the current epoch/generation,
   with other validation, pair, age and lag checks safe. Established coverage loss,
   zero quotes, malformed evidence, overload, provider loss and emergency rollback
   remain active. Required reconnects are not suppressed.
4. Add context-only observed segments to all four nominal-window projections and
   a separate dashboard table. Preserve complete provenance identities, repeated
   A-B-A transitions and overlapping identity buckets; deduplicate identical
   identity/bucket observations. Report observed extrema, first/last bucket,
   provider/capture clocks, nominal and elapsed coverage, elapsed missing spans
   and future buckets separately. Pending/invalid/incomplete-identity inputs are
   excluded. No mixed-identity pooled range becomes an eligible ORB; directional,
   prediction, model and training eligibility on context segments are always false.

The remediation checkout lacked the caller-owned lock/SQLite budget implementation
already present in the observed canonical runtime. Only those required budget
functions and fault tests were ported before implementing the above fixes, so a
future remediation activation would not restore older unbounded capture writes.
This prerequisite port is not represented as a newly discovered live correction.
No database schema or historical data migration was introduced.

## Provisional usability and grace

Observed context remains useful when the full opening range is incomplete, but
it cannot establish unknown opening extrema or an authoritative breakout. A
20-second absolute delayed-start candidate includes November6's first actual
captures(+16.559sSPX,+17.561sNDX), whereas15seconds does not. No grace threshold or
production signal promotion was activated or introduced. Genuine late processing
of already-captured opening inputs is distinct from a late first observation.
The new terminal audit budget is not a price-acquisition grace period.

Historical nominal coverage stays90%for both first5-minute windows and86.81%SPX/
86.39%NDX for the first hour. Those windows mix2/3identities respectively. A
display improvement or excusing the initial missing seconds cannot change that
history. Future full-window completeness requires real complete observations,
stable provenance and successful persistence in a live eligible session.

## Verification and evidence

The joint pre-fix fault baseline failed7cases and passed4controls: four missing
negative sidecars and three publication-lock isolation cases. New tests use real
isolated SQLite/WAL journals and native CBBO timing records; those are synthetic
market fixtures, not a replay of missing historical provider arrivals. Additional
faults cover post-commit exceptions, unavailable clocks, busy storage, immutable
positive decisions, genuine safety losses and per-window context eligibility.

The full-hour end-to-end rehearsal exercises720five-second buckets for each of
four configured instruments, durable raw/final rows, all four window projections,
runtime binding and the API. Gap/delayed-start/reconnect scenarios remain partial.
Executed checks, before-images, function-scoped patches, retained-data analyses,
source-bound final receipts and the final evidence-backed report are saved at:

`C:/Users/LukeD/.codex/visualizations/2026/10/27/01a17f25-1b0c-7f63-b077-449c2bb3ff81/orb-full-window-remediation-20261106/`

The expanded archive inventory also found uniquely retained07:00preflight logs
missing from the earlier mutable-log review:214.531s of the successful check was
database checking, and the first universe phase trace stopped during RUT
definitions. See `gaps-lifecycle/ARCHIVED_PREOPEN_ADDENDUM.md`; original reports
were preserved rather than overwritten. The07:15 internal timeout stage and
the exact first universe terminal provider error remain unavailable.

Passing offline tests do not establish a usable complete live ORB or predictive
reliability. Keep activation and future complete5/15/30/60-minute acceptance
explicit, and preserve every remaining gap as missing.
