# PR 31 history repair and opening rejection investigation

## Git history

The November 11 publication job failed at `git merge-base --is-ancestor
origin/friday-1/9 HEAD`, before path, secret, blob-size or whitespace checks ran.
The protected base had two commits absent from the PR branch: `d356e0d2b`
(runtime read and GEX diagnostics) and `fdf0d6976` (shadow v2 ablation and RUT
evidence). This was divergent history, not a shallow checkout: CI fetched full
history and the integration base.

Merge `7400600cc` incorporates that base without rewriting either history.
Two conflicts retained the PR's explicit opt-in shadow v2 default and its
matching-realized-outcome requirement. The incoming provenance test and runtime
diagnostics remain. Existing v2 opt-in tests cover the retained behavior.

After ancestry was repaired, the complete publication check exposed three
inherited `data/` files and whitespace issues. Commit `56c9b72ff` removes those
files from tracking while preserving local copies, adds exact ignore entries,
and normalizes eight files. Python AST and JSON value equivalence were checked
for the relevant whitespace-only edits. Original blobs remain in history.
The unchanged publication scanner passes against `origin/friday-1/9`.

This repair updates a review branch. It does not merge PR 31 into the default
branch, replace canonical application source, or restart a service.

## Why the opening buckets were rejected

The sealed November 9 and 10 failure journals were independently rehashed and
recounted on November 12. Their grain is a failed sampling attempt, not a quote
or unique market bucket. The 66 failures represent four missing instrument-day
first buckets, not 66 different opening intervals.

| Day | SPX attempts | NDX attempts | Recorded rejection |
| --- | ---: | ---: | --- |
| November 9 | 17 | 17 | PROCESSING_CLOCK_NOT_SYNCHRONIZED |
| November 10 | 16 | 16 | PROCESSING_CLOCK_NOT_SYNCHRONIZED |

All 66 receipts contain zero admitted shared live timing samples against 50
required, an unknown processing clock, a warming handoff with both SPX and NDX
missing, and quote-pair evaluation explicitly not evaluated. Thus the initial
readiness gate rejected them before parity/pair calculation. This is not proof
of a bad price formula or 66 rejected provider quotes.

The first retained positive timing observations occur after the five-second
cutoff: +6.050699 seconds on November 9 (one sample, VIX only) and +6.345154
seconds on November 10 (three samples, VIX only). The prior consistent SQLite
projection reports the first eligible nominal buckets at 08:30:20 CT and
08:30:15 CT respectively. That projection is retained evidence, not a fresh
query of the live database in this repair.

## Causal limits and existing corrections

The exact upstream reason for missing readiness remains unresolved. The original
primary CBBO SDK callback/reader/cache timeline was not retained in these
receipts. Later TCBBO recordings, cumulative counters and later cache snapshots
cannot determine whether the provider/network, SDK queue, reader scheduling,
or cache admission delayed the required records. A fresh recount cannot create
that missing observation.

PR 31 already contains two independently reproduced corrections: arm a newly
owned supported SDK iterator queue before starting its session, and wake the
existing sampler on qualified readiness events instead of relying only on a
250 ms poll. The former reproduced 50 delivered/zero consumed records under old
ordering and 50/50 with the correction. The latter preserves a controlled
+4.80-second dispatch opportunity. Neither proves it caused either historical
failure; these days have no retained fully ready state within the first bucket.

The PR also retains generation-tagged stage diagnostics and off-reader durable
acknowledgments to distinguish these mechanisms prospectively. The 50-sample
timing requirement, shared core handoff, current-generation quote/pair checks,
and first-bucket cutoff remain unchanged. Historical buckets remain rejected.

## Evidence references

- [PR 31](https://github.com/lukeduffy22-droid/MarketPinPredictor/pull/31)
- [Original failing workflow](https://github.com/lukeduffy22-droid/MarketPinPredictor/actions/runs/43104294377)
- Local sealed evidence: `MarketPin Audits/2026-11-08 - SPX and NDX Opening Readiness/2026-11-10 Resume and Offline Fixes/November 9 and 10 Opening Evidence` under the user's Documents folder.
- Journal SHA256, November 9: `4bec33966b859641288e4717c75ec76c6c8836fd83197f53329743af3668c99b`.
- Journal SHA256, November 10: `6f5965e0098896e3a425095c575345542234336680ab0ceca4c3097719f48eda`.

Current runtime health and successful live opening capture require separate
observations; this offline integration result does not establish either.
