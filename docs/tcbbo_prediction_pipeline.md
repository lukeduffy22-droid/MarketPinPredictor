# TCBBO prediction pipeline

## Truth boundary

- Raw Databento DBN files are the immutable observed source of truth.
- `tape_observed_minute` contains reproducible arithmetic aggregates only.
- `tape_inferred_minute_flow` contains quote-position heuristics and requires an
  inference method, version, and source DBN SHA-256.
- No aggressor-side estimate is described as an observed buyer, seller,
  opening trade, closing trade, or position.
- Legacy Polygon trade messages that lack side evidence are normalized to
  `aggressor=0` (unknown). They may contribute to observed volume urgency but
  cannot create directional urgency.
- Final integrity distinguishes actual OPRA TCBBO records from generic market
  records and reports event/receive timestamp coverage, valid pre-trade NBBO
  coverage, provider flags, and action counts. A session with generic trades
  but no verified TCBBO evidence cannot enter model research.
- Every daily open-interest `StatMsg` is replayed after the DBN closes into
  append-only `tape_open_interest_observations`, retaining its source SHA-256,
  byte offset, timestamps, sequence, channel, update action, flags, and value.
  `tape_open_interest` is only the deterministic latest-state projection.
- `tape_open_interest` is the latest provider-published daily baseline per
  instrument, not an intraday participant-position ledger. OPRA leaves
  `ts_ref` undefined on these live snapshots, so `asof_utc` uses `ts_event`
  and means the time the baseline became observable, not a provider-reported
  calculation/reference time.

## Runtime boundary

The live recorder writes provider DBN streams and does no per-record feature
calculation. Integrity inspection and feature replay occur only after the file
has closed. This prevents model work, SQLite writes, and full-cache scans from
blocking Databento's reader.

The verifier fails closed on an empty tape, a tape without required trades, a
missing subscription acknowledgement, provider error messages, or evidence of
a slow reader or skipped records. A readable DBN file alone is not proof that
the market feed was complete.

Control-plane readiness imports are isolated from replay and model dependencies.
The package resolves its public exports lazily, and the evidence-contract version
lives in a standard-library-only module, so a dashboard or audit does not load
pandas, Databento, Torch, or finalization code merely to count retained sessions.
Immutable OI and definition availability is queried by finalized DBN SHA-256,
using the observation ledgers' covering content-addressed indexes instead of
reading raw-definition BLOB pages.

## Current benchmark

On the local 2026-08-25 OPRA probe:

- 21,748 total DBN records
- 20,496 symbol mappings
- 1,250 TCBBO trade records
- 2 system records
- 2 observed and 2 inferred family-minute rows
- deterministic feature hash:
  `6366cffd94d539128f27776dab527202a40b9bbf164a22a00075c2a526a95e27`
- profiled Python three-pass replay: 23.7 seconds
- batched DataFrame replay: 20.1 seconds in the verification run

Most profiled time was spent rebuilding Databento symbology intervals. This is
CPU/library work; CUDA is not an ingestion optimization. Before full-session
production replay, benchmark native TCBBO array conversion plus a separately
cached point-in-time instrument map, and process session files in bounded
partitions.

## Prediction promotion gates

A TCBBO-derived model becomes a production MarketPin input only after:

1. Raw capture completes without provider errors, skipped records, or slow
   reader evidence on at least 10 full sessions.
2. Replay produces the same feature hash from the same DBN file on three
   consecutive runs.
3. Labels use only information after each prediction timestamp, and feature
   joins use only information known at or before it.
4. Evaluation uses chronological walk-forward folds with purging for
   overlapping horizons and reports each symbol family separately.
5. The candidate beats persistence, a regularized linear/logistic model, and
   the current MarketPin forecast on held-out MAE or log loss and calibration.
6. Improvements survive multiple volatility regimes and estimated
   spread/slippage; directional accuracy alone is insufficient.
7. CUDA is promoted only if a measured batched training or inference workload
   is materially faster than the CPU baseline without changing results beyond
   an explicitly tested numerical tolerance.

The first model targets should be explicit distributions or probabilities for
1-, 5-, 15-, and 60-minute underlying returns, realized volatility, and level
touch events. "Extreme accuracy" is not an acceptance criterion because it is
undefined and encourages leakage; calibrated out-of-sample improvement is.

## Implemented research controls

`backend.closing_tape.dataset` admits only sessions whose required feed is
finalized as complete, has no reconnect or slow-reader evidence, and whose
inferred rows reference the finalized DBN SHA-256. Point-in-time MarketPin
prices are joined backward at the end of each minute with a bounded freshness
tolerance; future and stale prices are discarded rather than filled.

Dataset admission additionally requires every TCBBO record to retain both
event and receive timestamps, at least 95 percent valid pre-trade NBBO
coverage, and a persisted daily OI observation for the relevant family.
Pre-evidence catalogs are not grandfathered by an old `complete=1` flag.

`backend.closing_tape.research` labels each minute bucket from the time the
bucket first becomes knowable, not from its opening timestamp. Forward returns
must exist at the exact horizon within the same session. Walk-forward folds are
ordered by session and purge training rows whose labels were not available
before the test fold. Feature columns are an explicit allowlist, and names
containing target, future, label, or availability fields are rejected.

The initial comparison is deliberately CPU-based ridge regression versus a
zero-return persistence baseline. It reports held-out MAE, RMSE, and directional
accuracy per fold and in aggregate. This establishes a cheap leakage-detection
baseline before any CUDA model is considered.
Both the ridge and CUDA evaluators also report held-out results separately for
each option family. Promotion fails if an aggregate gain hides a family whose
MAE does not beat the relevant baseline. Point-in-time MarketPin comparisons
likewise retain per-family metrics rather than allowing a high-volume family
to dominate the score.
The ridge promotion gate additionally uses a deterministic whole-session
bootstrap. Its 95 percent interval for held-out MAE improvement must exclude
zero, preserving intraday dependence within each resampled trading session
instead of treating millions of option records as independent evidence.
The CUDA MLP must pass the same session-block test independently against both
ridge and persistence.

`backend.closing_tape.calibration` turns already out-of-sample point forecasts
into explicit conformal ranges. Each family is calibrated separately from
whole earlier sessions whose labels were available before the current session;
future outcomes cannot narrow an earlier interval. Until the configured number
of prior labeled sessions exists, interval fields remain unavailable rather
than displaying heuristic confidence. Reports include target versus empirical
coverage, coverage error, interval width, and MAE overall and per family.

Production promotion additionally requires a separately frozen deployment
conformal calibration. It uses only out-of-sample residuals whose labels were
available by the calibration as-of time and is bound to the exact model version
and artifact hash. The promoted manifest carries one absolute-log-return radius
for each production family plus the raw-tape hashes, the distinct official-close
artifact hashes, alpha, and target coverage. Runtime predictions persist the point
estimate, lower and upper bounds, radius, and calibration provenance together in
the immutable production ledger.
The dashboard labels these as estimated ranges and reports target—not guaranteed
empirical—coverage.

The torch walk-forward report retains each held-out forecast with its capture
session, trading date, family, source hash, feature and label availability times,
the exact official-close artifact SHA-256, reference price, target, and comparator
predictions. After the promoted final-fit artifact is frozen,
`bind_candidate_oos_predictions` attaches its version and SHA-256 to those retained
rows; `fit_deployment_conformal_calibration` then fits the fixed family radii from
that authoritative evidence. The walk-forward report also retains the complete set
of finalized raw DBN hashes. Final fitting rejects a labeled frame whose raw DBN or
official-close hashes differ from the promoted walk-forward report, and manifest
construction rejects a caller-supplied raw-source set that differs from evaluation.
Hand-assembled or in-sample calibration rows are not an accepted production path.

`evaluate_close_models_if_ready` retains both the exact labeled frame admitted
by readiness and the rich walk-forward report in memory while keeping its JSON
contract unchanged. `package_promoted_candidate` consumes those two authoritative
objects, final-fits the safe JSON artifact, binds the retained OOS rows to the
resulting hash, fits deployment calibration, and writes the candidate artifact
only after every step succeeds. Packaging does not enable production; paper and
regime evidence plus atomic manifest publication remain separate gates.
The v2 frozen-model format embeds the exact sorted raw DBN and official-close
artifact hash sets alongside training rows, epochs, seed, and device. Candidate
package metadata repeats those sets, and the runtime gate reloads the model file
and requires exact equality with the promoted manifest. A weight file copied away
from its evidence can therefore be identified but cannot pass production loading.
The evaluation CLI exposes packaging only when explicitly requested with both
`--package-version` and `--candidate-artifact`; an optional timezone-aware
`--calibration-asof-utc` freezes the label-availability cutoff. It emits the
candidate package metadata beside the evaluation report but never writes
`closing_tape_model.json`. Packaging also requires `--output-json`: the strict
JSON evidence report is written through an atomic temporary file and an existing
report is preserved unless `--allow-output-replace` is explicitly supplied.
Each successful package also writes a canonical content-addressed v2 receipt
under `models/candidate_packages/`. The receipt binds the candidate version and
v3 model bytes to the embedded training counters, exact raw-DBN hash set, exact
official-close artifact hash set, independently replay-verified research-surface
artifact and receipt hashes, and deployment-calibration evidence. Packaging
alone does not activate it. Only the additional explicit
`--enable-paper-candidate` flag writes the small v3 paper descriptor. That
descriptor points to a newly created content-addressed activation receipt, which
binds its wall-clock activation time to the immutable candidate receipt,
version, and artifact. Replacing a different paper descriptor requires the
separate `--allow-paper-descriptor-replace` authorization.

Deployment calibration also computes a canonical SHA-256 over the exact eligible
OOS rows: session/date/family, label-availability time, source hash, candidate
identity, official-close artifact hash, prediction, and target. Promotion rejects a
missing or unbound label hash; runtime carries the evidence identity into each
estimated range; the immutable prediction ledger persists it; and the dashboard
hides batches with mixed or malformed calibration evidence identities. Paper
evaluation independently revalidates the official source and stream-rehashes every
retained close artifact before its metrics can enter promotion, then hashes the
canonical family/date-to-label mapping with its forecasts and outcomes. This binds
displayed uncertainty and promotion evidence to the precise feature and label bytes.

The immutable production ledger has an exact ordered-column contract. Both reads
and writes reject a legacy or forward-incompatible table instead of letting
`CREATE TABLE IF NOT EXISTS` silently preserve a partial schema. Because existing
prediction evidence is immutable, schema drift requires a separate reviewed
migration; runtime code does not alter or backfill the table opportunistically.
The API loader opens the market database read-only and does not create missing
storage. A corrupt or schema-incompatible immutable ledger is returned as an
explicit `available=false` state with no rows, allowing the dashboard to show
unavailability instead of presenting partial evidence or an unexplained zero.

Long-running recorders can retain an older imported replay module while source
evolves. Finalization therefore persists contract-minute rows through the stable
catalog methods rather than importing a newly added replay helper at module load.
If an analysis worker still records such a historical import failure, the exact
byte cutoff, processed sequence, and prefix hash allow the report and optional
paper shadow to be recovered in a fresh process without restarting capture. The
later offline finalizer recomputes full-file integrity and retains the recorder's
prior error in the append-only finalization audit.

Production activation uses `write_promoted_model_manifest`, which serializes
strict JSON to a temporary file in `models/`, runs the complete runtime model
gate against that file and the referenced artifact, and only then atomically
replaces `closing_tape_model.json`. A different existing promoted manifest is
not overwritten unless replacement is explicitly authorized; failed validation
removes the temporary file and leaves the live manifest untouched.

`backend.closing_tape.regimes` calculates trailing realized volatility only
from prices available by each feature timestamp. Calm, normal, and stressed
thresholds are learned separately per family from earlier sessions, so current
or future session volatility cannot redefine the regime being evaluated.
Forecast MAE, RMSE, and direction are then reported for every family/regime
slice; missing history remains unavailable.

The runtime frozen-model gate verifies rather than trusts its manifest. It
hashes the model artifact, validates the feature schema and at least 60 unique
source-tape hashes, requires passing held-out and calibration evidence for all
five production families, and requires passing calm/normal/stressed slices for
each family. It also requires a timestamp-aligned incumbent MarketPin forecast
for every held-out row and session, a positive session-block confidence
interval for improvement over that incumbent, and lower candidate MAE than the
incumbent in every production family. The incumbent forecast is evaluation
evidence only and is never admitted as a model feature. An aggregate gain alone
can never enable the model.

The feature hash must equal the contract compiled into the running app, not
merely resemble a SHA-256. Model manifests have their own versioned contract.
If a manifest selects CUDA execution, it must also carry a measured inference
speedup over the same CPU workload and prove CPU/CUDA prediction differences
remain within a declared numerical tolerance; otherwise runtime enablement
fails closed.

Production manifests are derived through one builder rather than assembled from
independent hand-entered metrics. It requires the promoted walk-forward report,
all five family metrics, all fifteen family/regime comparisons, family
calibration, sixty unique source hashes, twenty paper sessions, the frozen
artifact hash, and workload-matched execution evidence before it can emit an
enabled manifest. The current manifest contract is v6; its corresponding v3
model artifact and v2 candidate receipt must carry and revalidate both training
provenance and the independently replayed research-surface identities.

Paper evidence is stored separately from mutable score projections in
`paper_close_forecasts`. Forecast identity is fixed by model, family, trading
date, and decision horizon; database triggers reject updates and deletes, and
idempotent replays reject conflicting values. A companion append-only table
retains canonical ordered model features and the immutable live-prefix receipt
for every forecast. The campaign ledger permits only one artifact per model
version, requires activation before feature availability, and accepts an exact
close-minus-15 forecast only within 90 seconds of its feature timestamp. Scoring
joins only later source-verified close observations and counts a paper session
only when all five production families have a valid feature payload, one
activated artifact, the required horizon, a recorded pre-close forecast, and a
verified close.

The paper shadow runner is deliberately separate from production prediction.
It loads only an explicitly enabled activation descriptor, rehashes the complete
activation-to-candidate-to-model chain, and requires embedded raw-tape,
verified-close, surface-replay, and training provenance to match exactly. It then
requires exactly one exact-horizon surface row and a finite timestamp-aligned
incumbent for each of the five families, runs the small live batch on CPU, and
atomically appends each forecast with its canonical feature payload. Paper
evaluation reopens that chain, independently recomputes every model output, and
rejects caller-supplied values that differ. It also resolves the retained catalog
cutoff, rehashes the exact raw prefix bytes, rebuilds the horizon surface with
point-in-time MarketPin data, and requires exact feature/reference/incumbent
semantics before a row can count. Missing, moved, ambiguous, mutated, late, or
partial evidence produces no promotable paper session rather than a fallback
prediction.

At the live analysis barrier, the first callback observed at or after the
deadline arms a boundary at the preceding callback. This prevents a coordinator
that wakes a few seconds late from admitting future-local information. The
recorder persists that exact sequence, cutoff byte count, and prefix SHA-256 and
also rejects a prefix whose last provider event exceeds the horizon, then
immediately releases the aggregation worker. Only when a paper candidate is
configured does an isolated worker copy exactly those bytes, verify the prefix
hash, replay contract rows, join point-in-time OI and incumbent/reference
evidence, build the v3 surface, and invoke the shadow runner. The temporary DBN
is always deleted; the original raw file plus cutoff ledger remain the
reproducible source. Bulk replay never runs on the ingestion reader or while its
aggregation barrier is held.

Frozen MLP artifacts use a bounded UTF-8 JSON contract rather than executable
pickle content. The loader validates the exact feature names and order,
preprocessing dimensions, finite scaler and target parameters, every linear
layer dimension, and all weights before constructing CPU or CUDA tensors. The
runtime gate hashes and parses the artifact, so arbitrary or merely hash-matched
bytes cannot be enabled as a prediction model.

`backend.closing_tape.production` is the production consumption boundary. It
refuses to load an artifact unless the complete manifest evidence gate passes,
then re-reads and re-hashes both manifest and artifact before inference. Live
inference requires exactly one exact-horizon row for each of SPX, NDX, RUT,
VIX, and SPY, one shared verified source hash, finite positive reference
prices, and the workload batch size authorized by the manifest. Outputs retain
the source hash, model/artifact identity, feature time, execution device, and
an explicit `is_estimate` label. A promoted CUDA runtime never silently falls
back to CPU because doing so would violate its benchmarked execution contract.
At the same immutable live-prefix barrier used for paper shadowing, a present
production manifest is revalidated through this boundary. A passing exact
five-family batch is appended atomically to `promoted_close_predictions` in
the MarketPin database before the target close. Update and delete triggers
make the ledger immutable; idempotent replay returns the original keys, while
conflicting values or post-close writes fail closed. The read-only endpoint
`GET /closing-tape/production-predictions` exposes those estimate-labeled rows
with their complete provenance and returns an empty list when none exist. An
omitted date means the current New York trading date—not the latest ledger date.
The response exposes requested/current dates and `is_current_session`; historical
valid batches remain auditable but cannot become the app's current prediction.

## Production forecast governance

The production path now records an `ATTEMPT_STARTED` event before copying or
replaying the live prefix. Every attempt must end as either `PREDICTED` or an
explicit `ABSTAIN`; a crash between those events remains an unresolved
opportunity in the scorecard rather than disappearing from the denominator.
`PREDICTED` is accepted only for the exact five keys written from the same
loader-authorized `PromotedPredictionBatch`. Five caller-supplied keys, a
partial family set, a cross-session row, a post-close issuance, or a batch
without full provenance cannot become production evidence.

The authorized batch retains the exact, ordered `MODEL_FEATURE_COLUMNS` values
for SPX, NDX, RUT, VIX, and SPY. Before the terminal event is committed,
governance reruns the loader-verified frozen artifact on those retained rows
using the promoted execution device. It compares log return, point estimate,
and both calibrated interval bounds with declared numerical tolerances. A
feature-hash change or output mismatch aborts the transaction and the live path
records an abstention. Successful replay evidence is stored in the additive,
append-only `closing_tape_promoted_prediction_governance` sidecar: the ordered
feature values and contract hash, issuance feature hash, model/calibration and
human-approval identity, replayed value, observed errors, tolerances,
verification time, replay-result hash, and record hash. This sidecar is keyed by
the immutable prediction key; existing prediction rows are not rewritten.

`GET /closing-tape/production-predictions` is a pure read. It never runs a model
and never returns an unregistered row from `promoted_close_predictions`. Each
returned row has `forecast_id == prediction_key`, `decision_grade=true`, and a
persisted `DETERMINISTIC_REPLAY_VERIFIED` result. Missing, incomplete, malformed,
unapproved, or non-replayed evidence returns no decision-grade rows. The
governance tables are initialized additively during API lifespan startup against
the already configured persistent SQLite database; unsupported or incompatible
storage fails visibly instead of silently touching a default live path.

Promotion remains a separate human action. The operator workflow requires the
reviewed proposal SHA-256, writes a content-addressed approval receipt, and only
then sends the receipt-bound manifest through the complete promotion gate and
atomic publication. Research and AI-generated challengers remain shadow-only:
they cannot create an approval receipt, publish a manifest, or promote
themselves.

After an official close bundle commits, the verified-close importer runs an
idempotent governance reconciliation for each affected trading date. Only
replay-verified, approval-bound production forecasts are eligible. No governed
forecast is a clear non-error result; a partial or otherwise unscoreable
eligible batch fails visibly after the close commit so an operator can rerun the
idempotent reconciliation. Legacy snapshot scoring remains opt-in by explicit
prediction ID.

The read-only scorecard reports the opportunity denominator, abstention rate and
reasons, verified-outcome resolution, interval coverage, and comparisons with
persistence by symbol, horizon, volatility regime, model, and exact combined
slice. Minimum sample sizes produce unavailable metrics rather than zeros.
Raw point MAE and RMSE are never pooled across SPX, NDX, RUT, VIX, and SPY;
mixed-family summaries are suppressed while comparable per-symbol slices remain
available.

Final fitting is separately guarded by the walk-forward report: an unpromoted
candidate cannot be packaged. The approved architecture and median
early-stopping epoch count are carried from held-out evaluation, preprocessing
is refit on all currently labeled rows for future-only use, and deterministic
JSON serialization produces the artifact SHA-256 consumed by the manifest.
Packaging alone never creates or enables a production manifest.

The artifact benchmark measures the complete callable path—DataFrame coercion,
imputation, scaling, device transfer, neural-network math, and result transfer—
on identical rows. It reports median latency, CPU-relative CUDA speedup, and
the maximum absolute prediction difference. Batch size is part of the evidence:
a GPU win on bulk research rows does not imply a win for five-family live
inference. A CUDA production manifest must therefore make its measured row
count equal the deployed inference batch size; evidence from a different batch
fails closed.

`tools/audit_closing_tape_readiness.py` scans every retained catalog through
read-only SQLite connections. It lists each session's exact exclusion reasons,
requires an immutable passing finalization audit and hash-aligned observed,
inferred, and OI evidence for all production families, and reports progress
toward the 10-session capture, 60-session model, and 20-session paper gates.
The project-local `data/closing_tape` tree is always included. Additional
date-partitioned archive roots can be declared with the platform-path-separated
`CLOSING_TAPE_CATALOG_ROOTS` environment variable; on Windows, for example,
`G:\MarketPinHistory;H:\MarketPinArchive`. Each root must contain
`<YYYY-MM-DD>/closing_tape.sqlite`. Missing or inaccessible configured roots,
unreadable catalogs, and duplicate eligible captures for one trading date are
reported and keep readiness gates false. Default research-surface and model
evaluation commands use the same discovery receipt, while an explicit repeated
`--catalog <sqlite-file>` list remains a complete override. This archive-root
setting does not relocate the live recorder, PID, DBN, or launcher paths; the
active-capture compute guard therefore continues to inspect only the
project-local live status tree.
For every eligible capture without a complete verified-close bundle, it also
emits a label work item containing the trading date, recorder session, immutable
source hash, families already verified, and families still missing. Partial
official evidence is retained and visible but cannot advance the model gate.
Every missing family is paired with the exact approved source identifier and
first-party reference domains accepted by the guarded artifact fetcher; URL
discovery remains explicit because an allowlisted domain alone does not prove a
page or download publishes the requested trading date.
Database hash syntax alone is not label evidence. Readiness resolves the latest
verified observation to exactly one deterministic vault artifact and stream-
rehashes its bytes; missing, ambiguous, oversized, or mismatched artifacts are
reported and the family remains in the label work queue. Surface construction
and model evaluation independently repeat this byte attestation before labels
can reach any CPU or CUDA training gate.
The model gate counts only trading dates where one eligible immutable tape and
one complete five-family verified-close bundle intersect. Clean captures and
verified closes that occur on different dates cannot make training ready.

`tools/evaluate_closing_tape_models.py` performs an even cheaper first-stage
gate before reading any contract-minute tables. It loads only the verified-close
ledger and requires 60 complete five-family bundles. If that gate fails, the
command emits the per-family label counts, records that surface construction was
skipped, returns nonzero, and never imports CUDA or scans the multi-million-row
TCBBO research surface. Capture/label overlap, exact-horizon feature coverage,
walk-forward folds, and CUDA evaluation remain later gates once label sufficiency
is real.

The research-surface builder and model evaluator also refuse CPU/GPU-heavy work
while a fresh recorder `status.json` identifies a running session and feed. If
the heartbeat is stale, the guard still protects the compute window while the
recorded PID remains live; this covers recorder-monitor pauses without opening a
false post-close window. The guard prevents feature reconstruction or CUDA
training from competing with immutable DBN capture by default. An operator can use
`--allow-active-capture` only as an explicit override, and the resulting payload
records both the override and the active-session identity.

`backend.closing_tape.surface_artifact` freezes completed research surfaces as
content-addressed Parquet plus a strict manifest. The Parquet is treated as a
selection claim, not as self-authenticating evidence. Replay-verified loading
independently resolves every claimed trading date/session/feed/source hash to
exactly one retained catalog. Live sources are rehashed and decoded from raw DBN
bytes and checked against finalization counters; historical sources re-run the
bundle-manifest/component verifier. The selected sessions are rebuilt from
read-only catalogs and the point-in-time market database using the manifest's
price-age rule, and every column/value must be semantically identical to the
frozen surface. Missing raw bytes, an ambiguous catalog, or any mismatch fails
closed. Packaging and explicit CUDA evaluation require this replay-verified
surface object even when called directly from Python; `auto` on a bare DataFrame
is forced to CPU and cannot silently enter CUDA.

An independent capture session means one trading date, not one recorder launch:
multiple otherwise eligible retries on the same date are rejected as ambiguous
rather than counted as extra source evidence or repeated close labels. Training
coverage is likewise counted by unique trading date, while still requiring a
distinct finalized source hash for every admitted date.
The latest producer-retained report is available at `GET /closing-tape/readiness`
and is rendered in the dashboard as independent-session progress—not raw-record
progress—plus the latest session's explicit exclusion reasons. API startup owns
the asynchronous catalog refresh and snapshot write; the GET is a pure retained
evidence read and disables every readiness gate while that evidence is stale.

After the configured options-session stop time, the existing watchdog invokes
`tools/finalize_closing_tape_if_due.py` when no verified recorder remains. The
tool is read-only before its time gate, uses the same exclusive recorder lock
during finalization, and no-ops once a passing audit already matches the source
hash. A failed evidence contract is surfaced to the watchdog instead of being
silently promoted.

When the separate live GEX backend lacks a current-day universe cache, startup
now tries the bounded, family-complete prior-session scaffold before making a
potentially slow historical availability request. The scaffold never makes a
prediction valid by itself—current-generation live quotes remain required—and
its refresh interval is eight hours to avoid repeatedly tearing down a healthy
regular-session stream.

At the initial 2026-08-25 implementation check, the canonical MarketPin
database had retained point-in-time price snapshots for SPX, NDX, and VIX, but
not RUT or SPY. A full-family live capture was then started the same day. It is
still unfinalized, so no real out-of-sample accuracy claim or model promotion
is possible yet.

An isolated live smoke capture subsequently verified the operational path for
about 80 seconds of SPXW traffic: 23,924 DBN records, 3,426 TCBBO trades, 20,496
symbol mappings, zero unmapped trades, zero reconnects, zero slow-reader
warnings, and a finalized SHA-256. Its three SPX minute-feature rows joined to
valid point-in-time MarketPin prices and produced two exact one-minute labels.
This proves capture-to-label plumbing only; it is not enough data to measure or
claim forecast accuracy.

The FastAPI `/closing-tape/status` contract and Streamlit's observed/inferred
panel expose capture integrity, feature coverage, inference method/version,
open-interest coverage, closing-analysis state, and the frozen-model promotion
gate. Missing evidence is rendered as unavailable rather than zero, and a
running tape is explicitly unfinalized until its post-close integrity check.
On Windows the recorder also normalizes Task Scheduler's inherited process
priority to `Normal` and publishes the before/after priority evidence in
`status.json`; a failed normalization remains visible instead of being assumed.
The live monitor retries bounded SQLite `busy`/`locked` contention caused by the
concurrent close-minus-15 analysis and measures scheduler pauses between completed
monitor ticks, so its own persistence work cannot manufacture a sleep warning.

## Measured CUDA role

`backend.closing_tape.compute_benchmark` measures representative batched MLP
training throughput and explicitly sets `accuracy_claim=false`. On the local
RTX 5080 Laptop GPU with PyTorch 2.10.0+cu128, a 64-feature, 4,096-row-batch
verification run processed approximately 1.59 million samples/second on CUDA
versus 0.39 million on CPU, a 4.06x speedup. The reusable gate recommends CUDA
for this training workload because it exceeded the configured 1.25x threshold.

This result does not change the recorder design and does not promote a model.
Raw capture, symbology, integrity checks, and SQLite remain CPU/I/O work. CUDA
becomes relevant after enough complete sessions exist to train the same
candidate architecture inside the purged walk-forward evaluator.

`backend.closing_tape.torch_research` now evaluates that MLP on the same outer
test sessions as ridge and persistence. The last eligible training session is
reserved for chronological validation, preprocessing and target scaling fit on
earlier rows only, and early stopping never observes the outer test rows. A
candidate cannot promote with fewer than 60 integrity-verified sessions, fewer
than five walk-forward folds, or without beating both ridge and persistence.

Close-horizon evaluation is also aligned explicitly: TCBBO features and the
contemporaneous MarketPin `predicted_close` are compared with the later scored
close on the same return target. The production close loader accepts only SPX,
NDX, RUT, VIX, and SPY by default so synthetic test symbols cannot become model
evidence. At the latest check, the retained database had zero scored production
closes; a real current-versus-TCBBO accuracy comparison therefore remains
unavailable rather than being estimated from fixtures.

## Immutable official-close evidence

The legacy `eod_closes` row remains the current projection used by existing
scoring, but every new submission now first enters append-only
`eod_close_observations`. Each observation has a deterministic idempotency key,
source, source reference, observed and ingested timestamps, verification flag,
and optional correction lineage. A corrected value creates a new observation
linked to the prior record; it does not overwrite the original evidence.

Public/manual API submissions are always unverified. The operator-only
`tools/ingest_verified_closes.py` validates an entire CSV before writing,
requires timezone-aware observation timestamps and source references, and
enforces the expected authority for each family: Cboe Global Indices or S&P
Global for SPX, Nasdaq for NDX, Cboe Global Indices or FTSE Russell for RUT,
Cboe for VIX, and NYSE Arca for SPY. Only verified
references must be HTTPS URLs on the corresponding authority's official domain;
an operator note or third-party URL cannot become verified evidence. Every verified
row also binds the exact retrieved source artifact with a required SHA-256, so a
later webpage or download change cannot silently alter the evidence identity. Only verified
production observations are eligible for research labels, and their count is
exposed through the closing-tape API and dashboard panel. A verified-close
*session* is counted only when one trading date has verified positive closes
for all five production families (SPX, NDX, RUT, VIX, and SPY). Partial close
evidence remains retained, but it cannot advance the session readiness gate.
The operator importer rejects incomplete bundles before writing. A partial
correction requires `--allow-partial-correction`, and every submitted row must
carry an explicit `correction_of_id`.
All validated rows are then appended with their current `eod_closes` projections
inside one database transaction. Any constraint, lineage, or persistence failure
rolls back every family; scoring begins only after the full bundle commits.

The CSV also requires `source_artifact_path`. Before any database write, the
operator tool streams the local file through SHA-256, rejects a mismatch or an
empty/oversized artifact, and atomically retains the exact bytes under
`data/verified_close_sources/<date>/<symbol>/<sha256>.<ext>`. Repeated imports
reuse the identical content-addressed file; a conflicting vault file fails the
operation.

`tools/fetch_verified_close_artifact.py` provides the guarded acquisition step.
It accepts only the symbol's approved authority and HTTPS domain, revalidates
the final URL after redirects, requests identity encoding, streams with a hard
100 MiB default limit, fsyncs the temporary file, and atomically publishes the
content-addressed artifact. It emits strict JSON containing the final URL,
retrieval time, byte count, SHA-256, and vault path, and never writes a close or
scores a prediction.

`tools/fetch_verified_close_bundle.py` is the five-family post-close acquisition
path. It is blocked until 18:00 America/New_York unless an operator explicitly
overrides the timing guard, then captures Cboe's exact SPX and RUT daily-history
CSVs, Nasdaq NDX historical JSON, Cboe VIX history CSV, and NYSE Arca SPY
history JSON into the immutable vault. It derives each close from the structured
family-specific row, applies the same semantic validators as ingestion, writes an
atomic candidate CSV, and runs the read-only full-bundle audit. It deliberately
does not ingest the database or score predictions. Previously retained S&P Global
rendered SPX evidence and FTSE Russell daily PDFs remain valid under their original
source identities; the default Cboe histories also support exact-date backfill.

`tools/audit_verified_close_bundle.py` is the read-only boundary between fetched
bytes and labels. Given a candidate CSV, target trading date, and bundle profile,
it rehashes every artifact and runs the same source and semantic validators as
the importer. Each required family is reported as `SEMANTICALLY_VALID`, `STALE`,
`INVALID`, `DUPLICATE`, or `MISSING`; only an all-valid bundle reports `READY`.
The audit command returns a nonzero exit status for every non-ready bundle and
cannot stage artifacts, insert close observations, update projections, or score
predictions. This prevents a current URL that still publishes a prior session
from being mistaken for same-date model evidence.

SPY close evidence uses NYSE's public first-party quote JSON endpoint for the
NYSE Arca listing. The semantic validator accepts only the exact SPY endpoint
and requires the retained JSON to identify both `SPY` and exchange `ARCX`, then
binds the requested trading date and close to one row in
`quoteHistory.historyList`. A generic NYSE page, a different exchange, or a
matching number elsewhere in the response fails closed. NYSE's entitlement-
based TAQ Closing Prices file remains a compatible higher-grade source option
for future acquisition work.

## Recorder recovery and runtime provenance

`start_closing_tape.ps1` launches only through the repository's application
venv and writes each launch attempt to timestamped stdout/stderr files. A
recovery therefore preserves the failed attempt's evidence instead of
truncating a shared daily log. Status payloads record the Python executable,
environment prefix, base prefix, and whether a venv is active.

Status publication uses a process/thread-unique temporary file and bounded
Windows sharing-violation retries. A dashboard or operator reading
`status.json` can delay one status update, but it cannot terminate raw DBN
capture. The 2026-08-25 live recovery was exercised with repeated concurrent
status reads while records, trades, statistics, and definitions continued to
advance with no slow-reader warning or queue accumulation.

Live status keeps callback-derived TCBBO, timestamp, and valid-NBBO counters under
explicit `provisional_tcbbo_*` names. The authoritative `tcbbo_*` counters remain
terminal DBN-scan evidence and therefore stay zero while the file is open. At
shutdown, all three provisional counts must equal the independently decoded raw
DBN counts; a mismatch makes the session incomplete. This exposes live coverage
without relabeling a mutable aggregation as immutable file truth.

The existing five-minute MarketPin watchdog now also supervises the recorder.
It verifies the PID evidence belongs to this repository and recorder module,
checks that `status.json` is fresh and running, and leaves a verified but
degraded writer untouched for operator inspection rather than risking tape
truncation. When no verified writer exists, it invokes the idempotent launcher.
Automatic launches are deferred until 10:35 ET, five minutes after the
60-minute opening range is complete. The recorder then requests its TCBBO replay
from the 09:30 ET cash open, preserving the full-session tape while keeping its
broad second OPRA client out of the opening gamma and ORB capture window.

For the guarded RUT opening canary, the live-subscription ceiling is 3,200
contracts. On the 2026-09-04 current-day universe this retained every primary
and next-listed call/put pair for SPX, NDX, VIX, and RUT with zero reservation
shortfall, while trimming 400 far-shadow contracts. The four-family plan is
therefore no larger than the measured 3,204-contract core-only plan; the live
canary still rolls RUT back if SPX/NDX freshness, lag, or coverage deteriorates.
The launcher refuses to create a misleading partial session at or after the
close-minus-15 analysis cutoff and preserves one log pair per recovery attempt.

Offline finalization records per-stage wall-clock timings and record throughput
for integrity, open-interest replay, minute construction/persistence, and parity
construction/persistence. The minute rebuild reuses the finalizer's matching
verified integrity report, avoiding a redundant full DBN boundary scan and
SHA-256 pass without weakening the source-hash check. These measurements decide
whether future optimization belongs in DBN decoding, pandas aggregation,
SQLite persistence, CPU parallelism, or a genuinely tensor-shaped CUDA stage.
TCBBO rows are canonically ordered by immutable event/receive timestamps and
stable record fields before aggregation. Minute first/last prices therefore do
not change when the provider delivers the same trades in a different callback
order during a recovery replay.
The `tcbbo-observed-v7` contract also requires OCC symbol fields to be parsed
after canonical ordering resets the row index. This prevents expiration,
option type, and strike metadata from being realigned onto a different trade
by pandas index labels. A passing v7 rebuild verifies the persisted contract
fields against `raw_symbol`; older derived contract rows are not admitted by
readiness even though their immutable DBN source remains valid.
If a historical catch-up burst starves only the recorder's local monitor
thread, the incident remains in the append-only finalization audit as an
operational warning. It can be cleared from the current evidence projection
only after a full raw-DBN integrity scan and deterministic minute rebuild prove
there was no provider slow-reader message, callback drop, reconnect, or other
derived failure. Those actual loss signals remain hard exclusions.
After acquiring the exclusive process lock, a replacement recorder marks any
orphaned `running` catalog sessions explicitly `incomplete`; raw DBN files are
never altered by this reconciliation.

`tools/finalize_closing_tape.py` provides the corresponding post-close repair
path. It must acquire the same exclusive recorder lock, refuses DBN paths that
escape the trading-day directory, recomputes the file hash and TCBBO evidence,
replays immutable OI observations, aligns inferred rows to the finalized hash,
and updates the feed projection idempotently. It cannot run beside the live
writer. Missing mappings, schema replays, definitions, statistics, family
coverage, or minute evidence remain explicit failures; an expensive minute
rebuild requires the separate `--rebuild-minutes` flag.
Every attempt is also written to append-only `tape_finalization_runs`, keyed
deterministically by the source hash, evidence-contract version, rebuild mode,
and result. This preserves the recorder's prior status, error, and gap evidence
even though `tape_feed_status` remains the current-state projection; retrying
the identical finalization does not duplicate the audit record.
