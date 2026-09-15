# MarketPin Market Data Lifecycle and Storage Best Practices

- Status: proposed operating standard and current-state reconciliation
- As of: 2026-09-03
- Applies to: the canonical local MarketPinPredictor checkout
- Operating timezone: `America/Chicago`; persisted timestamps remain UTC

## 1. Purpose

This document defines how MarketPin should ingest, preserve, validate, transform,
archive, replay, and use both live and historical market data at multi-year scale.
It also records what the application currently does so that a proposed storage or
processing improvement is not mistaken for an implemented safeguard.

The central decision is:

> Do not build one enormous SQLite database. Preserve immutable provider files by
> trading date, use small operational catalogs for control and audit, and publish
> compact versioned Parquet datasets for research and training.

The system must remain usable as history accumulates without weakening the live
session, obscuring provenance, introducing hindsight leakage, or making one local
disk the only copy of irreplaceable evidence.

### Status labels

- **CURRENT** — confirmed in the repository as of the date above.
- **TARGET** — required design or behavior that is not yet fully implemented.
- **POLICY DEFAULT** — a proposed operating value that may be deliberately changed.
- **BLOCKER** — prevents evidence from becoming training- or promotion-eligible.
- **FORBIDDEN** — a shortcut that would invalidate provenance, replay, or safety.

### Contents

1. Purpose and non-negotiable invariants
2. Current live, closing-tape, historical, and legacy behavior
3. Target architecture and storage tiers
4. Required data and schema contracts
5. Live-data operating practices
6. Historical acquisition, import, and throughput practices
7. Anti-leakage requirements
8. SQLite, Parquet, and JSON usage
9. Cloud archive, local cache, capacity, and retention
10. Monitoring, recovery, security, and operational runbooks
11. Prioritized implementation plan and acceptance criteria

## 2. Non-negotiable invariants

1. **Raw provider evidence is authoritative.** Derived SQLite rows, Parquet files,
   API payloads, charts, and predictions are reproducible projections of raw data;
   they are not replacements for it.
2. **Raw data is immutable.** Never edit, normalize, truncate, or overwrite a
   finalized provider file. Corrections produce a new object and manifest.
3. **Observed and inferred data remain separate.** A trade and its pre-trade BBO
   are observed. Quote-position classification, aggressor estimates, gamma, max
   pain, and model output are inferred or derived and must carry method/version
   metadata.
4. **Every artifact has lineage.** Any user-visible value must trace to a trading
   date, session, provider, dataset, schemas, request window, source hashes,
   transformation version, and availability time.
5. **Time is point-in-time.** Persist event time, receive time, ingest time, and
   feature availability in UTC. Convert to Central time only for operator display.
6. **Invalid input fails closed.** Missing, stale, cross-generation, incomplete, or
   malformed evidence yields `ABSTAIN`, `STALE`, `UNAVAILABLE`, or an explicit
   research-only state—not a plausible fallback price or zero-filled calculation.
7. **Live and historical workloads are isolated.** Historical download, replay,
   surface construction, and CUDA work must not contend with the live provider
   reader, live recorder, or active-session SQLite writer.
8. **Publication is content-addressed and idempotent.** Retrying the same verified
   input produces the same identity and does not duplicate eligible sessions.
9. **One trading date counts once.** Multiple eligible captures for the same date
   are ambiguous until explicitly reconciled; recorder retries are not extra
   out-of-sample evidence.
10. **Acquisition is not readiness.** A valid provider response can still lack
    coverage, labels, or point-in-time evidence required for modeling.
11. **Cost and capacity are preconditions.** No bulk execution begins without a
    reviewed provider estimate, local-space estimate, and explicit ceilings.
12. **Deletion requires proof.** Local evidence is not evicted until a durable copy,
    its manifest, its checksum, and a representative restore/replay have passed.

## 3. What MarketPin does today

### 3.1 Current data planes

| Plane | Current input and purpose | Current persistence | Authority |
|---|---|---|---|
| Live GEX and product plane | Databento `OPRA.PILLAR`, normally bounded `cbbo-1s` data for the active SPX/NDX/VIX universe | Generation-tagged memory cache, `data/market_data.db`, valid NDJSON exports, audit JSON, and periodic compressed calculation inputs | Derived product state; not a complete event archive |
| Closing-tape plane | Independent `tcbbo`, `statistics`, and `definition` subscriptions for nine explicit option parents | `data/closing_tape/<date>/opra_options.<session>.dbn`, `closing_tape.sqlite`, `status.json`, lock/PID evidence, and runtime logs | Raw DBN is the live research/replay truth |
| Historical acquisition plane | Databento Historical `definition`, `statistics`, and `tcbbo`, one immutable three-file bundle per session | `data/databento_history/<date>/*.dbn.zst` plus `manifest.v2.json` | Verified bundle and attestation are historical raw truth |
| Historical catalog plane | Deterministic import of a verified historical bundle | `data/closing_tape/<date>/closing_tape.sqlite` alongside any retained live session for that date | Rebuildable observed/inferred ledgers and readiness state |
| Research surface plane | Eligible catalogs plus verified labels | Content-addressed Parquet and a strict manifest | Rebuildable training/evaluation input |
| Shadow research plane | Experimental formulas and challengers | `data/shadow_research.db` | Research-only; never production authority |
| Legacy Polygon/Massive plane | Aggregate bars, old gamma snapshots, derived exports, and daily option aggregates | Root CSVs, root legacy SQLite files, `exports/`, `logs/audit/`, and `historical_data/options/` | Separate legacy evidence family; never Databento-equivalent raw tape |

The canonical product database is `data/market_data.db`. Root files such as
`market_predictor.db`, `gamma_analysis.db`, and `market.db` are legacy or
noncanonical unless an explicit migration proves otherwise.

### 3.2 Live GEX and prediction handling

**CURRENT:**

- The provider callback performs bounded parsing and validation, tags accepted
  quotes with the active subscription generation, and moves calculation and
  persistence work to a separate compute worker.
- A reconnect creates a new generation, clears fresh-quote state, enters a warming
  handoff, and blocks publication until current-generation quotes arrive.
- Quotes must match the current generation and subscription cutoff and remain
  within the freshness window.
- A generation change during calculation invalidates the result before persistence
  or publication.
- Queue-full, slow-client, skipped-record, and reconnect evidence is retained and
  expensive calculation is suspended when transport health is compromised.
- Same-day SPX/NDX expiration remains primary. Later expirations remain shadow
  context and cannot silently replace stale or missing 0DTE evidence.
- Periodic calculation inputs include chain rows, parameters, formula versions,
  universe metadata, rejection counts, and output summaries. They are canonicalized,
  SHA-256 hashed, compressed, and stored in `gamma_calculation_runs` and
  `gamma_calculation_input_blobs`.
- Valid snapshots append to `exports/<SYMBOL>/<UTC-date>.ndjson`. Valid and invalid
  calculations are retained in audit JSON and `gamma_audit_snapshots`; invalid
  rows cannot overwrite a valid gamma-pin bucket.
- Prediction snapshots commit to SQLite first. Prediction NDJSON is a best-effort
  projection, not the authoritative commit.
- `data/market_data.db` enables WAL, foreign keys, a busy timeout, and automatic WAL
  checkpoints.
- Lifecycle and passport logic strip numerical forecasts from abstain/stale/
  unavailable states and reject fallback, invalid, stale, or cross-generation
  input.

**CURRENT GAPS:**

- The live GEX path retains periodic point-in-time inputs, not every CBBO event.
  Only the separate closing-tape DBN is a complete retained raw session for its
  subscribed schemas.
- Some audit/export/input persistence failures are logged as best-effort failures
  and do not by themselves fail the API readiness state. The full operator gate
  must therefore check audit and database advancement separately.
- Current audit filenames use second precision in part of the streamer path and can
  collide. **TARGET:** use a collision-proof timestamp plus session/generation/hash.
- The API health endpoint is narrower than full session health. It does not replace
  two-sample message advancement, audit checks, WAL/`quick_check`, and persistence
  advancement.
- The live historical-context adapter still reads legacy CSVs rather than the new
  Databento historical catalogs. **TARGET:** publish a small versioned offline
  feature artifact for live consumption; never query a multi-year archive from the
  latency-sensitive provider loop.
- Ad-hoc launches can resolve a legacy database path differently from the canonical
  launcher. **TARGET:** expose the resolved absolute database identity in health and
  fail startup if required processes disagree.

### 3.3 Closing-tape live handling

**CURRENT:**

- Raw DBN is written before callback-derived processing and remains authoritative.
- The provider callback performs a nonblocking enqueue into a bounded 250,000-record
  queue; aggregation and SQLite writes occur off the provider event loop.
- Live output is date-partitioned under `data/closing_tape/<date>/`.
- The recorder defaults to a 10-second flush interval, 8 GiB free-space floor, and
  5 GiB per-feed file cap.
- A single-writer lock prevents overlapping writers for the same session directory.
- Live provisional counters remain distinct from terminal counters produced by an
  independent DBN scan.
- Terminal finalization validates subscription acknowledgements, replay completion,
  record framing, timestamps, valid two-sided BBO coverage, definitions, open
  interest, mappings, family coverage, close-window advancement, provider errors,
  reconnects, slow-reader warnings, callback drops, and the final file SHA-256.
- Queue overflow can make derived live output incomplete while preserving the raw
  DBN for an offline deterministic rebuild.
- Recovery marks orphaned `running` sessions incomplete without altering raw DBN.

**TARGET:** after terminal finalization, archive the raw DBN and manifest off-host,
verify the remote checksum, publish normalized Parquet, and make local retention a
cache policy rather than the only copy.

### 3.4 Historical handling

**CURRENT:**

- Acquisition is separate from the live recorder.
- The default bulk command is a metadata/condition/cost/billable-size dry run and
  makes no time-series request.
- A day requires a complete bundle of `definition`, `statistics`, and `tcbbo` for
  the canonical nine parents: `SPX.OPT`, `SPXW.OPT`, `NDX.OPT`, `NDXP.OPT`,
  `RUT.OPT`, `RUTW.OPT`, `VIX.OPT`, `VIXW.OPT`, and `SPY.OPT`.
- Execution requires the exact reviewed plan or batch hash plus explicit local cost
  and billable-byte ceilings. These are client preflight controls, not provider-side
  billing limits.
- Provider condition/revision is rechecked around each request.
- Components download to unique partial files, are decoded, validated, flushed,
  fsynced, atomically promoted, and hashed. Failed components remain diagnostic-only
  and cannot be discovered as complete evidence.
- The final manifest binds request identity, provider revision, schemas, parents,
  component hashes, mapping identity, record/coverage evidence, planning estimates,
  provenance, and a root attestation.
- A matching complete bundle is fully reverified and reused instead of downloaded
  again.
- Import refuses to compete with an active live recorder, preflights temporary/
  catalog/WAL capacity, materializes components in a temporary directory, imports
  point-in-time definitions and open interest before TCBBO features, and removes
  temporary raw files after verification.
- Completed imports are content-addressed and idempotent. Failed attempts remain in
  the finalization ledger and can be retried against the same bundle.
- Readiness distinguishes acquisition/import state, immutable capture eligibility,
  verified-close work, and model-training readiness. Bulk completion never trains or
  promotes a model automatically.

**CURRENT BLOCKERS AND SCALE GAPS:**

1. Historical acquisition accepts only 2024-01-01 through 2026-12-31 because the
   checked-in market calendar does not cover 2023. A rolling three-year request in
   September 2026 reaches 2023 and fails closed.
2. If the shared calendar import fails, one helper currently falls back to empty
   holiday/early-close sets. **TARGET:** calendar load failure must fail closed.
3. Planning, per-day downloads, the three per-day schemas, and imports are serial.
4. Full decoding/validation occurs multiple times across download, discovery, bulk
   verification, and import. Boundary verification must remain, but redundant full
   scans should be replaced only by trustworthy content-addressed receipts.
5. Resume is component/day-level, not byte-range-level. An interrupted component
   restarts that component.
6. Execution journals exist only after execution begins; there is no retained
   reviewed three-year dry-run artifact today.
7. Bulk billable-byte ceilings do not represent the final expanded SQLite,
   temporary, WAL, Parquet, and cache footprint for the entire job.
8. Historical DBN, journals, catalogs, and Parquet are local-only. Object-store
   upload, retention, restore verification, and safe cache eviction are not wired.
9. Some completion checks bind to an absolute local manifest path. Durable identity
   should be the bundle/attestation hash plus an immutable object URI; a local path is
   cache metadata.
10. Failed diagnostic files have no automated retention lifecycle.
11. Definition observations have database-level mutation guards; open-interest
    observations are append-only by application behavior but do not yet have equal
    database-level mutation triggers.

### 3.5 Legacy Polygon/Massive data

The legacy corpus is useful but must remain a separately versioned source family:

- `historical_market_data_3years.csv` contains price aggregates suitable for price,
  regime, and seasonality research. It is not raw options evidence and currently has
  empty volume/VWAP/transaction fields.
- `exports/<SYMBOL>/<date>.ndjson`, root SQLite files, and `gamma_snapshots.csv`
  contain derived gamma/prediction snapshots, not replayable option tape.
- `historical_data/options/` contains a large set of daily contract aggregates.
  Sampled SPX/NDX/RUT/DJI date files appeared duplicated and not correctly
  symbol-filtered. Treat the approximately 123 GB tree as unclassified until a full
  hash/content inventory is complete.
- Raw Polygon option trades, quotes, definitions, and open-interest observations were
  not retained in the same form as Databento closing-tape DBN.

**FORBIDDEN:** relabel legacy aggregates or derived snapshots as Databento TCBBO,
fill missing historical microstructure with later live data, or allow a legacy source
to silently satisfy a decision-grade five-family readiness gate.

Before any cleanup, create a read-only inventory, compute SHA-256 hashes, group exact
duplicates, inspect filename-versus-content identity, preserve one canonical copy,
and verify a second durable copy. Deletion remains a separate user-approved action.

## 4. Target architecture

```text
                           CONTROL PLANE
  plans + approvals + cost ceilings + job journal + cache index + readiness
                                   |
             +---------------------+---------------------+
             |                                           |
      LIVE ACQUISITION                           HISTORICAL ACQUISITION
  Databento -> raw daily DBN               metadata/cost dry run -> approval
             |                                           |
             +------------- immutable raw ---------------+
                                   |
                    BRONZE: DBN/Zstd + manifests
                     versioned private object storage
                                   |
                      verified bounded local cache
                                   |
          definitions + OI + TCBBO deterministic normalization
                                   |
             SILVER: partitioned observed/inferred Parquet
                                   |
       verified closes + exact decision horizon + feature contract
                                   |
          GOLD: content-addressed surfaces/evaluation artifacts
                                   |
              purged chronological evaluation and promotion gates
```

### 4.1 Storage tiers

| Tier | Contents | Preferred format | Mutability |
|---|---|---|---|
| Bronze/raw | Provider bytes, original manifest, provider revision, planning receipt, hashes | DBN/Zstd plus JSON manifest | Immutable |
| Silver/normalized | Typed observed ledgers and separately typed inferred records | Date-partitioned Zstd Parquet | Immutable and contract-versioned |
| Gold/research | Exact-horizon surfaces, verified labels, fold assignments, evaluation inputs | Content-addressed Parquet plus manifest | Immutable |
| Control/hot state | Active recorder state, locks, attempts, recent projections, job/cache indexes | Small local SQLite and JSON status | Mutable only while active; finalized snapshots become read-only |

SQLite remains appropriate for active-session coordination and small per-day audit
catalogs. It must not become the primary multi-year analytical fact store. DuckDB or
Arrow may read partitioned Parquet for offline analysis, but neither belongs on the
live provider callback.

### 4.2 Recommended object layout

```text
bronze/
  provider=databento/
    dataset=OPRA.PILLAR/
      trading_date=YYYY-MM-DD/
        bundle=<attestation_sha256>/
          schema=definition/source.dbn.zst
          schema=statistics/source.dbn.zst
          schema=tcbbo/source.dbn.zst
          manifest.v2.json

silver/
  table=tape_observed_contract_minute/
    contract=<version>/trading_date=YYYY-MM-DD/part-<sha256>.parquet

silver/
  table=tape_inferred_contract_minute_flow/
    method=<method>/version=<version>/trading_date=YYYY-MM-DD/
      part-<sha256>.parquet

gold/
  surface=contract-surface-v4/
    horizon=close-minus-15/artifact=<sha256>/
      surface.parquet
      manifest.json

quarantine/
  trading_date=YYYY-MM-DD/attempt=<attempt-id>/
```

Do not partition by contract, strike, or individual minute; that creates millions of
small objects. Partition primarily by trading date, table/schema, and contract
version, with family or hour only when measured query behavior justifies it. Sort
contract-minute data by family, expiration, raw symbol, and minute. Target reasonably
large Parquet objects and row groups rather than one file per small unit.

Object stores do not provide filesystem rename semantics. Upload data objects to a
staging prefix, verify their checksums, and publish the final manifest/commit marker
last. Readers must ignore a partition without the final verified manifest.

## 5. Required data contract

Every persistent record or artifact must expose or inherit:

- `source_kind`: Databento live, Databento historical, Polygon aggregate, legacy
  derived, official close, synthetic fixture, or another explicit source.
- Provider, dataset, schema, input symbology, output symbology, and explicit parents.
- Exchange trading date and the authoritative calendar/close-policy version.
- Event timestamp and receive timestamp in UTC nanoseconds when supplied.
- Ingest timestamp and `available_at` timestamp.
- Session ID, feed name, source SHA-256, and record offset or deterministic source
  key.
- Evidence-contract version and schema hash.
- Transformation method/version and code revision for normalized, inferred, or
  derived rows.
- Units, fixed-point scale, nullability, and semantic type.
- Quality state and machine-readable reason codes.
- Input hashes, output hash, parameters, dependency-lock identity, row counts, and
  completion timestamps for every transformation.

Preserve provider fixed-point integers where practical and define explicit scales for
normalized prices and strikes. Never use numeric zero to mean unknown, missing, not
applicable, stale, or unavailable. Historical operational counters that do not exist
must be null or `not_applicable`, not observed zeroes.

### Schema evolution

- Adding a nullable field without changing meaning is a minor contract change.
- A change in meaning, unit, type, availability semantics, or nullability requires a
  new major contract version.
- Never reinterpret an existing SQLite or Parquet column in place.
- Readers accept only explicitly supported contract versions.
- Golden fixtures, deterministic replay, migrations, and old-version rejection must
  be tested in CI.
- Observation ledgers should be protected against update/delete at both application
  and database levels after compatibility review.

## 6. Live-data best practices

### 6.1 Before connection

1. Resolve the exchange session, holiday/early-close rule, UTC request windows,
   parents, schema versions, disk reservation, and resolved database paths.
2. Fail closed if the authoritative calendar cannot load.
3. Acquire the date-level writer lock before migrations or output-file creation.
4. Verify that the canonical launcher owns the expected ports and database identity.
5. Reserve enough disk for the live raw file, WAL growth, status/audit output, and at
   least one recovery/finalization pass.

### 6.2 During capture

1. Write provider DBN before callback-derived work.
2. Keep the provider callback bounded and nonblocking. Never calculate Greeks, run
   CUDA, upload cloud objects, perform full-cache scans, or make slow SQLite calls on
   the provider event loop.
3. Tag every quote and calculation with its subscription generation. Discard any
   calculation spanning generations.
4. Bound queues and expose depth, peak, drops, derived lag, and raw-byte growth.
5. Persist observed aggregates separately from inferred classifications.
6. Keep current-day primary data separate from prior-session universe scaffolding and
   shadow future-expiration context.
7. Retain invalid diagnostic evidence but prevent it from updating valid production
   projections or training exports.
8. Publish status at least every flush interval with event/receive watermarks,
   subscription state, provider warnings, queue state, disk, and WAL evidence.

### 6.3 Decision horizon and finalization

1. Freeze the processed sequence and exact raw-byte cutoff at the decision horizon.
2. Hash the exact raw prefix and reject any feature input containing later event-time
   data.
3. On shutdown, stop intake, drain the queue, durably close/sync raw output, and run
   an independent DBN integrity scan.
4. Reconcile provisional callback counters with terminal raw-file counters.
5. Mark any mismatch, callback drop, slow-reader message, provider error, missing
   mapping/OI/definition, or missing family as incomplete.
6. Rebuild derived data from raw DBN when possible; never edit raw evidence to make a
   session pass.
7. Upload terminal raw evidence and its manifest, verify the remote checksum, and
   only then publish Silver data or consider local eviction.

### 6.4 Live health gate

Do not infer health from an HTTP 200 or an open port. A regular-hours `healthy` state
requires, at minimum:

- backend and dashboard reachability;
- active transport and generation-aligned handoff;
- message counts and raw bytes advancing across samples;
- fresh, valid current-generation SPX and NDX evidence;
- VIX reported separately when its chain is legitimately thin;
- no queue overflow, callback drop, slow-reader warning, provider error, or
  unexplained reconnect;
- current audit advancement and valid GEX identities;
- WAL mode, `PRAGMA quick_check = ok`, controlled WAL size, and required table
  advancement;
- enough free space for the remainder of the session and finalization;
- explicit abstention when any decision-grade gate fails.

## 7. Historical acquisition and import best practices

### 7.1 Plan before downloading

1. Query entitlement, provider condition/revision, schema availability, exact request
   windows, estimated cost, and estimated billable bytes without time-series access.
2. Use an authoritative exchange calendar; reject non-sessions rather than shifting
   them.
3. Bind the calendar version and close policy into the request/plan identity.
4. Persist the reviewed batch plan and approval receipt by hash. **CURRENT GAP:** the
   dry run prints its plan but does not write a durable three-year plan artifact.
5. Require per-day and aggregate cost/byte ceilings plus a provider-side monthly
   spending limit. Local ceilings are not provider billing controls.
6. Estimate compressed raw, temporary uncompressed, catalog/WAL, Parquet, local cache,
   and durable object-storage capacity separately.
7. Run a representative 10-session pilot before authorizing the complete interval.
   Include an ordinary day, expiration day, volatile day, and early close.

### 7.2 Acquire immutable daily bundles

1. Prefer month-sized operator checkpoints while retaining daily immutable bundles.
2. Download into a plan-specific staging directory under a per-date lock.
3. Recheck provider revision before and after every component request.
4. Flush, fsync, fully decode, validate, and SHA-256 each component.
5. Publish the three-file bundle only when all components share the required mapping
   identity and request contract.
6. Publish the manifest last. A partial file or sidecar is diagnostic-only.
7. Upload the raw bundle to private versioned object storage and verify remote bytes
   before local eviction.
8. Make retries idempotent. Reuse a verified component only when plan, request,
   provider revision, and hash identities match exactly.
9. Record cost, billable bytes, compressed bytes, provider latency, retries, and
   condition drift for every component.

### 7.3 Import and normalize

1. Run only when the live recorder and live data plane have a safe compute window.
2. Hydrate one verified bundle into a bounded local cache.
3. Verify manifest and component hashes again at the trust boundary.
4. Materialize temporary uncompressed files only when required and remove them after
   terminal verification/import.
5. Replay point-in-time definitions before open interest and TCBBO features.
6. Reconcile raw counts to normalized counts with explicit exclusion reasons.
7. Keep observations and inferred rows in different tables and Parquet datasets.
8. Publish Silver Parquet and a quality manifest.
9. Acquire verified official-close labels as a separate job.
10. Build Gold surfaces only after capture, availability, and label gates pass.
11. Mark each date terminally as eligible, quarantined, unavailable, or failed; never
    silently omit it.
12. Make each completed month usable independently. Do not wait for the whole
    three-year corpus before beginning validated research.

### 7.4 Concurrency and throughput

**CURRENT:** dates, schemas, and imports are serial.

**TARGET:** introduce bounded parallelism only after the pilot establishes provider
latency, temporary-space peaks, disk bandwidth, SQLite contention, and verification
cost. A safe initial policy is:

- two concurrent downloads for different dates;
- one import writer per physical volume;
- one writer per daily catalog;
- zero historical imports during the regular-hours live session;
- dynamic pause when live health, provider limits, free-space reserve, or WAL limits
  fail.

Do not remove repeated integrity checks merely for speed. Replace redundant full
decodes only when a signed/content-addressed validation receipt proves identical
bytes and the next trust boundary still performs an independent check.

CUDA is for suitably batched model training/evaluation after readiness. Raw capture,
DBN decoding, symbology, hashing, validation, SQLite, and small-file handling are
primarily CPU/I/O work.

## 8. Anti-leakage requirements

1. A feature may use only evidence whose `available_at` is no later than the feature
   cutoff.
2. Join open interest by receive-time availability. Do not use `ts_ref`, event time,
   or a later mutable projection to backfill an earlier feature.
3. Use point-in-time contract definitions and symbol mappings.
4. Preserve provider deletes and corrections as append-only observations. The latest
   eligible delete yields missing OI; do not resurrect an older value.
5. Reference prices use a backward as-of join with a declared staleness bound.
6. Exact decision-horizon equality is required. A later nearest row is not a
   substitute.
7. Official-close labels are separately sourced, hashed, and timestamped. They never
   become features.
8. A label must become available after its feature row. The row must retain both
   timestamps.
9. Count independent trading dates, not launches, files, or raw-record volume.
10. Split and purge by whole chronological sessions. Apply embargo when features or
    labels span sessions.
11. Fit scaling, imputation, encoding, feature selection, calibration, and
    hyperparameters using training data only.
12. Calibration uses earlier out-of-sample residuals whose labels were already
    available.
13. Freeze and hash datasets, folds, parameters, artifacts, and reports.
14. Older price-only or lower-resolution Polygon features remain a separately named
    family with availability masks. Never forward-fill unavailable TCBBO features
    into older history.
15. Historical fallback remains context-only and cannot gain production authority
    because current live input is missing.

## 9. Database and file-format practices

### 9.1 SQLite

- Keep one active writer per daily catalog.
- Use WAL for active capture, a bounded busy timeout, foreign keys, and batched
  transactions.
- Use SQLite's online backup API for an active database. Never copy only the
  `.sqlite` file while its WAL is active.
- Run `PRAGMA quick_check` after finalization and before archive. Run full
  `integrity_check` periodically and after abnormal shutdown.
- Checkpoint the WAL before producing an archival database snapshot.
- Freeze finalized daily catalogs read-only.
- Record schema migrations explicitly; do not infer version only from columns.
- Keep the raw DBN as recovery authority if a derived catalog is corrupt.
- Do not place a writable SQLite database directly on object storage, a network URL,
  or an eventually consistent synchronized folder.
- Treat multi-year SQLite catalogs as compatibility/audit caches, not the primary
  analytical store.

### 9.2 Parquet

- Use Zstd compression and typed nullable columns.
- Partition primarily by date, table/feature contract, and source family.
- Avoid contract/strike-level partitions and tiny files.
- Record row counts, min/max timestamps, family coverage, schema hash, source hashes,
  transform version, and output SHA-256 in a manifest.
- Rehash on load and reject an unsupported feature contract.
- Prove dual-read parity against finalized SQLite before making Parquet the default
  research reader.

### 9.3 JSON/NDJSON

- Use JSON for manifests, status, compact audit receipts, and human inspection.
- Use NDJSON as an append-only projection or interchange format, not the sole source
  for a multi-year analytical scan.
- Every line requires a stable ID/source hash. A corrupt trailing line must not make
  earlier immutable records ambiguous.

## 10. Cloud archive and bounded local cache

### 10.1 Durable storage

Use a private S3-compatible object store or equivalent with:

- blocked public access;
- encryption at rest and TLS in transit;
- versioning and retention/object lock where licensing permits;
- explicit SHA-256 verification rather than trusting a multipart ETag;
- least-privilege writer, reader, and retention-administrator roles;
- access/audit logging;
- lifecycle policies subordinate to provider licensing and artifact references.

If training is local, prioritize predictable egress and a local NVMe cache. If
training moves to a cloud GPU, colocate compute and storage to control latency and
egress. The choice of vendor must not change object identities or lineage.

### 10.2 Cache behavior

1. Resolve an artifact through the metadata/catalog index.
2. Download to a unique `.partial` path.
3. Verify size, SHA-256, manifest identity, and expected compression/schema.
4. Fsync and atomically rename to the cache path.
5. Hold a lease while replay/import uses the artifact.
6. Evict only verified, unleased, restorable objects.
7. Log every eviction with source hash and durable object key.

**POLICY DEFAULT:** stop admitting new historical cache entries at 80% of the
dedicated cache budget and evict verified least-recently-used entries back to 70%.
The active live reservation and next scheduled import always take priority.

Size the cache from measured working sets rather than an arbitrary year count. It
must hold at least the largest expected daily bundle, its verified temporary
materialization, its expected catalog/WAL growth, two live-session reserves, and a
safety margin for the configured concurrency.

`CLOSING_TAPE_CATALOG_ROOTS` already permits additional local date-partitioned
catalog roots. It is discovery, not cloud archival. **TARGET:** add a hydration layer
or compact federated catalog so an object-store archive can be restored on demand
without pretending an object URL is a writable filesystem.

If the archive is unavailable, historical research pauses or uses already verified
local artifacts. Live capture continues independently. Cloud failure must never cause
silent substitution of stale historical evidence into the live forecast.

## 11. Capacity and performance planning

### 11.1 Measured pilot, not a universal ratio

The retained 2026-08-27 historical pilot contained approximately:

- 5.13 million decoded records;
- 396.7 MB provider billable/uncompressed estimate;
- 84.1 MB compressed historical components;
- roughly 1.22 GB for the date's SQLite catalog plus WAL at inspection time.

That catalog also contains an incomplete live session, so its 14.5-times ratio to the
compressed bundle is a date-volume snapshot, not a clean historical-only multiplier.
The successful persisted import attempt took about 85.5 seconds, while component
transfer/manifest publication was roughly 101 seconds in the observed pilot. Neither
is a guaranteed multi-year throughput rate.

### 11.2 Required estimates

For every approved batch, report independently:

```text
raw archive bytes       = sum(final compressed component bytes)
temporary peak          = max concurrent decompression/materialization footprint
catalog/WAL growth      = measured per-day retained normalized footprint
Parquet footprint       = measured Silver plus Gold compressed artifacts
local cache reservation = active live reserve + import peak + cache high watermark
durable archive         = raw + manifests + retained derived artifacts + redundancy
```

The existing importer conservatively reserves decoded working space, roughly
four-times decoded catalog amplification, temporary buffers, WAL headroom, and an
8 GiB reserve. Retain the conservative gate until a Parquet-first workflow is proven.

Before a full run:

1. Execute and retain a 10-session benchmark report.
2. Record provider latency per schema, compressed/billable ratio, validation passes,
   import throughput, peak temporary space, SQLite/WAL growth, Parquet size, and
   cloud upload/restore time.
3. Recompute cost and ETA from medians plus a high-percentile safety case.
4. Verify the whole job fits durable storage and the bounded local working set.
5. Process month by month and update the estimate from actuals.

## 12. Retention, deduplication, and deletion

All retention is subordinate to provider licensing and legal requirements.

| Artifact | Local policy | Durable policy |
|---|---|---|
| Active raw DBN and SQLite | Retain through finalization, archive, and restore verification | Preserve immutable raw while any research/model artifact cites it |
| Historical DBN bundles | Bounded verified cache | Preserve as canonical Bronze evidence for the approved retention period |
| Silver Parquet | Bounded hot cache | Retain supported contract versions and any version cited by Gold/models |
| Expanded daily SQLite | Preserve until Parquet parity and restore are proven | Optional compressed cold snapshot; not primary analytics |
| Gold surfaces, folds, evaluation, models | Keep active artifacts local | Retain while cited plus the configured governance period |
| Manifests, hashes, lineage, quality, deletion ledger | Always indexed/cached | Retain indefinitely because they are small and authoritative |
| Temporary uncompressed DBN | Remove after terminal verified import | Never archive |
| Failed/partial diagnostics | Quarantine with attempt metadata | Retain through incident review, then expire under an explicit policy |
| Runtime logs/status | Keep enough for operations and incidents | Retain summarized incident evidence longer than routine logs |

Eviction or deletion requires all of:

1. Durable object bytes and SHA-256 verified.
2. Manifest/attestation verified.
3. No active job lease.
4. At least one successful restore/replay for that artifact class.
5. Reference scan proves cited models/research remain reproducible.
6. SQLite is checkpointed/backed up correctly if applicable.
7. The action is recorded with operator, timestamp, source hash, object key, and
   reason.
8. Material deletion is explicitly approved by the user or established retention
   policy.

For irreplaceable raw evidence, maintain at least two independent durable verified
copies, with the local cache as a third transient copy where practical. Test a random
date restore and deterministic replay monthly; test metadata-catalog restore
quarterly.

## 13. Monitoring and operating thresholds

### 13.1 Live metrics

- status heartbeat age;
- raw-file byte growth and receive/event lag by family;
- subscription/replay state and active generation;
- queue depth, peak, drops, and derived lag;
- valid two-sided TCBBO ratio, mapping coverage, OI/definition/family coverage;
- reconnect, slow-reader, skipped-record, and provider-error counts;
- SQLite commit latency, busy retries, WAL size, and integrity state;
- audit/input/prediction persistence advancement;
- free space and predicted end-of-session reserve;
- finalization, archive, and Parquet-publication lag.

**POLICY DEFAULT warnings:**

- status older than twice the configured flush interval;
- queue above 50% for 30 seconds, critical above 80%;
- derived lag above 30 seconds, critical above 60 seconds;
- no raw-byte growth while an active feed is expected;
- archive not verified before the next session;
- projected reserve below two representative live sessions plus the largest pending
  import working set.

### 13.2 Historical metrics

- planned, blocked, downloaded, verified, archived, imported, quarantined, labeled,
  and training-eligible dates;
- per-component provider revision, request latency, retries, cost, billable bytes,
  compressed bytes, and hash status;
- throughput and ETA based on completed sessions;
- temporary/WAL peak and retained amplification;
- raw-to-normalized row reconciliation and exclusion reasons;
- upload and restore verification;
- duplicate-date/duplicate-bundle detection;
- verified-close work queue;
- provider and storage budget utilization.

Alert at deliberate budget thresholds such as 50%, 75%, and 90% of provider and
storage limits. A local estimate must not be described as a provider-enforced cap.

## 14. Recovery rules

| Failure | Required action | Forbidden shortcut |
|---|---|---|
| Interrupted historical component | Retain diagnostic partial, rerun the exact approved component, verify hash | Rename a partial file complete |
| Provider revision changed | Stop, produce a new plan, review and approve again | Reuse the old plan hash |
| Raw DBN passes but derived queue failed | Mark derived path incomplete and rebuild from raw under a new finalization record | Mark live derived rows complete |
| SQLite corruption | Preserve files, restore/rebuild from raw DBN, record incident | Edit rows until `quick_check` passes |
| Active WAL during backup | Use online backup/checkpoint workflow | Copy only the main `.sqlite` file |
| Duplicate eligible date | Block readiness and reconcile identities/evidence | Count both as independent sessions |
| Missing official close | Keep label pending and training blocked | Substitute an unverified close |
| Cloud archive unavailable | Pause hydration/history work; protect live capture | Use an unverified local or stale fallback |
| Disk reserve breached | Pause new historical work and protect live reservation | Delete unverified raw evidence |
| Calendar unavailable | Fail closed | Treat every weekday as an ordinary full session |

Recovery must be idempotent and append an attempt/finalization record. It must not
erase the prior failure evidence.

## 15. Security and licensing

- Keep provider credentials in the approved environment/secret mechanism only.
- Never include credentials in plans, manifests, status, logs, object metadata,
  screenshots, or backups.
- Rotate any credential found hard-coded in legacy source, remove it from active code,
  and review repository history before external sharing. `collect_historical_data.py`
  currently requires this remediation; never quote the credential in documentation.
- Use private encrypted buckets and least-privilege service identities.
- Separate writer, reader, and retention-administrator permissions.
- Hashes prove integrity; they do not encrypt sensitive/licensed data.
- Audit every destructive lifecycle action.
- Confirm provider terms for historical retention, backup regions, derived data, and
  redistribution before uploading or sharing data.
- Never expose raw licensed market data through the public API or UI without an
  explicit entitlement design.

## 16. Operational runbooks

### 16.1 Live day

1. Start the full stack through `start_databento_app.ps1`; do not independently
   launch mismatched backend/dashboard databases.
2. Verify process/listener ownership, backend/dashboard reachability, and the resolved
   canonical database path.
3. During regular hours, prove message/raw progression over at least two samples and
   apply the full live health gate.
4. Do not run historical import, surface construction, or training during active raw
   capture.
5. After close, finalize raw DBN, reconcile counters, run integrity/readiness, archive
   verified evidence, and record any gaps.

### 16.2 Historical dry run

```powershell
& .\.venv\Scripts\python.exe .\tools\run_closing_tape_historical_bulk.py `
  --start <YYYY-MM-DD> `
  --end <YYYY-MM-DD>
```

Review and retain:

- `batch_plan_sha256` and every `plan_sha256`;
- provider condition/revision;
- exact UTC windows and parents;
- blocked/already-eligible dates;
- estimated cost and billable bytes;
- local expanded/cached/archive capacity estimate;
- provider-side monthly limit.

### 16.3 Download-only phase

```powershell
& .\.venv\Scripts\python.exe .\tools\run_closing_tape_historical_bulk.py `
  --start <YYYY-MM-DD> `
  --end <YYYY-MM-DD> `
  --execute `
  --download-only `
  --approve-batch-plan-sha256 <REVIEWED_HASH> `
  --max-estimated-cost-usd <REVIEWED_CEILING> `
  --max-estimated-billable-bytes <REVIEWED_CEILING>
```

After each monthly checkpoint, verify all physical components and manifests and copy
them to durable storage. Do not delete local source files merely because the process
returned exit code zero.

### 16.4 Import and readiness phase

Run the same approved bulk command without `--download-only`. Verified bundles are
reused rather than downloaded again, then imported and audited.

```powershell
& .\.venv\Scripts\python.exe .\tools\audit_closing_tape_readiness.py `
  --project-root .
```

Review acquisition/import state, capture eligibility, label work, feature-contract
state, duplicate dates, and model readiness separately.

## 17. Prioritized implementation plan

### P0 — authorize the interval safely

1. Replace the 2024 calendar floor with an authoritative exchange-calendar and
   mixed index/equity-options close policy covering the entire requested interval.
2. Make calendar load failure terminal rather than weekday fallback.
3. Persist dry-run plans and operator approval receipts by hash.
4. Add a whole-batch retained-storage/temporary/WAL/cache capacity gate.
5. Run and retain a representative 10-session benchmark before the full order.
6. Rotate and remove the hard-coded legacy Polygon credential.

### P1 — archive before expanding

1. Add immutable object-store upload with checksum verification and manifest-last
   publication.
2. Add a cache index, `.partial` hydration, leases, high/low watermarks, and audited
   eviction.
3. Add restore verification and a random-date replay drill.
4. Archive existing verified DBN without deleting local evidence.

### P2 — compact research storage

1. Publish Silver observed and inferred Parquet separately.
2. Add manifests containing lineage, schema, counts, quality, and hashes.
3. Prove SQLite-versus-Parquet dual-read parity on the pilot.
4. Switch offline research reads to partitioned Parquet only after parity passes.
5. Keep finalized SQLite catalogs until restore and readiness compatibility are
   proven.

### P3 — improve throughput without weakening trust

1. Add bounded download concurrency and one import writer per physical volume.
2. Add bounded retry/backoff with provider revision rechecks.
3. Reduce redundant decodes through content-addressed validation receipts while
   preserving independent trust-boundary verification.
4. Process month-sized checkpoints and continuously recompute actual ETA/cost.

### P4 — reconcile legacy history

1. Build a SHA-256 and schema/content inventory of Polygon/Massive artifacts.
2. Identify exact duplicates and filename/content mismatches.
3. Preserve one canonical copy plus a verified backup before any deletion.
4. Convert trustworthy aggregate bars/features into a separately versioned Parquet
   family with explicit missingness/availability.
5. Keep legacy-derived data out of Databento readiness and model-promotion counts.

## 18. Acceptance criteria

The multi-year add-on is not complete until all of the following pass:

1. Every requested session is exactly one of eligible, quarantined, unavailable, or
   failed; no date is silently shifted or omitted.
2. The authoritative calendar covers the full interval and known early closes.
3. Every Bronze object rehashes to its manifest and fully decodes.
4. Every three-component historical bundle shares the required mapping/request/
   provider-revision identity.
5. An idempotent rerun performs no unnecessary provider download and produces the
   same content identities.
6. Raw-to-Silver row counts reconcile with explicit exclusion reasons.
7. Observed and inferred data remain physically and semantically distinct.
8. Every Silver/Gold row traces to raw hashes and transformation versions.
9. Feature/label availability and all anti-leakage assertions pass.
10. A complete five-family verified-close bundle is required before training
    eligibility.
11. Historical-only operational counters remain `not_applicable`, not zero.
12. Finalized SQLite passes integrity checks and has a safe checkpointed backup.
13. Parquet dual-read output matches finalized SQLite on the representative pilot.
14. Object-store restoration of a random date reproduces the manifest, normalized
    counts, and research-surface hash.
15. Local eviction is demonstrated without loss of reproducibility or application
    functionality.
16. Live capture remains healthy while archive uploads run, and historical import is
    automatically deferred during active capture.
17. Cost, storage, throughput, retention, and recovery reporting use measured
    evidence.
18. No model, formula, or prediction gains authority merely because download/import
    completed.
19. Unsupported or incomplete evidence appears as explicit abstention/unavailability,
    never as a plausible fallback.
20. A documented disaster-recovery drill can restore a selected date and resume an
    interrupted monthly batch without duplicate eligible evidence.

## 19. Related authoritative documents and code

- [Market-day engineering plan](MARKET_DAY_ENGINEERING_PLAN_2026-08-24.md)
- [Databento historical backfill](databento_historical_backfill.md)
- [Databento historical import](databento_historical_import.md)
- [Closing-tape historical bulk automation](closing_tape_historical_bulk.md)
- [TCBBO prediction pipeline](tcbbo_prediction_pipeline.md)
- `backend/closing_tape/live_recorder.py`
- `backend/closing_tape/historical_backfill.py`
- `backend/closing_tape/historical_bulk.py`
- `backend/closing_tape/historical_import.py`
- `backend/closing_tape/catalog.py`
- `backend/closing_tape/catalog_discovery.py`
- `backend/closing_tape/readiness.py`
- `backend/closing_tape/surface_artifact.py`
- `backend/databento_streamer.py`
- `backend/database.py`
- `backend/api/lifecycle.py`
- `backend/prediction_passport.py`

This document governs data handling and evidence quality. It does not authorize a
provider purchase, destructive cleanup, model promotion, or trading action.
