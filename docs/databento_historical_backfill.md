# Databento historical backfill

MarketPin keeps Databento Historical acquisition separate from the live closing-tape
recorder. A live DBN can contain multiple subscribed schemas, while Historical API
requests return one schema per file. A historical day is therefore represented as a
hash-bound bundle of three immutable components:

- `tcbbo`: every option trade with the consolidated pre-trade BBO;
- `statistics`: the provider-published daily statistics and open interest;
- `definition`: point-in-time contract definitions and symbol identity.

The bundle uses the same nine explicit parents as the recorder: `SPX.OPT`,
`SPXW.OPT`, `NDX.OPT`, `NDXP.OPT`, `RUT.OPT`, `RUTW.OPT`, `VIX.OPT`,
`VIXW.OPT`, and `SPY.OPT`. It never requests all OPRA symbols.

## Safety boundary

The command defaults to a metadata-only dry run. Metadata calls determine provider
condition, account-entitled per-schema availability, estimated USD cost, and
uncompressed billable bytes. No time-series request is made until `--execute` is
supplied together with explicit local estimate ceilings.

Those ceilings are checked by MarketPin before `get_range` starts. They are not
sent to Databento and are not provider-enforced billing limits. A configured
Databento historical monthly limit is the provider-side protection; accounts may
instead be set to `Unlimited`. See Databento's
[monthly-limit documentation](https://databento.com/docs/knowledge-base/portal/billing/manage-monthly-limit).

```powershell
& .\.venv\Scripts\python.exe .\tools\backfill_closing_tape_history.py `
  --date 2026-08-27
```

The known August 27 gap can be acquired only after reviewing that plan:

```powershell
& .\.venv\Scripts\python.exe .\tools\backfill_closing_tape_history.py `
  --date 2026-08-27 `
  --execute `
  --approve-plan-sha256 <PLAN_SHA256_FROM_DRY_RUN> `
  --max-estimated-cost-usd 0 `
  --max-estimated-billable-bytes 500000000
```

Downloads are written to `data/databento_history/<date>/` as DBN/Zstd. The final
manifest binds the dataset, exact `ts_recv` request windows, explicit input/output
symbology, provider condition revision, parent list, component byte sizes and hashes,
decoded compression/DBN metadata,
point-in-time mapping identity, record counts, timestamp bounds, family coverage,
and open-interest coverage. `bundle_sha256` binds the stable request, component, and
legacy-source identity. A separate `provenance_sha256` binds the provider-condition
revision, reviewed planning estimates/ceilings, SDK versions, and acquisition times.
`attestation_sha256` is the single root over both hashes and is the identity external
consumers should track.
All components must share the same mapping hash.

Low or missing per-family trades, open interest, or two-sided BBO coverage is
attested as an explicit quality gap rather than disguised as complete model evidence.
It blocks downstream readiness but does not erase an otherwise decodable provider
response.

Every day has a single-writer file lock. Verified components are reusable only for
the exact approved plan and provider revision after an interrupted run. Failed or
partial responses are retained as clearly marked diagnostic files but are never
discoverable as bundle evidence. A complete manifest is
idempotent: a subsequent run fully verifies and reuses it rather than issuing
another time-series stream.

The provider condition and its last-modified date are rechecked immediately around
each component request. A changed revision aborts the bundle and requires a new dry
run and approval. Existing native bundles must match the current provider revision;
legacy v1 reuse requires a v2 sidecar that records a distinct revision observation
made when the legacy evidence was attested.

The original August 27 pilot uses the earlier v1 manifest. It remains immutable.
The verifier decodes and validates all of its content, and an optional
`manifest.v2.json` attestation can add the richer evidence without overwriting the
original acquisition manifest.

The tool skips dates that already pass MarketPin's immutable live-capture readiness
audit unless `--include-eligible` is explicitly supplied.

## Availability boundary

The account-entitled `OPRA.PILLAR` range must be queried at runtime. At the September
1, 2026 audit, TCBBO and CMBP-1 began March 28, 2023. Trades, CBBO-1m, statistics,
definitions, and OHLCV began April 1, 2013. Databento therefore cannot provide 30
years of equivalent OPRA microstructure. Older and lower-resolution data must remain
a separately versioned feature family; it cannot be silently mixed with TCBBO.

The current acquisition command intentionally accepts only 2024 through 2026. That
is the range covered by MarketPin's checked-in holiday and early-close calendar.
Dates outside it fail closed until an authoritative exchange-calendar dependency and
mixed index/equity options close policy are added. This restriction prevents older
holidays and early closes from receiving plausible but incorrect request windows;
it does not change Databento's longer provider availability.

## Training boundary

A downloaded bundle is retained provider evidence, not automatically a training-ready
MarketPin session. The historical importer must remain separate from the live mixed-
DBN finalizer, rebuild point-in-time symbol mappings before open interest, enforce
open-interest availability times to prevent leakage, replay each component with its
own hash, bind a verified official close, and pass the existing chronological
readiness gates. Live acknowledgement/replay counters are not applicable to a
historical session and must never be represented as observed zeroes. CUDA remains
downstream of those checks.
