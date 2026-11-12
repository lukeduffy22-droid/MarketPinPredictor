# October 30 calculation-lineage correction

Implemented offline in the existing `codex/audit-remediation-20261027`
worktree, based on `cb017379aabb2488c785b131d231db080b23ab4e`.
This change is not activated. It does not modify the canonical checkout,
restart services, contact providers, rewrite historical records, reconstruct
missing calculation identities, or change model/promotion authority.

## Cause and resulting behavior

Between retained-input captures, the streamer computed a fresh result without
capturing its full inputs. That result could replace the last acknowledged
publication while retaining a usable flag. Periodic forecasts and research
passports could then be written without calculation identity. A regression
previously explicitly accepted that behavior.

- A noncapture numerical refresh is now a `CALCULATION_CAPTURE_THROTTLED`
  diagnostic. It cannot replace or restamp the acknowledged publication or
  trigger a forecast callback. A due capture must commit its run and full
  inputs before publishing its identity and input SHA-256.
- Lifecycle admission resolves the exact retained run/blob before loading
  context or invoking the model. An immutable input copy and revision guard
  prevent changes during inference, persistence and final publication.
- New Databento prediction persistence independently checks symbol/session,
  provider, epoch/generation, issuance/calculation/quote timestamps, reference
  and material GEX values, schema, bounded decompression, hash and embedded
  replay-input identity. A producer's success flag or nonempty id is not proof.
  Conflicting top-level overrides are rejected.
- A same-quote retry cannot return an old model/feature/point revision as a new
  successful write. Existing unbound rows remain untouched and are not returned
  as accepted retries. Failed persistence produces an explicitly unissued
  attempt; commit acknowledgment is distinguished from a later publication
  or refresh failure.
- Newly issued numeric research passports require verified lineage, just as
  other admitted forecasts do. Research classification still permits absent
  calibration; it does not permit absent retained inputs. Historical passport
  readers and immutable records are unchanged.

## Other software corrections

Option-chain median bid/ask are retained as `option_chain_median_bid` and
`option_chain_median_ask`, with option-premium units. Index `market_bid` and
`market_ask` remain unavailable without an independently sourced underlying
quote. A market snapshot can no longer combine an index reference price with
an option-chain premium spread.

Future input captures also retain the exact solver clock, fractional expiry
year values, floor and cutoff parameters. Integer calendar days to expiration
are not substituted for solver inputs. Historical captures are not backfilled.

`tools/audit_calculation_lineage.py` provides a separate, bounded read-only
coverage assessment. It imports no application modules, opens SQLite using
`mode=ro`, `query_only` and a transaction, and reports exact lineage together
with valid/invalid/no-run minute buckets and retained attempt receipts. It
never repairs a missing id by joining to a nearby timestamp. Resource limits
or incomplete schema/evidence return an explicit blocked assessment rather
than a partial successful cohort. Outputs require a new audit directory and
include a hash-bound coverage manifest; this is not a training-eligibility or
economic feed-completeness certificate.

## Deliberate boundaries

The default 60-second input capture cadence and 30-second freshness limit are
unchanged. The retained publication keeps its original time and must become
stale when that limit is exceeded; numerical availability may therefore
abstain between acknowledged captures. No timestamp is advanced to conceal
that interval and no extra full-input capture frequency is silently added.
Any cadence/storage change needs its own performance and capture assessment.

The existing remediation already includes late-shadow-label quarantine,
prospective fixed-lead EOD selection, clock/opening diagnostics, source-bound
validation, UI evidence stages, and bounded indexed journal reads. These
remain isolated implementation changes until a later authorized activation.

Software cannot create a missed live opening bucket, an independent cash
validation source, unique economic trade identity, missing historical labels,
calibrated held-out sessions or a production promotion approval. Such evidence
continues to require abstention or an explicitly qualified research-only use.
No source prices, outcome tolerances, forecast coefficients, freshness limits,
or optional-family gates were tuned to improve the October results.

## Offline verification and handoff

Tests use the canonical `.venv` interpreter without installing/syncing packages,
with provider credentials cleared and an absolute isolated SQLite target.
`tools/source_validation.py` binds the result and logs to the precise source
inventory. Development runs with `source_stable=false` are diagnostic only;
the final combined run must be successful and source-stable.

Coverage includes missing/spoofed/cross-generation identities, corrupt and
hash-valid wrong-content blobs, future timestamps, mutable source dictionaries,
duplicate retries, commit/export failure stages, direct passport issuance,
producer throttling/freshness, independent quote units, solver timing, API
read purity, and the earlier remediation suites. Retained real October 27
captures are also checked in a new isolated database without modifying their
saved files.

Validation artifacts are saved outside application data under the chat's
`remediation-20261030/` directory and in unique ignored development output
directories. Source receipts are evidence of offline execution, not signatures,
activation approval, current live-readiness proof or forecast accuracy.
