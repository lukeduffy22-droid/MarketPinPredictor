# Shadow contextual-model governance

Effective 2026-09-19, `shadow-pin-context-linear:0.1.0-preregistered` is a
frozen research benchmark and a rejected promotion candidate. Its equation,
coefficients, and live guardrails must not be edited. It remains journaled so
future candidates can be compared against the same historical benchmark.

`shadow-pin-context-no-zero-gamma:0.2.0-preregistered` is a separate,
shadow-only candidate. It preserves the 0.1.0 equation except that the
zero-gamma term is removed. It is not fitted, cannot replace a production
signal, and cannot promote itself.

## Evidence protocol

- Evaluation splits on complete America/Chicago trading sessions, never random
  rows.
- The first five sessions are temporal warm-up; every later session is an
  independent held-out fold with a 300-second embargo.
- Promotion review requires at least 60 held-out sessions plus an explicit
  approval. Meeting that count does not automatically promote a formula.
- Zero-gamma and pin-concentration ablations are evaluated offline. Contested
  pins remain live abstentions; their counterfactual metrics are diagnostic
  only and never relax the guardrail.

## Data-quality priorities

Quote pairing and receive-to-processing backlog are P1 engineering issues
because either one can suppress the last eligible five-minute forecast. Their
thresholds must not be lowered to improve coverage.

Historical missing provenance is retained unchanged. Current lifecycle
recording requires a nonblank committed `calculation_id`, a canonical process
epoch, and a positive subscription generation before a shadow record can be
written. There is no inferred or synthetic provenance backfill.

## RUT scope

RUT is accepted by the shadow engine under the same freshness, pair-count,
coverage, processing-lag, GEX, timing, and provenance thresholds as SPX and
NDX. This does not implicitly subscribe RUT. The existing optional-family
canary and launcher configuration remain authoritative; when RUT inputs are
missing or invalid, the formula abstains.
