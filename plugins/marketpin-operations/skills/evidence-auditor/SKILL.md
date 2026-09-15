---
name: evidence-auditor
description: Audit a MarketPinPredictor gamma level, prediction, closing-tape result, or dashboard metric for provenance, freshness, aligned timestamps, replayability, and abstention behavior. Use when a value looks wrong or an analysis must be trustworthy.
---

# MarketPin Evidence Auditor

Treat trustworthiness as the deliverable. Use the packaged workspace and Python execution policies. Inspect current source and evidence read-only unless the user explicitly asks for a fix.

## Align the claim

Identify the symbol, market session, provider, source timestamp, generated timestamp, timezone, units, expiration scope, model/formula version, and requested claim. Do not compare records until symbol and time bucket align.

Separate:

- observed quotes or trades;
- published open interest;
- inferred flow or participant positioning;
- calculated gamma, zero-gamma, pin, or max-pain levels;
- model forecasts and confidence;
- official closes and other later-known outcomes.

## Trace evidence

Follow the value across its available chain: dashboard -> API/schema -> calculation inputs -> audit artifact -> persistence row -> replay or verifier. Check freshness, coverage, subscription generation, invalid reasons, null/zero semantics, and whether the stored input blob can reproduce the output.

For closing-tape research, preserve raw market capture as immutable evidence. Verify required cutoff artifacts and distinguish a recorded session from a completed analysis. Do not infer intent from trade prints alone.

## Decide trust

Return one verdict:

- `trustworthy`: aligned, fresh, valid, internally consistent, and replayable enough for the stated use;
- `usable_with_caveats`: bounded gaps do not invalidate the limited claim;
- `abstain`: stale, thin, invalid, mismatched, unreplayable, or materially incomplete;
- `contradicted`: aligned evidence disproves the displayed or stated value.

Prefer an honest abstention over fallback data. Do not convert missing data to zero. Do not promote shadow formulas or models to production based on in-sample or same-session evidence.

## Completion

Lead with the verdict, then provide aligned evidence, provenance gaps, reproducibility result, the narrow claim that remains supportable, and the next check that would change the verdict. Clearly distinguish fact, calculation, inference, and scenario.
