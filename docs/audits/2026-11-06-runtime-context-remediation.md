# November 6 runtime-context and ORB read remediation

Status: implemented and tested offline in codex/audit-remediation-20261027; not activated.
Starting HEAD: d318c2baba7e42a4a42a31c9ca40c89d48a43c5c.
Canonical runtime: C:/Cprojectsgpu_app/MarketPinPredictor.
Edit location: C:/Users/LukeD/.codex/worktrees/audit-remediation-20261027/MarketPinPredictor.

## Verified defect and correction

The second health read brackets the query-only retained-history read. Its TimeoutError,
OSError, or sample-validation ValueError previously reached the retained-database error
handler, discarded the readable history projection including verified official closes,
and mislabeled it RETAINED_EVIDENCE_UNAVAILABLE. This affects all symbol projections
in a failed bracket. The production incidence/denominator is not established.

The correction isolates the second context-read failure. It retains the loaded history,
adds RUNTIME_CONTEXT_UNAVAILABLE and RUNTIME_CONTEXT_RECHECK_UNAVAILABLE reasons,
marks intraday evidence runtime_unverified, and abstains. A successful first sample
cannot replace a missing second sample. The source database is never written.

Malformed health objects and sample timestamps now raise explicit ValueError reasons.
The existing two-second HTTP budget, fifteen-second sample age cap, symbol validity,
epoch/generation, handoff, quote freshness, and publication gates remain unchanged.

## Offline evidence

Eleven new regression cases cover failed first/second reads, retained daily and intraday
counts, no numeric or journaled forecasts on failure, source byte-hash preservation,
fresh-bracket recovery, malformed payloads, stale/future samples, and unchanged timeout.

The three second-read regressions fail against the previous HEAD implementation
(expected 1 verified daily row, actual 0). They pass against the correction.
Focused integration and publication-gate checks also pass. Final source-bound validation
results and receipt are in the separate evidence folder; no passing test establishes live
deployment, capture completeness, predictive skill, or resolution of transport starvation.

## November 6 runtime observations

The research service reported runtime health TimeoutError around 14:48 CT.
The backend log separately documents a real feed disruption:
- logs/runtime/backend.stderr.log:474, 14:49:24 CT: provider disconnect after 111 seconds without data.
- :483, 14:49:27: watchdog forced reconnect due stalled quote progress.
- :504, 14:49:33: skipped_records_after_slow_reading.
- :511, 14:49:38: reused bounded 2,114-symbol universe.
- :527, 14:50:40: resumed sustained progression, 24,674 accepted messages.

Runtime identity subsequently verified again under the same epoch and generation 4
(previously 3). This recovery occurred without this patch being activated.
The precise transient HTTP deadline/lock-owner cause is unproven; no stack was retained.
This patch corrects error handling, not the provider disconnection or health latency.

At 14:58:50 CT both symbols had fresh but invalid current-generation calculations:
SPX had 21 primary-expiration strikes (minimum 30) and pair coverage 0.0940 (minimum 0.1);
NDX had 17 strikes (minimum 30). These are separate, appropriate abstentions.

The 14:59:53 CT publication check still abstained for both symbols.
At 15:00:18 CT runtime_context_stable was true and generation 4, but SPX pair coverage
was 0.042735 and NDX 0.050420 with 26 strikes. The session was post_close_research and
prediction_session_allowed was false. New same-day pre-close forecasts cannot be issued
after the target close or by substituting a last reference/pin for an official outcome.

## Preservation and remaining gaps

Original audit reports/snapshots, collected rows, raw captures, logs, configuration,
canonical dirty state, and running services were preserved. Only isolated source/tests
and this document were edited; analysis evidence is outside the application paths.
No restart, activation, provider request, threshold reduction, stale-context cache,
historical backfill, merge, commit, or push is part of this remediation.

Remaining: independently identify the transient health read bottleneck if it recurs;
address valid near-close quote-pair/chain coverage using separately tested capture changes;
obtain official November 6 closes with provenance before scoring. A provider reconnect
or a passing offline test does not establish successful runtime deployment.

Evidence folder:
C:/Users/LukeD/.codex/visualizations/2026/10/27/01a17f25-1b0c-7f63-b077-449c2bb3ff81/runtime-context-remediation-20261106/

## Screenshot and forecast reconciliation

The user supplied a dashboard screenshot with page snapshot 14:59:14 CST and an
Opening Range & Pin Drift error: HTTP 503; RUNTIME_READ_TIMEOUT. The screenshot
establishes a failed bounded ORB read. It does not identify a lock owner or prove
a process exit. The supplied screenshot was copied byte-for-byte to the separate
evidence folder, with its SHA256 retained.

The app's later 7,375 value is corroborated by retained, valid primary predictions:
- prediction_snapshots id 27313 at 14:55:49.568 CT: predicted_close 7375.4453,
  current_price 7374.3900, gamma_pin 7375, model databento_quant_ensemble_v1.
- id 27314 at 14:57:04.485 CT: predicted_close 7375.5934.
- id 27315 at 14:58:18.452 CT: predicted_close 7379.1747.

The assistant's quoted 7394.19 came from the separate eod_equal_candidate_blend_v1
research forecast at 14:46:36 CT. Later research records also moved toward 7375,
using last_price_v1 after the generation change left too little contiguous history.
These are different model definitions and issuance times. The assistant failed to
deliver a current, reconciled estimate in the useful pre-close window; the old
research value must not be treated as the app's later primary output.

The final 14:59:58 primary lifecycle record abstained. No November 6 official-close
observations for SPX or NDX were retained at the 15:02:37 check, so an official-close
error is not computed from a pin, a last reference, or the user-reported display.
Executed mode=ro/query_only reconciliation, schemas, nine verified frozen research
rows and receipts, the three targeted primary rows and passports, and UI routing
source excerpts are saved under value-reconciliation/ in the evidence folder.

## Avoiding repeated ORB input reads during a generation change

The live ORB route can build the same fixed-as-of projection three times when
runtime identity changes twice. Repeating full-day reference/structure reads and
normalization can exhaust the existing four-second collection budget.

The isolated correction reuses normalized retained inputs only within one request,
keyed by journal instance, symbol, trading date, and exact as-of time. Every retry
still recomputes runtime binding, current-generation alignment, and freshness.
Each new request reads persistence again. No global/result cache, timeout increase,
index installation, database migration, or validity-threshold change is introduced.

This is a reproducible reconnect-path amplification defect. It is not established
as the exact cause of the screenshot's 14:59 failure, when generation 4 had already
been observed. Final combined offline tests and source-bound receipts must be read
with that limitation; actual live latency and incident closure remain unverified.
