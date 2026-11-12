# NDX sampled opening-range repair — October 1, 2026

## Diagnosis

The displayed NDX low of **26,996.116717640016** came from retained CBBO parity samples at **09:00:05 and 09:00:10 CT**, not an observed cash-index low. A third malformed sample at 09:00:00 CT produced 29,270.78951514487. The surrounding retained estimates were 30,487.87 at 08:59:55 and 30,465.52 at 09:00:15.

The installed Databento decoder identifies `9223372036854775807` as `UNDEF_PRICE`, with a fixed-price scale of one billion. The live decoder previously accepted this finite missing-value marker and converted it to `9223372036.854776`. The malformed retained formula inputs contain that value as both call and put mid-prices. Subtracting equal missing values canceled their numerical magnitude; the discounted strike then looked like a positive parity estimate and passed the old checks.

The screenshot's general high/low columns represented the **60-minute** range. They were not the 5-minute or 15-minute ORB. The first sample was also 20 seconds late, so every opening window remains partial regardless of the repaired extrema.

## Repair

- Reject undefined and non-finite provider prices before normalization. Apply the same representable-price check to cached quotes admitted to the independent ORB sampler.
- Reject malformed option-price inputs at both reference persistence entry points. Preserve the existing parity formula, sample cadence, freshness checks, subscriptions, and capture-completeness rules.
- Annotate price-input validity when reading retained records. Evaluate the bounded projection's input check inside SQLite without returning full formula JSON to the live endpoint.
- Exclude invalid samples from range calculations while retaining their original immutable rows and decisions. Report excluded counts and sample IDs; a rejected newest sample cannot turn an earlier estimate into a current value.
- Label the table columns **First sampled price**, **60m sampled high**, and **60m sampled low**, and explain exclusions explicitly.

## Read-only replay result

Three NDX opening-hour samples were excluded. No SPX, VIX, or RUT opening-hour samples were excluded by this check.

| NDX window | Remaining sampled low | Remaining sampled high | Capture |
| --- | ---: | ---: | --- |
| 5 minutes | 30,466.52 | 30,574.90 | Partial |
| 15 minutes | 30,466.52 | 30,574.90 | Partial |
| 30 minutes | 30,436.68 | 30,574.90 | Partial |
| 60 minutes | 30,327.24 | 30,574.90 | Partial |

These are **retained option-parity research ranges**, not independently verified exchange OHLC. Removing proven malformed inputs does not recover the missed opening bucket or establish that every remaining estimate equals the cash index.

The full repaired projection was exercised against production evidence using SQLite `mode=ro` and `query_only=ON`: individual projections took 0.142–0.190 seconds. All ten traced original low-price records and their formula JSON remained byte-for-byte unchanged.

## Tests and evidence

Canonical `.venv`, isolated test-only database: the regression run passed **286 tests** in 55.39 seconds. The final focused run passed **7/7 price tests** in 9.13 seconds, including the additional newest-invalid-sample test. Six overlap with the regression run. `git diff --check` passed. Existing staged and unstaged work was preserved.

Evidence under `output/ndx_orb_20261001/`: `raw-reference-trace.json`, `repaired-readonly-preview.json`, `before-tests.xml`, `tests.xml`, `final-price-tests.xml`, source baselines, tested source hashes, and activation receipts.

## Activation

Activation completed at **14:56:48 CT** through the canonical guarded backend launcher, with verified ownership and the expected listener PID. The initial attempt made no process changes because another guarded workflow had already replaced dashboard PID 29916 with PID 41628. The retry preserved that dashboard and replaced backend PID 52440 with verified PID 27484. The canonical launcher and elevated wrapper both returned exit code 0.

The three changed backend files match their tested hashes in the startup fingerprint and on disk. The dashboard visibly renders the renamed columns, the corrected NDX low of **30,327.24**, and **3** excluded samples; `dashboard-corrected.png` retains the proof. The earlier runtime-read isolation repair remains present. No activation step remains for this repair.

Post-startup observations through **15:02:02 CT** show messages advancing from 12,574 to 55,368, active handoff, and zero current-process provider skipped-record warnings. The existing cbbo-1s / near-term-shadow / zero-replay configuration was preserved. The last health and symbol-dashboard reads took 13–73 ms; the corrected ORB endpoint returned HTTP 200 in 1.924 seconds. No `RUNTIME_READ_TIMEOUT` appeared in these observations.

Read-only database verification returned WAL mode and `quick_check=ok`. Calculation runs and input blobs advanced from 23,299 to 23,305; audit rows advanced from 41,494 to 41,502. Prediction, market-snapshot, and gamma-pin high-water marks did not advance during this interval. SPX and NDX continued to fail primary pair-coverage checks, VIX had too few qualifying strikes, and RUT had too few paired quotes. Forecast endpoints correctly returned fast abstentions rather than timeout errors. The overall operator inspection therefore remained **degraded**: successful ORB activation is not proof of forecast readiness.

The staged diff hash remained identical to its baseline; nothing was staged or committed. Final evidence: `activation.json`, `live-2.json`, `live-3.json`, `post-inspection.json`, and `activation-verification.json`. The missing first opening bucket cannot be repaired from later samples. Independently verified cash-index OHLC and the separate recorder/close-evidence issues remain outside this repair.
