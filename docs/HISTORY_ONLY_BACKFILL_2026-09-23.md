# Verified history recovery and research forecasts — September 23, 2026

The app's daily history was advanced from six sessions ending September 2 to **70 consecutive sessions through September 22** for SPX, NDX, RUT, VIX and SPY. Existing forecast formulas, eligibility thresholds and subscriptions were unchanged.

## Implemented safeguard

`tools/ingest_verified_closes.py` now accepts `--history-only` (and `ingest(..., history_only=True)`). This mode:

- Rejects prediction-selection or scoring-evidence arguments before writing.
- Requires an existing database with the current close tables and columns.
- Skips general database initialization, which can reconcile unrelated interrupted capture decisions.
- Preserves the existing canonical-database binding, source-byte, semantic, complete-bundle and append-only lineage checks.
- Imports verified close evidence without running legacy or governed prediction scoring.
- Returns explicit `history_only` reasons for both scoring paths.

The default post-close workflow keeps its existing behavior. History-only is a deliberate import mode, not an automatic retry scheduler or a schema migration.

Validation: **44 tests passed**, covering the importer, new history-only behavior and existing post-close scoring. Real-data scratch validation imported the entire batch twice and verified idempotency, unchanged original observations and unchanged forecast scores.

## Observed import result

Receipt directory: `output/history_only_repair_20260923T173527Z`.

The public official sources were fetched at approximately 12:40 CT and validated against exact symbol, session, close value and retained SHA-256. The exchange calendar identified 70 consecutive sessions through September 22. Their real retrieval times were retained; no historical availability was backdated.

An online SQLite backup preserved the pre-import database at `market-data-before.db`; its SHA-256 is `d1e60544f42e98518699a76b3a515f79c0e752541865f3513f2a79af301f807a`. Import was first rehearsed on a full copy of that backup. The scheduled post-close selection and settlement jobs were observed idle; their next scheduled runs were 15:05 and 17:05 CT.

The live import completed at **12:47:37 CT**:

- 350 new immutable close observations: 320 missing symbol/session pairs and 30 equal-price provenance revisions with explicit parent IDs.
- All 32 pre-existing close observations remained byte-for-byte equivalent at the row level. No prior close value changed.
- Both accuracy ledgers and existing prediction score fields were unchanged by the import.
- All 350 source references were staged, reconciled and resolved against retained artifact bytes.
- Each symbol now has 69, 68, 65 and 60 qualified return windows at horizons 1, 2, 5 and 10 respectively.

These are overlapping return windows, not 60 independent held-out model-validation sessions. History sufficiency does not establish forecast skill or current-input eligibility.

## Forecast result and remaining limits

At **12:49:17 CT**, one bounded query-only research capture using the repaired reader produced SPX and NDX candidates for today's close, Thursday September 24 and Friday September 25. Exact estimates, candidate equations, source times, runtime checks and retained inputs are preserved under `forecasts/20260923T174917Z/`. These are inferred, unpromoted research estimates with no calibrated prediction interval; their reference prices are derived from OPRA option parity.

RUT still abstains on invalid current inputs. VIX and SPY lack a verified current-session cash reference in this research path. Their sufficient daily history does not override those failures.

The port-8001 worker observed the new 70-session history, but its 12:49 and 12:50 captures abstained because runtime-context reads failed. `RETAINED_EVIDENCE_UNAVAILABLE` can also result when the second context poll raises an HTTP error inside the reader's broad exception handler; that code alone is not proof of database corruption. Runtime-read contention and clearer failure classification remain unresolved.

The SPX same-timestamp conflict repair is tested on disk but remains unactivated: Windows canceled both administrator launches. The running research worker still reports code fingerprint `ea3c74a00d816cc076764306560d2983b23a5fc5d85c62de44d331057d2f8705`; the repaired reader fingerprint is `7e4951bdfdc5a06cddd30656c756d8d66b314d8f83a1082e058ead75a6879cd8`. The one-shot forecast did not start a server or another provider connection.

**NOT YET VERIFIED:** continuous automated forecast availability, activated SPX conflict protection, full five-symbol current-input eligibility, complete ORB/closing-tape capture, and predictive performance. The historical backfill and one successful capture do not establish these properties.

The saved forecast report is a fixed snapshot, not a live panel. Future captures must recheck freshness and generation, and may abstain. The earlier permanent-reliability plan remains the roadmap for opening capture, daily-close retries and longer-term validation.

## Final API and dashboard confirmation

**OBSERVED:** The unchanged port-8001 worker produced SPX and NDX `RESEARCH_ONLY` forecasts at 12:52:38 and again at 12:53:40 CT. All three requested horizons were captured from the same 12:53:40 state. The dashboard visibly rendered SPX's September 23 estimate and NDX's September 25 estimate from that exact state. See `output/history_only_repair_20260923T173527Z/LIVE_FORECASTS.md` and its three accompanying API payloads.

The older SPX conflict is no longer in the 2,000-row read window: the preserved 12:49 input set begins at 09:55:09 CT, later than the 09:32:51 conflict. This recovery does not demonstrate activation of the preventive reader fix. Earlier failed captures remain in the receipt set.
