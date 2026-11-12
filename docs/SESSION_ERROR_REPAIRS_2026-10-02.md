# October 1 session errors — overnight repair record

Prepared October 2, 2026, America/Chicago. This request covers the October 1 cash session and overnight follow-up. Existing dirty work and immutable market evidence were preserved. No model, formula, subscription, or acceptance threshold was changed by these repairs.

**Morning result: live acceptance failed.** The final 09:53 CDT check found stale, older-generation SPX/NDX calculations and unavailable passport-backed predictions. Exact opening buckets were missing, opening coverage remained incomplete, indicators reported an entitlement failure, and the recorder's primary-readiness gate prevented launch. The bounded heartbeat was deleted after its final check. See [Friday opening watch result](FRIDAY_OPEN_WATCH_RESULT_2026-10-02.md) for timestamped evidence and unresolved work. Earlier pending-verification statements below describe the overnight baseline.

## Confirmed defects repaired

| Failure | Evidence and repair | Verification |
| --- | --- | --- |
| Repeated Arrow conversion exceptions in gamma-wall tables | Missing put-side values were filled with `N/A` beside floats. The display now formats a separate text table before serialization; numeric source/export data and unknown-versus-zero distinctions remain intact. | Real PyArrow serialization of mixed missing, measured-zero and nonzero side values; source unchanged. |
| A brief SQLite lock could suppress the same forecast revision indefinitely | The passport writer already used short bounded attempts, but the lifecycle loop marked the revision as seen even after transient issuance failure. It now permits at most three lifecycle attempts, spaced two seconds apart, for SQLite busy/locked failures. Every retry re-reads the latest payload and repeats freshness, epoch, generation and publication checks. Permanent contract errors remain terminal for that observation. | Bounded retry, eventual recovery, contention and publication-guard regressions. No longer SQLite busy timeout was introduced. |
| Recorder aggregation could remain paused | Callback-side horizon freeze was enabled even for capture-only runs. Exceptions before a feed entered the frozen-feed list could also strand its pause. Capture-only feeds no longer arm it; the analysis `finally` releases every feed and prevents rearming. | Worker continues after the horizon; an injected readiness failure releases the pause. Exact pre-horizon prefix tests remain passing. |
| Recorder waited for evidence that could not enter a frozen snapshot | Once the frozen prefix was fully processed, readiness still waited up to 30 seconds for replay/watermarks. It now records the same incomplete-evidence reasons immediately, allowing analysis cleanup to resume aggregation without that pointless delay. | Frozen-unready regression fails if it sleeps; still rejects incomplete replay and does not admit later records. |
| Dashboard startup component scan failed | `watchfiles` was partially installed: the native extension reported 1.2.0, but package entry points and metadata were missing, with some Python files from a different version. Restored the same 1.2.0 from its local cached archive after verifying its RECORD hashes. Twelve files restored; changed originals backed up. | `watchfiles.watch` imports; installed metadata reports 1.2.0; Streamlit manifest scan succeeds. No network/package upgrade. |
| Startup claimed a rejected model was absent | SPX's existing checkpoint lacks its ordered feature contract. Startup now distinguishes a present-but-rejected artifact from no installed artifacts. | Rejected artifact remains disabled; diagnostics identify its validation failure. No feature order or weights invented. |

The recorder fixes address reproducible pause defects. They do **not** establish that all 778,394 October 1 callback drops had this cause or that future full-session throughput is sufficient. The original incomplete session and its raw DBN remain unchanged; no failed capture was relabeled complete.

## Earlier and coordinated repairs

- The false NDX ORB low and runtime-read isolation corrections remain in the tested source. See `NDX_ORB_UNDEFINED_PRICE_REPAIR_2026-10-01.md` and `RUNTIME_READ_ISOLATION_2026-10-01.md`.
- With Luke's explicit authorization, the `Verify MarketPin live session` chat owns official-close catch-up, indicator activation, backtesting, and one coordinated full-stack recovery. It reports successful October 1 official-close settlement and a new bounded 06:30 weekday catch-up task. Its receipts and scope are in `FRIDAY_OPEN_READINESS_2026-10-02.md` and `output/session_readiness_20261002/`.
- All three app endpoints became unreachable overnight after previously successful activation. This repair run did not stop them. The coordinated chat completed guarded startup at 01:16:59 CDT. The disappearance's cause remains unproven; the timing of a separate Git hang does not establish causation.

## Verification and retained evidence

Canonical `.venv`, offline fixtures, absolute isolated test database:

- Focused regression run: **127 passed**.
- Broader recorder, passport, publication, dashboard, API and ORB run: **366 passed, 1 skipped**. The skipped finalizer test requires a local OPRA probe that was unavailable.
- Final focused and inference-status run: **14 passed**. These runs overlap; their totals must not be added as distinct tests.
- `git diff --check` passed; staged diff SHA-256 remained `02932c0c5d65cdb70191751a066ddbf51f5734d1645a1d5e879a738e19bf0f83`. No staging or commit was performed.
- The pre-repair read-only database inspection confirmed WAL mode and `quick_check=ok` in `baseline-health.json`; off-hours inactive transport is not itself proof of a feed fault.

Evidence root: `output/session_repair_20261002/`. It contains dated/deduplicated log classifications, preserved source copies and hashes, test XML/logs, `environment-repair.json`, original environment files, and `release-manifest.json`. The environment receipt records before/after hashes and missing files for rollback.

## Activation and remaining limits

**Observed activation:** the coordinated canonical startup completed at 01:16:59 CDT. Backend, dashboard and research listener identities were recorded as 68596, 31796 and 56568. The 01:19:01 CDT readback returned HTTP 200 for backend, live status, dashboard and research. The backend remained in off-hours admission with `prediction_pipeline_ok=false`; the research service remained research-only. Tested disk hashes and the available backend startup fingerprints matched. The startup fingerprint records file contents at router import; it does not prove every module was executed or every dashboard element rendered.

The initial Windows PowerShell helper failed before launching services because `Get-FileHash` was unavailable. The unchanged guarded helper succeeded under installed PowerShell 7. An initial three-second authenticated passport probe timed out; the single post-warmup recheck passed HTTP 200. Both initial failures remain recorded.

Receipts: `output/session_readiness_20261002/20261002T061457610Z.recovery.json`, `post-recovery-readback.json`, and `logs/runtime/startup-profile-897fe1f1d46949f9913b1aa66cd9414e.json`. This report's source checks are retained in `output/session_repair_20261002/activation-verification.json`.

The recorder will load its repair on its next normal scheduled launch; it was not started off-hours. The overnight contract scaffold is explicitly labeled October 1 fallback. Existing 07:00/07:15 pre-stage tasks and the first allowed preopen connection are configured to prefer validated Friday contracts; that transition and Friday's 08:30 CDT opening capture remain unverified. No additional manual deployment of this repair set is pending, but market-hour acceptance is still required.

Unresolved conditions that must stay visible:

1. A direct live EQUS.MINI connection was denied entitlement. Historical retrieval success is not live entitlement. The enabled historical-poll indicator collector has not yet demonstrated fresh Friday bars; neither current-session availability nor freshness is inferred from its off-hours running state.
2. October 1 missed its exact opening bucket and had incomplete tape capture. Later data cannot repair point-in-time knowledge or certify a complete live session.
3. Thirty-two older close-ledger rows have missing original artifacts. Newer equal-price evidence does not recreate the missing bytes or correction history. Preserve those failures pending exact-byte recovery or a reviewed append-only adjudication path.
4. SPX/NDX pair coverage, VIX chain breadth, and RUT paired quotes failed at the end of the session. These checks remain enforced. They need fresh session evidence, not lower thresholds.
5. The trained SPX artifact lacks its feature contract; no trained-model accuracy or promotion is established. The separate chronological backtest did not justify promoting the experimental formula.
6. Friday transport, recorder throughput, persistence, clock admission and exact first-bucket capture require market-hour observations. Offline tests and an off-hours startup cannot prove them.

## Follow-up readiness and opening watch

At 01:50 CDT, a fresh read-only inspection classified the app as warming/off-hours with no issues or warnings; SQLite remained in WAL mode with `quick_check=ok`. The full database inspection took several minutes and completed normally. At 01:52:46 CDT, bounded backend, live-status, dashboard and research reads returned HTTP 200 in 15–32 ms. Tested disk hashes and available startup hashes still matched. These observations do not certify Friday live forecasts.

Windows task readback confirmed enabled weekday preparation/start checks at 07:00, 07:15, 07:45 and 08:15 CDT, plus the watchdog every five minutes from 07:50. The scheduled Windows PowerShell engine resolved `Get-FileHash` and `Get-ScheduledTask` successfully. The recorder's existing source gate is 09:35 CDT (10:35 ET), subject to primary-capture readiness; it is not expected at the cash open.

The Codex heartbeat `verify-friday-marketpin-opening-and-recorder` belonged to this chat, with scheduled checks at :20, :35 and :50 during hours 07, 08 and 09 CDT on October 2. It checked preparation, current-day source provenance, opening evidence and the post-09:35 recorder gate. It was deleted after the final checkpoint, with live-readiness blockers documented in the linked result. The coordinating chat confirmed it had no duplicate watch or pending runtime edits. No runtime service was manually restarted or stopped by this watch; the Windows supervisor independently replaced the backend before the cash open.

Receipts: `next-open-schedules.json`, `next-open-baseline.json` and `next-open-endpoints.json` under `output/session_repair_20261002/`.
