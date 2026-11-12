# Quote-pair and reader-clock repair for October 1

Prepared September 30 while the canonical application continued running.
This is an offline-tested release; installation and next-session activation
must be recorded separately. Evidence lives in the canonical project's
`output/clock_readiness_redesign_20260930T160616Z/followthrough` directory.

## Confirmed defects and changes

1. Current CBBO bid/ask intervals were rejected by opening capture when their
   optional last-sale timestamp came from an earlier calendar date. Retained
   September 30 calculation inputs prove this occurs in the actual feed.
   The explicit `cbbo-last-sale-metadata.v2` validation policy accepts that
   metadata without changing its raw value. It still rejects future/malformed
   trades, stale/future intervals, wrong generations/mappings, crossed quotes,
   and insufficient complete pairs. The existing CBBO interval layout remains
   version 1; the separately versioned validation policy is retained in each
   contributing leg and immutable formula inputs, included in sample identity,
   and projected into range identity. Unmarked historical evidence retains the
   original same-day rule. No migration or historical rewrite is required.
2. `_record_processing_clock_sample` acquired the prediction-publication lock
   on the live reader. A deterministic contention test reproduces the stall.
   A separate short memory-only lock now protects clock evidence. Generation
   transitions still honor the publication barrier and atomically clear old
   clock evidence. No clock path acquires publication while holding this lock.
   Both direct sampling and the actual synthetic SDK reader progress while
   publication is blocked. This proves removal of that stall path, not that
   it alone caused every second of today's opening delay.
3. Startup enables independent Windows clock diagnostics unless explicitly
   disabled. This measures host synchronization separately from provider lag.
   It remains diagnostic only: it cannot turn insufficient provider evidence
   into a synchronized capture. Both launchers now require the matching 72-file,
   55-module preflight contract.

The 50-provider-sample gate, 30-second production freshness limit, minimum five
pairs, generation/handoff checks, immutable first bucket, and final persistence
deadline remain unchanged. Subscription scope and model formulas are unchanged.

## Retained-input comparison

The same source-time, local-age, price, generation, and mapping-presence filters
were applied to copies of today's earliest retained calculation inputs. These
are observations at approximately 08:30:47–08:30:54 CT, not a replay of the exact
08:30:00–08:30:05 bucket. The mapping catalogue and live handoff were not replayed.

| Symbol | Old-rule primary pairs | New-rule primary pairs |
|---|---:|---:|
| SPX | 52 | 106 |
| NDX | 31 | 244 |
| RUT | 5 | 25 |
| VIX | 0 | 12 |

`tools/replay_cbbo_quote_policy.py` reproduces this comparison without opening
a provider connection or writing the market database. Input hashes and full
results are retained in `policy-replay.json`.

## Validation and limits

- 489 Python tests passed, including provider reader, reconnect generation,
  quote timing, immutable persistence, opening-window rehearsal, ORB reads,
  host-clock isolation and startup contracts.
- 29 Windows PowerShell launcher tests passed.
- Strict preflight passed against a new isolated SQLite database, with zero
  schema or source issues. This does not prove the future production launch.
- The broader run exposed eight stale publication-test fixtures on the original
  source too. Their synthetic inputs omitted the current-session universe
  prerequisite. Fixtures now supply it so the persistence tests exercise their
  intended boundary. No production universe gate was weakened.
- The first revised ORB projection affected legacy query behavior; it was
  corrected so policy extraction is limited to CBBO detail rows. Legacy bounded
  projection checks pass. Early failed receipts remain available.

Missing actual bid/ask legs and truly stale quotes remain failures. RUT/VIX
model eligibility is separate from ORB pair eligibility. A broader host-clock
bootstrap policy is not activated by this release: independent absolute clock
accuracy and authority semantics still need validation. Provider/network delay
and raw-tape disconnects can recur. No missed historical opening is backfilled.

## Deployment

Use the sealed manifest and deployment procedure in the evidence directory.
Installation is restricted to September 30 after 18:15 CT or October 1 before
06:45 CT, after raw tape has finalized. Verify every source before-hash and
package after-hash; preserve backups and fail closed on drift. Install only the
named release files. Do not switch branches, discard dirty work, migrate data,
change credentials, or start a provider connection during installation.

The existing SYSTEM startup/watchdog has an ownership-guarded prior-day backend
refresh before the open. Verify that it actually creates a new canonical backend
and loads the expected 72 file hashes on October 1. If it does not, report the
specific failure and use only existing guarded, authorized recovery. Installation
alone is not activation. Verify the first bucket and 5/15/30/60-minute ranges,
valid persistence progression, independent tape, afternoon data and finalization.

Provider semantics: https://databento.com/docs/schemas-and-data-formats
