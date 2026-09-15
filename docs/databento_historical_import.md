# Databento historical evidence import

Historical OPRA bundles remain separate from live recorder sessions. A verified
bundle must contain the canonical `definition`, `statistics`, and `tcbbo`
components, their compressed-file hashes, one shared point-in-time mapping
identity, the canonical nine option parents, and the v2 bundle/provenance/root
attestations.

The importer keeps the evidence boundary explicit:

- The compressed `.dbn.zst` files and `manifest.v2.json` remain the immutable
  observed source.
- Instrument-definition records and daily open-interest updates are appended to
  immutable observation ledgers.
- TCBBO trades and their pre-trade NBBO are deterministically aggregated into
  observed family/contract minute rows.
- Price-versus-pre-trade-NBBO classifications are written only to inferred
  tables and remain labeled estimates, not aggressor side or holdings.
- All derived rows bind to the three-component `bundle_sha256`; the finalization
  audit separately retains every compressed and temporary raw DBN hash.
- Live subscription, replay, reconnect, and callback counters are explicitly
  not applicable to historical sources. They are not treated as observed zeroes
  by status/readiness consumers.

The read-only dry run verifies every persistent component byte and shows whether
an active recorder blocks execution:

```powershell
.\.venv\Scripts\python.exe tools\import_closing_tape_history.py `
  data\databento_history\2026-08-27\manifest.v2.json
```

After the live recorder has terminally finalized, execute the import:

```powershell
.\.venv\Scripts\python.exe tools\import_closing_tape_history.py `
  data\databento_history\2026-08-27\manifest.v2.json --execute
```

There is intentionally no active-capture override. Execution streams each
compressed component into a temporary uncompressed DBN, verifies record framing
and manifest counts, persists the ledgers/features, records an immutable
`databento-historical-bundle-v1` finalization audit, and then removes the
temporary files. A failed import is marked incomplete and can be retried against
the same bundle hash, with the failed attempt retained in the finalization
ledger; a completed import is idempotent. The deterministic historical session
is added alongside any older incomplete live session for that date rather than
overwriting or relabeling the original capture.
