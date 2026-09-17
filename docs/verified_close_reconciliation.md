# Verified-close reconciliation

Tape completion and label availability are separate gates. Historical import
returns `already_complete` once the original source and finalization audit match;
re-importing does not repair official-close provenance. Readiness and dataset
loading resolve the ledger's exact date/family/SHA-256 in the close vault.
Previously this required exactly one filename before rehashing. Identical aliases
under different suffixes failed that check; hash-only model lookups also rejected
identical multi-day history artifacts retained for different sessions.

`tools/reconcile_verified_close_artifacts.py` appends a deterministic receipt:

```powershell
.\.venv\Scripts\python.exe tools/reconcile_verified_close_artifacts.py `
  --trading-date 2026-08-25 --symbol SPX --source-artifact-sha256 <ledger-sha256>
```

Use `--vault-root` for an alternate vault. The command requires existing canonical
files in `<vault>/<date>/<symbol>/<hash>.*`. Every matching file must hash to the
requested identity. Missing, empty, oversized, linked or conflicting artifacts
fail without creating a receipt. No bytes are fetched, inferred or substituted.

Receipts live in `_registry/<date>/<symbol>/<hash>/<receipt-hash>.json`. Their
canonical JSON binds the sorted inventory and deterministic first path. Creation
is exclusive and retries are idempotent; existing receipts, tapes, and the close
ledger are never rewritten. New aliases require another receipt. Interrupted or
altered receipts fail closed; reconciliation does not overwrite them.

Readers rehash every alias and require the receipt for the current inventory.
Hash-only lookups with copies across sessions require receipts for every
date/family involved. Receipts prove byte identity, not a new official close or
session qualification. Existing ledger references, label correction rules, tape
eligibility, family completeness, and model gates still apply. Readiness upgrades
dynamically only when those independent gates pass. Re-run readiness and dataset
loading after reconciliation; no tape re-import is needed.

On September 17, 2026, the local `data/market_data.db` contained 32 verified ledger
rows dated August 25 through September 2, but `data/verified_close_sources` was
absent. This is an observed missing-artifact blocker, not evidence that duplicate
files caused those local failures. The registry cannot recover missing bytes;
restore the exact retained official artifacts through the existing validated
artifact ingestion workflow before reconciliation. No local session was upgraded
as part of implementation.
