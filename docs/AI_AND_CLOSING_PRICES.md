# AI and official closing prices

## AI use after deployment and API access repair

Advisor -> Analyze Live State -> AI operational diagnosis. Preview the evidence, type a question about capture/readiness/application changes, then Ask AI to review this evidence. Download the change-request bundle and attach it in Codex for implementation. The app has no automatic connection to the Codex conversation, no execution tools, and does not tune forecasting models.

The previous live checks returned 401 with the inherited OpenAI credential and 403 with the project credential. Successful live AI access remains unverified; do not paste credentials into chat.

## Save official closes

Daily Snapshot Exports -> Official closing prices — save and review -> choose session -> Fetch and save official closes. The action takes the union of the dashboard selection and the backend tracked-symbol inventory. If the backend inventory cannot be read, it refuses collection rather than silently omitting symbols.

Current backend inventory observed 2026-09-14 15:26 CT: SPX, NDX, VIX, RUT. Each has an existing official source adapter. SPY is also supported; other symbols are explicitly UNSUPPORTED_SOURCE until a validated adapter is added.

The existing official-source publication gate is 18:00 Eastern / 17:00 Central. This is a publication delay, not a market recording cutoff. Before that gate the report saves AWAITING_PUBLICATION with empty prices. Manual fetching remains available. The optional `MarketPin-VerifiedClose-Settlement` task runs at 17:05, 17:20, and 17:45 Central so a temporarily unavailable authority can be retried without weakening validation. Prior reports and source artifacts are retained.

Successful official artifacts are hashed and semantically checked for symbol, date, price and observation chronology. Reading saved closes rechecks the retained hash and semantics; changed/missing artifacts suppress the price and show ARTIFACT_INVALID. One source failure does not hide the other symbols. Missing values are never filled from AI, last trade, option parity or gamma pin.

Reports persist under C:/Cprojectsgpu_app/MarketPinPredictor/data/closing_prices/YYYY-MM-DD/. Source artifacts use the existing data/verified_close_sources vault. Reports remain distinct from the existing complete-bundle outcome ledger. No database ingestion, model scoring or training occurs through this UI action.

## Scheduled verified-close settlement

Install the two current-user tasks with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools\install_verified_close_postclose_tasks.ps1
```

`MarketPin-VerifiedClose-Selection` runs at 15:05 Central. It records the last lineage-valid SPX, NDX, and RUT prediction at or before the cash close in an immutable SHA-256-named receipt under `data/prediction_score_selections/YYYY-MM-DD/`. It must complete after cash close and before the 17:00 publication gate. The selection rule is fixed before official close evidence is fetched, so retries cannot select a different prediction after observing the outcome.

`MarketPin-VerifiedClose-Settlement` runs after the publication gate. It reuses the existing five-family SPX/NDX/RUT/VIX/SPY official-source fetcher and semantic audit, requires the canonical `data/market_data.db`, atomically ingests the complete bundle, and scores only prediction IDs in the immutable selection receipt. Diagnostic scores remain `training_eligible=false` and `performance_claim_eligible=false`. Result receipts are retained under `data/postclose_pipeline/YYYY-MM-DD/`.

DJI remains explicitly unsupported for verified-close scoring because the current repository has no approved official DJIA close adapter and the Databento path has no direct DJI cash-index feed. DIA is not substituted because its price is in different units. The dashboard shows this state instead of implying complete four-index coverage.

Remove the tasks without deleting retained evidence:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools\install_verified_close_postclose_tasks.ps1 -Uninstall
```

The AI evidence packet now includes today's saved closing-price report when available, including missing statuses. Codex can read these workspace files on a later request; there is no automatic message handoff.

## Verification and limits

55 tests passed in 2.62 seconds: new workflow, diagnostic integration, existing verified-close import and official bundle parsing. An isolated fresh Streamlit AppTest rendered the fetch action and loaded all four saved pending rows. Primary runtime deployment is not claimed: its process-identity guard previously refused the dashboard restart, and that blocker remains unresolved. No primary backend or dashboard was restarted in this turn.

Today at 15:26 CT, the workflow saved all four tracked symbols as AWAITING_PUBLICATION until 22:00 UTC. It has not yet fetched or saved today's numerical official closes. See tracked-closing-prices-2026-09-14.json and closing-prices-tests.txt.
