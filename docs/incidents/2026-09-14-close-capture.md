# Late-session capture and diagnostic assistant

Status: OPEN. Recording continues through the regular cash-session close; there is no 14:45 CT cutoff in the Databento compute path.

Observed on 2026-09-14 after close: SPX has 360 valid exported records, last 19:45:16.866549 UTC (14:45:16 CT). Its last failed audit is 19:59:48.491458 UTC (14:59:48 CT), reporting PSEUDO_PARITY_TOO_FEW_PAIRED_QUOTES: 3 pairs < 5. Earlier failures include PRIMARY_PAIR_COVERAGE_LOW. NDX has 372 valid rows, last 19:55:51.967564 UTC. Backend logs record a full queue at 14:56:34 CT, a no-data timeout at 14:56:55, and reconnect at 14:58:00. Those later events do not alone establish why SPX first degraded.

Implemented source corrections:

- Export screen distinguishes last valid research snapshot from last retained recording attempt, with downloadable failed attempt evidence. Latest gamma pin is not called final price or official close.
- Compute suspension and inactive handoff leave throttled, price-free failure receipts when the regular-session compute loop is invoked. These do not turn skipped cycles into valid samples; a dead process cannot issue receipts.
- Advisor adds explicit read-only OpenAI diagnosis, bounded evidence preview, validated citations, and downloadable change requests. It has no execution tools and does not tune models.

Unresolved: quote-pair degradation and overload prevention. Automatic official-close acquisition is not implemented by this change. Existing verified-close evidence contracts remain authoritative; a final observation or pin never substitutes for official close.

Acceptance: verify deployed source identity; observe valid final-session inputs or explicit failed receipts throughout the final 15 minutes; verify no silent gaps beyond configured cadence; obtain a separately verified official close before outcome scoring. Test regular and early-close boundaries. Restarting after close cannot repair historical missing observations.

AI usage: Advisor -> Analyze Live State -> AI operational diagnosis -> preview evidence -> ask -> download bundle. Uses OPENAI_API_KEY and optional OPENAI_DIAGNOSTIC_MODEL (default gpt-4o-mini). API failure returns an error, never fabricated diagnosis. Suggestions remain unverified proposals.

Verification: 168 focused tests passed in 8.93s. Temporary Streamlit preview visibly showed last valid SPX at 14:45:16 CT and last attempt at 14:59:48 CT. Primary dashboard activation was refused by the existing process-identity guard; backend remains un-restarted. Inherited API credential returned 401; project .env credential returned 403 Permission Denied in synthetic-only tests. Deployment and live AI service access remain OPEN.
