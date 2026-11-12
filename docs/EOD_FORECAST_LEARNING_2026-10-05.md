# EOD forecast comparison and learning release — October 5, 2026

The upgraded research view is available at http://127.0.0.1:8502/ with a separate research service on port 8002. Select **Load forecast research** to refresh. The original dashboard on port 8501 and live capture backend on port 8000 were not restarted. The original research service on port 8001 retains its previously loaded code.

## Implemented behavior

- Compare the frozen last-price baseline, momentum, mean reversion, gamma context and fixed blend against verified official closes.
- Match forecasts within a 15-minute time-to-close bucket. Use one forecast per session, with the same forecast origin and close outcome for every compared method. Candidate-composition changes cannot borrow another blend's scores.
- Show candidate disagreement separately from uncertainty. A spread between models is not a probability interval.
- Add an independent adaptive research challenger. Inverse prior absolute-return errors determine half its weight; the other half remains on the last-price baseline. The baseline is the supplied observation, including an explicitly labeled OPRA parity reference where applicable.
- Require 20 prior training sessions and 20 later chronological test sessions. Training labels must have been available before each test's forecast time. Require at least 5% lower test MAE and a positive conservative paired-improvement screen. These are new research eligibility rules, not changes to existing production gates or proof of statistical significance.
- After at least 30 eligible replay test sessions, expose a historical 90% absolute-error band for an eligible challenger. Future coverage remains unverified; the interval is not a guarantee.
- Store forecast bodies and separate, hash-bound post-commit receipts in the research journal. Original journals, official close records, raw tapes, accuracy records, and selected production forecasts are not rewritten.

The adaptive output is separate from the existing selected estimate and cannot grant production authority. Fixed candidates and comparisons remain research only. No thresholds or weights in the primary live prediction model were changed.

## Evidence limits

The current journal contains thousands of intraday forecasts but only a few comparable sessions with available official closes. The 45–60 minute comparison at 14:07 Central had 1 SPX and 3 NDX sessions. Counts change with the time-to-close bucket and available candidate family. These samples do not establish superior accuracy.

Older research receipts contain a declared analysis time, without the new confirmation that their database transaction had finished before the close. They remain available for retrospective comparisons but are excluded from adaptive fitting. They are not silently reissued or assigned new historical timestamps. Recovering verified official closes can improve comparison coverage; it cannot manufacture proof that a forecast was recorded before its outcome.

The previously run five-minute shadow backtest is a different horizon and uses retained OPRA parity outcomes. Its candidate MAE exceeded the last-price baseline: SPX 4.4076 vs 2.9098 over 234 samples; NDX 21.9713 vs 16.3997 over 302 samples. Those results do not establish EOD accuracy and were not used to tune today's primary model.

Additional observed dependencies remain: October 2 official close evidence was missing from the current daily-history projection; the existing ETF bar collector reported ENTITLEMENT_UNAVAILABLE. This release does not fabricate missing closes/bars or expand subscriptions.

## Runtime verification

At 14:17:37 Central:

- **OBSERVED:** preview source fingerprint matched the on-disk backend; four collection cycles completed; eight journal rows had eight valid post-commit receipts.
- **OBSERVED:** original research instance remained `f6aeb66b255d4272b0bd644d70550f30`; production feed remained generation 1, advanced to 7,661,373 messages and reported prediction_pipeline_ok=true.
- **OBSERVED:** browser displayed SPX/NDX research estimates and the new comparison table. The adaptive challenger was withheld.
- **OBSERVED:** 106 focused tests passed, including causality, close conflicts, receipt integrity, pre-close timing, independent-session counting, worker failure, stale display, horizon isolation, and existing research/indicator integration.
- **NOT YET VERIFIED:** improved live forecast accuracy, calibrated future interval coverage, and deployment of this code into the original elevated processes.

The current terminal is not elevated; Windows hid the original processes' executable paths and command lines. Their ownership could not be verified safely for restart. The separate preview was launched under the current user instead. It opens production and historical journals query-only, uses its own journal, and has provider/indicator collection disabled. Its quotes come from the existing local feed. No extra provider subscriptions were started.

CUDA is unavailable in the canonical environment (`torch 2.9.1+cpu`). Reading 4,847 frozen forecasts took approximately 1.1 seconds; per-symbol evaluation took approximately 0.16 seconds. This workload does not justify changing the GPU environment during live capture. No CUDA acceleration or accuracy improvement is claimed.

## Files and retained receipts

- Implementation: `backend/eod_forecast_learning.py`
- Collector integration: `backend/market_research_service.py`
- Frontend: `app/services/forecast_research_view.py`
- Reproducible review: `tools/evaluate_eod_candidates.py --port 8002 --output <new receipt path>`
- Preview launcher: `tools/start_forecast_comparison_preview.ps1` (refuses occupied or production ports)
- Before/after comparison receipts: `output/forecast_learning_20261005/comparison-before-activation.json` and `comparison-after-activation.json`
- Runtime evidence: `output/forecast_learning_20261005/20261005T191737504985Z-runtime-verification.json`
- Running preview journal and process receipts: `output/forecast_learning_20261005/preview/`

The preview is a manually launched research companion, not a newly installed scheduled service. Source changes will enter the canonical research service on a later verified restart. Keep the original production recorder running during market hours. Any promotion of an adaptive model requires a separate review of its retained out-of-sample evidence.
