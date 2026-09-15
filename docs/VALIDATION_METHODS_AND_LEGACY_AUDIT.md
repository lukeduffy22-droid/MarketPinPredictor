# Validation methods and targeted legacy-code audit — 2026-09-14

Status: source implemented and tested; primary runtime activation remains unverified.
Source reference: private canonical checkout on branch `copilot/vscode-mtwynpc2-ellx`. Existing dirty work was preserved during the audit.

## Preservation and classification

Original NDJSON exports and JSON audits are not migrated or rewritten. New producer records declare snapshot_version databento-live-2.0, validation_method_id databento-primary-gamma-gates-v1, the policy descriptor, and its SHA-256. Existing numeric gates are unchanged: 0.96–1.04 moneyness, 30 primary strikes, 10 nonzero strikes, 0.9 maximum concentration, and the existing configured paired-quote minimum. These describe primary gamma calculation checks, not every eligibility or prediction rule.

Derived review labels identify recorded, unrecorded, unrecognized, or malformed method metadata; preserve the original producer result; and always mark accuracy NOT_ESTABLISHED and retrospective revalidation false. Unrecorded does not mean incorrect. Producer-pass does not mean accurate. Truthy strings/numbers can no longer substitute for the explicit boolean validity flags.

The export UI selects one method/outcome group at a time. Downloads separate policies and producer outcomes, including CSVs in the end-of-day ZIP. Consumers expecting the old flat combined CSV/NDJSON filenames must use the new validation-groups folders or nested per-symbol ZIP. Historical identity-unverified records and invalid audits remain explicitly separate. Original fields are retained in derived copies with review annotations.

The debug viewer now calls arithmetic invariants arithmetic consistency only: zero placeholders can satisfy an identity without establishing a usable observation.

## Findings and acceptance gates

| Priority | Finding and evidence | Status / next gate |
|---|---|---|
| 1 | Primary dashboard at port 8501 still displayed “In-App Live Advisor — Uses live backend endpoints to produce structured proposal tiers.” at approximately 16:50 CT, instead of the current source interface. Backend/dashboard processes had morning start times. | Deployment gap observed. Previous guarded dashboard restart refused because process ownership could not be verified. No forced restart performed. Verify ownership, activate through the canonical launcher, then prove visible UI, loaded identities, fresh advancing backend and recorder evidence. Source tests alone cannot close this. |
| 2 | Latest VIX audit 20260914-195948 has three primary strikes in the ±4% band, versus the generic minimum 30. Listed strikes in that band are 16.5, 17, 17.5. Older Aug21 records lack current lineage fields despite sharing the old snapshot version. | VIX remains invalid. No relaxation or model tuning. Design a separately versioned, empirically validated VIX policy before considering any change. Preserve old producer-pass records as method-unrecorded. |
| 3 | build_etf_rich_features.py:130 emits symbol-major rows; train_etf_rich_model.py:58 slices rows 80/20 without global date sorting. Reproducing its finite-input filtering yielded 3,393 training rows and 849 validation rows, both spanning 2025-08-27–2026-09-08, with 202 overlapping dates. Feature builder line 154 normalizes volume using the complete history mean/std. | Confirmed research evaluation defects: split is not chronological and normalization uses future information. No training executed or artifacts modified. Require date-separated walk-forward evaluation and past-only feature computation before relying on reported performance. Not proven to drive the live prediction path. |
| 4 | backend/ai_predictor.py:30 caches a source-file hash on first call. Disk source could change after import but before that call. | Conditional provenance risk, not an observed corrupted forecast. Bind identity to loaded/startup code and test disk-change behavior before treating this hash as sufficient runtime proof. |
| 5 | backend/inference.py permits a missing feature_schema_version to be labeled legacy-checkpoint, though it rejects absent ordered features or feature-count mismatch. | Dormant compatibility risk. Startup log at 07:55:25 reported no gamma_model_*.pt installed and trained-model inference disabled. This is startup evidence, not proof about all later runtime state. Require explicit compatible metadata for future artifact activation. |

Reassuring exclusions: train_gamma_model.py:79 immediately rejects the retired training path; legacy backtesting is disabled in Databento mode, confirmed in source and dashboard. The live coordinator's default uses backend.ai_predictor.build_ai_prediction. Presence of old files alone is not proof that they run.

This is a targeted review of capture, validation/export, inference/training and runtime deployment. It is not an exhaustive guarantee that every application path is current.

## Exact verification

205 targeted tests passed in 12.84 seconds using the canonical virtual environment, bytecode disabled, pytest cache provider disabled:

```powershell
.\.venv\Scripts\python.exe -B -m pytest tests/test_validation_method_labels.py tests/test_snapshot_history.py tests/test_databento_streamer.py tests/test_snapshot_export_eligibility.py tests/test_app_snapshot_cache.py tests/test_app_prediction_export.py tests/test_export_catalog.py -q -p no:cacheprovider
```

An isolated Streamlit AppTest verified method selection and separate archive generation. SHA-256 before/after matched for all three sampled original sources:

| Original | SHA-256 before = after |
|---|---|
| exports/VIX/2026-08-21.ndjson | d0b2fa07c6b7d52da598aabaae11a554d9f35e1ce8438ec2eb9f5cc40b8c7223 |
| logs/audit/VIX/20260914-195948.json | 7b3f19b410b5c8a05a94b0d4df4bce995ce5d6d22bf55bf4be8aae3757d4d2db |
| logs/audit/SPX/20260914-195948.json | ce725fe272b53588671ec9cffa355dd6b522abb838c19676ae91910521ac83b5 |

Evidence is in the current Codex task's outputs: validation-method-tests.txt, validation-preservation-evidence.json, preserved-snapshot-validation-review.zip, and vix-readonly-review.json. The three-file preservation check is not a claim that all historical data has been validated.

## Premarket recurrence prevention

Verify loaded runtime identity against expected source; inspect a newly emitted record's method ID, descriptor and hash; verify per-symbol progression and explicit invalid reasons; confirm exports separate unknown methods and failures. Never upgrade an old producer-pass result to current eligibility or forecast accuracy by relabeling it. Keep live runtime verification OPEN until actual postconditions are observed.
