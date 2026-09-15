"""Lossless closing-session tape capture and derived research features.

Raw Databento DBN files are the source of truth. SQLite rows in this package
are compact, point-in-time aggregates and manifests; they are not a substitute
for the raw provider records.

The package intentionally resolves its public API lazily. Control-plane code
such as readiness and status audits must not import pandas, Databento, Torch,
or replay/finalization code merely because Python initialized this package.
"""

from __future__ import annotations

from importlib import import_module


_EXPORTS = {
    "FeedSpec": "config",
    "SessionConfig": "config",
    "SubscriptionSpec": "config",
    "build_session_config": "config",
    "ComputeBenchmarkReport": "compute_benchmark",
    "benchmark_tabular_training": "compute_benchmark",
    "OnlineConformalReport": "calibration",
    "DeploymentConformalCalibration": "calibration",
    "bind_candidate_oos_predictions": "calibration",
    "build_online_conformal_intervals": "calibration",
    "evaluate_online_conformal_intervals": "calibration",
    "fit_deployment_conformal_calibration": "calibration",
    "FrozenCandidatePackage": "candidate",
    "load_paper_candidate_activation_receipt": "candidate",
    "package_promoted_candidate": "candidate",
    "write_paper_candidate_descriptor": "candidate",
    "attach_point_in_time_prices": "dataset",
    "load_complete_tape_features": "dataset",
    "load_marketpin_reference_prices": "dataset",
    "load_scored_marketpin_closes": "dataset",
    "TapeIntegrityReport": "integrity",
    "inspect_dbn": "integrity",
    "HistoricalImportResult": "historical_import",
    "VerifiedHistoricalBundle": "historical_import",
    "describe_historical_import": "historical_import",
    "import_historical_bundle": "historical_import",
    "verify_historical_bundle_manifest": "historical_import",
    "run_live_prefix_paper_shadow": "live_shadow",
    "FrozenModelArtifact": "model_artifact",
    "FrozenModelBuild": "model_artifact",
    "FrozenInferenceBenchmark": "model_artifact",
    "benchmark_frozen_model_inference": "model_artifact",
    "fit_promoted_frozen_model": "model_artifact",
    "load_frozen_model_artifact": "model_artifact",
    "PromotedModelRuntime": "production",
    "PromotedPrediction": "production",
    "load_promoted_close_predictions": "production",
    "load_promoted_model_runtime": "production",
    "predict_promoted_close": "production",
    "record_promoted_close_predictions": "production",
    "build_closing_tape_scorecard": "governance",
    "finish_forecast_attempt": "governance",
    "initialize_governance_ledger": "governance",
    "load_governed_promoted_close_predictions": "governance",
    "score_promoted_prediction_outcomes": "governance",
    "start_forecast_attempt": "governance",
    "RecorderLockError": "recorder",
    "SessionRecorder": "recorder",
    "TrainingReadinessReport": "readiness",
    "audit_training_readiness": "readiness",
    "finalize_closed_session_if_due": "postclose",
    "postclose_finalize_decision": "postclose",
    "PaperEvaluationReport": "paper",
    "evaluate_paper_forecasts": "paper",
    "record_paper_candidate_activation": "paper",
    "record_paper_forecast": "paper",
    "PaperShadowResult": "paper_shadow",
    "record_paper_shadow_predictions": "paper_shadow",
    "build_promoted_model_manifest": "promotion",
    "approve_and_write_promoted_model_manifest": "promotion",
    "load_promotion_approval_receipt": "promotion",
    "record_promotion_decision": "promotion",
    "write_promoted_model_manifest": "promotion",
    "RegimeEvaluationReport": "regimes",
    "build_online_volatility_regimes": "regimes",
    "evaluate_predictions_by_regime": "regimes",
    "build_minute_rows": "replay",
    "parse_occ_symbol": "replay",
    "persist_minute_rows": "replay",
    "TorchWalkForwardReport": "torch_research",
    "evaluate_torch_walk_forward": "torch_research",
    "WalkForwardReport": "research",
    "PrecomputedComparisonReport": "research",
    "build_forward_return_labels": "research",
    "build_session_close_labels": "research",
    "compare_precomputed_predictions": "research",
    "evaluate_ridge_walk_forward": "research",
    "purged_session_walk_forward": "research",
}

__all__ = tuple(_EXPORTS)


def __getattr__(name: str):
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f"{__name__}.{module_name}"), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted((*globals(), *__all__))
