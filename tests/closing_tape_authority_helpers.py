from __future__ import annotations

from pathlib import Path

import pandas as pd

from backend.closing_tape import surface_artifact as artifact_module
from backend.closing_tape.catalog_discovery import CatalogDiscovery
from backend.closing_tape.pipeline import ResearchSurfaceReport
from backend.closing_tape.surface import MODEL_FEATURE_COLUMNS
from backend.closing_tape.surface_artifact import (
    load_replay_verified_research_surface_artifact,
    write_research_surface_artifact,
)
from backend.closing_tape.torch_research import (
    TorchFoldMetric,
    TorchOutOfSamplePrediction,
    TorchWalkForwardReport,
)


def issued_replay_surface(frame: pd.DataFrame, tmp_path: Path, monkeypatch):
    report = ResearchSurfaceReport(
        catalog_count=1,
        eligible_contract_rows=len(frame),
        surface_rows=len(frame),
        sessions=int(frame["session_id"].astype(str).nunique()),
        feature_schema_hash=str(frame.iloc[0]["feature_schema_hash"]),
        model_feature_contract_hash="unused-by-artifact-writer",
        model_feature_columns=(),
        source_sha256s=tuple(sorted(frame["source_sha256"].astype(str).unique())),
        family_coverage=(),
    )
    written = write_research_surface_artifact(
        frame,
        artifact_dir=tmp_path / "surface-artifacts",
        report=report,
        max_price_age_seconds=90.0,
    )
    catalog = tmp_path / "catalogs" / "2026-08-25" / "closing_tape.sqlite"
    selection = CatalogDiscovery(
        catalog_roots=(str(catalog.parents[1]),),
        catalog_paths=(catalog,),
        issues=(),
        resolution_id="c" * 64,
    )
    monkeypatch.setattr(
        artifact_module,
        "select_closing_tape_catalogs",
        lambda *_args, **_kwargs: selection,
    )

    def verify(_paths, *, claims):
        return (
            (catalog,),
            tuple(
                {
                    "trading_date": claim["trading_date"],
                    "session_id": claim["session_id"],
                    "feed_name": claim["feed_name"],
                    "source_sha256": claim["source_sha256"],
                    "source_kind": "databento_live",
                }
                for claim in claims
            ),
        )

    monkeypatch.setattr(
        artifact_module,
        "_resolve_and_verify_selected_sessions",
        verify,
    )
    monkeypatch.setattr(
        artifact_module,
        "build_research_surface_dataset",
        lambda *_args, **_kwargs: (frame.copy(), report),
    )
    return load_replay_verified_research_surface_artifact(
        written.manifest_path,
        project_root=tmp_path,
        market_db_path=tmp_path / "market.db",
    )


def make_torch_report(
    *,
    promoted: bool = True,
    device: str = "cpu",
    source_sha256s: tuple[str, ...] = ("a" * 64,),
    label_source_artifact_sha256s: tuple[str, ...] = ("b" * 64,),
    predictions: tuple[TorchOutOfSamplePrediction, ...] = (),
) -> TorchWalkForwardReport:
    fold = TorchFoldMetric(
        fold=0,
        device=device,
        epochs_trained=2,
        train_sessions=1,
        validation_sessions=0,
        test_sessions=1,
        train_rows=1,
        validation_rows=0,
        test_rows=max(1, len(predictions)),
        test_start_utc="2026-08-25T19:45:00+00:00",
        test_end_utc="2026-08-25T20:00:00+00:00",
        torch_mae=0.1,
        ridge_mae=0.2,
        zero_return_mae=0.3,
        torch_rmse=0.1,
        ridge_rmse=0.2,
        zero_return_rmse=0.3,
        torch_directional_accuracy=1.0,
        ridge_directional_accuracy=0.5,
        zero_return_directional_accuracy=0.0,
        incumbent_mae=0.25,
        incumbent_rmse=0.25,
        incumbent_directional_accuracy=0.5,
        incumbent_rows=max(1, len(predictions)),
    )
    return TorchWalkForwardReport(
        horizon_minutes=15,
        features=MODEL_FEATURE_COLUMNS,
        device=device,
        hidden_units=(4, 2),
        complete_sessions=max(1, len({item.session_id for item in predictions})),
        source_files=len(source_sha256s),
        source_sha256s=source_sha256s,
        label_source_artifact_sha256s=label_source_artifact_sha256s,
        held_out_rows=len(predictions),
        held_out_sessions=len({item.session_id for item in predictions}),
        folds=(fold,),
        family_metrics=(),
        out_of_sample_predictions=predictions,
        torch_mae=0.1,
        ridge_mae=0.2,
        zero_return_mae=0.3,
        incumbent_mae=0.25,
        incumbent_rmse=0.25,
        incumbent_directional_accuracy=0.5,
        incumbent_rows=len(predictions),
        incumbent_sessions=len({item.session_id for item in predictions}),
        torch_improvement_over_ridge_pct=50.0,
        torch_improvement_over_zero_pct=60.0,
        torch_improvement_over_ridge_ci_low_pct=1.0,
        torch_improvement_over_ridge_ci_high_pct=99.0,
        torch_improvement_over_zero_ci_low_pct=1.0,
        torch_improvement_over_zero_ci_high_pct=99.0,
        torch_improvement_over_incumbent_pct=50.0,
        torch_improvement_over_incumbent_ci_low_pct=1.0,
        torch_improvement_over_incumbent_ci_high_pct=99.0,
        torch_rmse=0.1,
        ridge_rmse=0.2,
        zero_return_rmse=0.3,
        torch_directional_accuracy=1.0,
        ridge_directional_accuracy=0.5,
        zero_return_directional_accuracy=0.0,
        promoted=promoted,
        promotion_reasons=() if promoted else ("test-only rejection",),
    )
