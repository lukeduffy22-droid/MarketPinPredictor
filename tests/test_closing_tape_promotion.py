from types import SimpleNamespace

import pytest

from backend.closing_tape.dataset import PRODUCTION_FAMILIES
from backend.closing_tape.calibration import DEPLOYMENT_CALIBRATION_METHOD
from backend.closing_tape.model_artifact import FrozenInferenceBenchmark
from backend.closing_tape.promotion import build_promoted_model_manifest
from backend.closing_tape.surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH


def _evidence():
    family_metrics = tuple(
        SimpleNamespace(
            family_root=family, rows=20, sessions=20,
            torch_mae=0.8, ridge_mae=1.0, zero_return_mae=1.1,
            incumbent_mae=1.05, incumbent_rows=20,
        )
        for family in PRODUCTION_FAMILIES
    )
    evaluation = SimpleNamespace(
        promoted=True, features=MODEL_FEATURE_COLUMNS, complete_sessions=60,
        folds=tuple(range(5)), family_metrics=family_metrics,
        held_out_rows=100, held_out_sessions=20,
        incumbent_rows=100, incumbent_sessions=20,
        source_sha256s=tuple(f"{value:064x}" for value in range(1, 61)),
        label_source_artifact_sha256s=tuple(
            f"{1000 + value:064x}" for value in range(300)
        ),
        torch_improvement_over_ridge_pct=20.0,
        torch_improvement_over_ridge_ci_low_pct=5.0,
        torch_improvement_over_incumbent_pct=10.0,
        torch_improvement_over_incumbent_ci_low_pct=2.0,
    )
    calibration = SimpleNamespace(
        family_metrics=tuple(
            SimpleNamespace(family_root=family, sessions=20, coverage_error=0.02)
            for family in PRODUCTION_FAMILIES
        )
    )
    candidate = SimpleNamespace(
        metrics=tuple(
            SimpleNamespace(family_root=family, volatility_regime=regime, sessions=5, mae=0.8)
            for family in PRODUCTION_FAMILIES
            for regime in ("calm", "normal", "stressed")
        )
    )
    ridge = SimpleNamespace(
        metrics=tuple(
            SimpleNamespace(family_root=family, volatility_regime=regime, sessions=5, mae=1.0)
            for family in PRODUCTION_FAMILIES
            for regime in ("calm", "normal", "stressed")
        )
    )
    return evaluation, calibration, candidate, ridge


def _deployment_calibration(**overrides):
    values = dict(
        method=DEPLOYMENT_CALIBRATION_METHOD, alpha=0.1,
        target_coverage=0.9, asof_utc="2026-08-25T21:00:00+00:00",
        model_version="candidate-1", artifact_sha256="a" * 64,
        eligible_rows=100, excluded_future_rows=0,
        evidence_sha256="c" * 64,
        source_sha256s=tuple(f"{value:064x}" for value in range(1, 21)),
        label_source_artifact_sha256s=tuple(
            f"{1000 + value:064x}" for value in range(100)
        ),
        family_radii=tuple(
            SimpleNamespace(
                family_root=family, rows=20, sessions=20,
                radius_log_return=0.01,
            )
            for family in PRODUCTION_FAMILIES
        ),
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _build(**overrides):
    evaluation, calibration, candidate, ridge = _evidence()
    paper = SimpleNamespace(
        model_version="candidate-1", artifact_sha256="a" * 64,
        evidence_sha256="d" * 64,
        activation_receipt_sha256s=("e" * 64,),
        close_source_artifact_sha256s=tuple(
            f"{2000 + value:064x}" for value in range(100)
        ),
        prefix_replay_receipt_sha256s=tuple(
            f"{3000 + value:064x}" for value in range(20)
        ),
        final_tape_source_sha256s=tuple(
            f"{4000 + value:064x}" for value in range(20)
        ),
        campaign_opportunities=20,
        campaign_coverage_receipt_sha256="f" * 64,
        complete_sessions=20, eligible_rows=100,
        candidate_mae=0.8, incumbent_mae=1.0,
        family_metrics=tuple(
            SimpleNamespace(
                family_root=family, rows=20, sessions=20,
                candidate_mae=0.8, incumbent_mae=1.0,
            )
            for family in PRODUCTION_FAMILIES
        ),
    )
    arguments = dict(
        evaluation_report=evaluation, calibration_report=calibration,
        deployment_calibration=_deployment_calibration(),
        candidate_regime_report=candidate, ridge_regime_report=ridge,
        source_sha256s=[f"{value:064x}" for value in range(1, 61)],
        artifact_path="candidate.json", artifact_sha256="a" * 64,
        surface_artifact_sha256="b" * 64,
        surface_replay_receipt_sha256="c" * 64,
        paper_report=paper, version="candidate-1", execution_device="cpu",
        inference_batch_rows=5,
    )
    arguments.update(overrides)
    return build_promoted_model_manifest(**arguments)


def test_manifest_is_derived_from_connected_evidence():
    manifest = _build()

    assert manifest["enabled"]
    assert manifest["feature_schema_hash"] == MODEL_FEATURE_CONTRACT_HASH
    assert manifest["complete_sessions"] == 60
    assert manifest["incumbent_rows"] == manifest["held_out_rows"] == 100
    assert len(manifest["family_metrics"]) == 5
    assert len(manifest["calibration_metrics"]) == 5
    assert len(manifest["regime_metrics"]) == 15
    assert manifest["execution_device"] == "cpu"
    assert len(manifest["deployment_calibration"]["family_radii"]) == 5
    assert manifest["deployment_calibration"]["eligible_rows"] == 100
    assert manifest["deployment_calibration"]["excluded_future_rows"] == 0
    assert manifest["deployment_calibration"]["evidence_sha256"] == "c" * 64
    assert len(manifest["label_source_artifact_sha256s"]) == 300
    assert len(manifest["paper_close_source_artifact_sha256s"]) == 100
    assert manifest["paper_evidence_sha256"] == "d" * 64
    assert manifest["paper_activation_receipt_sha256s"] == ["e" * 64]
    assert len(manifest["paper_prefix_replay_receipt_sha256s"]) == 20
    assert len(manifest["paper_final_tape_source_sha256s"]) == 20
    assert manifest["paper_campaign_opportunities"] == 20
    assert manifest["paper_campaign_coverage_receipt_sha256"] == "f" * 64
    assert manifest["surface_artifact_sha256"] == "b" * 64
    assert manifest["surface_replay_receipt_sha256"] == "c" * 64
    assert len(
        manifest["deployment_calibration"]["label_source_artifact_sha256s"]
    ) == 100


def test_manifest_refuses_insufficient_paper_or_unpromoted_evidence():
    _evaluation, _calibration, _candidate, _ridge = _evidence()
    paper = SimpleNamespace(
        model_version="candidate-1", artifact_sha256="a" * 64,
        complete_sessions=19, eligible_rows=95,
        candidate_mae=0.8, incumbent_mae=1.0,
        family_metrics=(),
    )
    with pytest.raises(ValueError, match="20 paper"):
        _build(paper_report=paper)
    evaluation, calibration, candidate, ridge = _evidence()
    evaluation.promoted = False
    with pytest.raises(ValueError, match="not promoted"):
        _build(evaluation_report=evaluation)


def test_manifest_refuses_missing_or_multiple_paper_activation_receipts():
    paper = _build()["paper_activation_receipt_sha256s"]
    assert paper == ["e" * 64]

    evaluation, calibration, candidate, ridge = _evidence()
    report = SimpleNamespace(
        model_version="candidate-1",
        artifact_sha256="a" * 64,
        evidence_sha256="d" * 64,
        activation_receipt_sha256s=(),
        close_source_artifact_sha256s=tuple(
            f"{2000 + value:064x}" for value in range(100)
        ),
        prefix_replay_receipt_sha256s=tuple(
            f"{3000 + value:064x}" for value in range(20)
        ),
        final_tape_source_sha256s=tuple(
            f"{4000 + value:064x}" for value in range(20)
        ),
        campaign_opportunities=20,
        campaign_coverage_receipt_sha256="f" * 64,
        complete_sessions=20,
        eligible_rows=100,
        candidate_mae=0.8,
        incumbent_mae=1.0,
        family_metrics=tuple(
            SimpleNamespace(
                family_root=family,
                rows=20,
                sessions=20,
                candidate_mae=0.8,
                incumbent_mae=1.0,
            )
            for family in PRODUCTION_FAMILIES
        ),
    )
    with pytest.raises(ValueError, match="exactly one valid candidate activation"):
        _build(paper_report=report)
    report.activation_receipt_sha256s = ("e" * 64, "f" * 64)
    with pytest.raises(ValueError, match="exactly one valid candidate activation"):
        _build(paper_report=report)


def test_manifest_requires_complete_unique_paper_prefix_replay_proof():
    evaluation, calibration, candidate, ridge = _evidence()
    base = SimpleNamespace(
        model_version="candidate-1",
        artifact_sha256="a" * 64,
        evidence_sha256="d" * 64,
        activation_receipt_sha256s=("e" * 64,),
        close_source_artifact_sha256s=tuple(
            f"{2000 + value:064x}" for value in range(100)
        ),
        prefix_replay_receipt_sha256s=(),
        final_tape_source_sha256s=(),
        campaign_opportunities=20,
        campaign_coverage_receipt_sha256="f" * 64,
        complete_sessions=20,
        eligible_rows=100,
        candidate_mae=0.8,
        incumbent_mae=1.0,
        family_metrics=tuple(
            SimpleNamespace(
                family_root=family,
                rows=20,
                sessions=20,
                candidate_mae=0.8,
                incumbent_mae=1.0,
            )
            for family in PRODUCTION_FAMILIES
        ),
    )
    with pytest.raises(ValueError, match="prefix replay or campaign coverage"):
        _build(paper_report=base)

    base.prefix_replay_receipt_sha256s = tuple("a" * 64 for _ in range(20))
    base.final_tape_source_sha256s = tuple(
        f"{4000 + value:064x}" for value in range(20)
    )
    with pytest.raises(ValueError, match="prefix replay or campaign coverage"):
        _build(paper_report=base)

    base.prefix_replay_receipt_sha256s = tuple(
        f"{3000 + value:064x}" for value in range(20)
    )
    base.campaign_coverage_receipt_sha256 = "invalid"
    with pytest.raises(ValueError, match="prefix replay or campaign coverage"):
        _build(paper_report=base)


def test_manifest_rejects_unbound_or_incomplete_deployment_calibration():
    with pytest.raises(ValueError, match="model version"):
        _build(deployment_calibration=_deployment_calibration(model_version="other"))

    radii = _deployment_calibration().family_radii[:-1]
    with pytest.raises(ValueError, match="every production family"):
        _build(deployment_calibration=_deployment_calibration(family_radii=radii))

    radii = list(_deployment_calibration().family_radii)
    radii[0] = SimpleNamespace(
        family_root=radii[0].family_root, rows=19, sessions=19,
        radius_log_return=0.01,
    )
    with pytest.raises(ValueError, match="calibration evidence is insufficient"):
        _build(deployment_calibration=_deployment_calibration(family_radii=tuple(radii)))

    with pytest.raises(ValueError, match="OOS evidence hash"):
        _build(deployment_calibration=_deployment_calibration(evidence_sha256="invalid"))

    with pytest.raises(ValueError, match="row counters do not match"):
        _build(deployment_calibration=_deployment_calibration(eligible_rows=99))

    with pytest.raises(ValueError, match="row counters are invalid"):
        _build(
            deployment_calibration=_deployment_calibration(
                excluded_future_rows=-1
            )
        )

    with pytest.raises(ValueError, match="close artifacts"):
        _build(
            deployment_calibration=_deployment_calibration(
                label_source_artifact_sha256s=("f" * 64,)
            )
        )


def test_manifest_rejects_caller_supplied_dbn_hashes_outside_evaluation():
    with pytest.raises(ValueError, match="DBN source hashes"):
        _build(source_sha256s=[f"{value:064x}" for value in range(2, 62)])


def test_manifest_requires_surface_replay_provenance():
    with pytest.raises(ValueError, match="surface artifact and replay receipt"):
        _build(surface_replay_receipt_sha256="invalid")


def test_cuda_manifest_requires_matching_passing_workload():
    benchmark = FrozenInferenceBenchmark(
        rows=100_000, repeats=5, cpu_median_milliseconds=70.0,
        cuda_available=True, cuda_median_milliseconds=45.0,
        cuda_speedup_vs_cpu=1.55, max_abs_prediction_difference=1e-9,
        prediction_tolerance=1e-6, numerically_equivalent=True,
    )
    with pytest.raises(ValueError, match="deployed workload"):
        _build(
            execution_device="cuda", inference_batch_rows=5,
            inference_benchmark=benchmark,
        )
