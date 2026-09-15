"""Tests for model artifact metadata helpers."""

from app.models.model_artifacts import load_gamma_model_artifact


def test_load_gamma_model_artifact_reads_expected_metadata():
    metadata = load_gamma_model_artifact("SPX")

    assert metadata["symbol"] == "SPX"
    assert metadata["artifact_schema_version"] == "gamma-artifact-v1"
    assert "feature_columns" in metadata
    assert isinstance(metadata["feature_columns"], list)
