import json
import sqlite3
from datetime import datetime, timedelta, timezone

import torch

from backend.ai_predictor import build_ai_prediction
from backend.research.cuda_shadow import (
    CudaShadowResearchRecorder,
    cuda_shadow_runtime_status,
)


UTC = timezone.utc


def _payload(index_at: datetime) -> dict:
    return {
        "symbol": "SPX",
        "provider": "databento",
        "calculation_id": "calc-cuda-shadow",
        "price": 100.0,
        "likely_close": 100.5,
        "gamma_pin": 101.0,
        "zero_gamma": 99.0,
        "max_pain": 100.0,
        "gross_gex": 1_000.0,
        "net_gex": 250.0,
        "top_strike_share": 0.15,
        "confidence": 0.8,
        "expirations_min_days": 0,
        "validation_is_valid": True,
        "validation_failure_reasons": [],
        "gamma_excluded_from_model": False,
        "usable_for_prediction": True,
        "universe_sha256": "definition-hash",
        "selected_universe_sha256": "selected-hash",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 7,
        "instrument_definition_version": "universe-sha256:definition-hash",
        "symbol_mapping_version": "mapping-set-hash",
        "mapping_version_missing_count": 0,
        "latest_ts_event_utc": (index_at - timedelta(seconds=1)).isoformat(),
        "latest_ts_recv_utc": (index_at - timedelta(milliseconds=10)).isoformat(),
        "observation_index_utc": index_at.isoformat(),
        "quote_age_max_seconds": 2.0,
        "fresh_quote_count": 80,
        "expected_quote_count": 100,
        "paired_quote_count": 40,
        "primary_pair_coverage_ratio": 1.0,
        "processing_clock_telemetry": {"status": "synchronized"},
        "contributing_crossed_quote_count": 0,
        "provider_timestamp_order_errors": 0,
        "universe_provenance": {"mode": "unit-test"},
    }


class _FakeCudaEngine:
    device = torch.device("cuda:0")

    def __init__(self, *, loaded: bool = True):
        self.models = {"SPX": object()} if loaded else {}
        self.calls = 0

    def get_model_load_evidence(self, symbol=None):
        evidence = {
            "SPX": {
                "symbol": "SPX",
                "status": "LOADED_SHADOW_ONLY" if self.models else "REJECTED",
                "research_only": True,
                "promotion_allowed": False,
                "artifact_sha256": "a" * 64,
                "feature_names": ["price"],
                "feature_schema_version": "databento-gex-structure-v1",
                "output_contract": {
                    "schema_version": "marketpin-cuda-shadow-output-v1",
                    "kind": "future_price",
                    "units": "index_points",
                    "prediction_horizon_seconds": 300,
                    "research_only": True,
                    "promotion_allowed": False,
                    "max_abs_move_bps": 500.0,
                },
                "error": None if self.models else "missing output contract",
            }
        }
        return evidence.get(symbol, {}) if symbol else evidence

    def predict_from_live_payload(self, symbol, payload, *, current_price=None):
        self.calls += 1
        return {
            "symbol": symbol,
            "predicted_value": 103.0,
            "confidence": 0.65,
            "inference_time_ms": 1.25,
            "artifact_sha256": "a" * 64,
            "inference_device": "cuda:0",
            "research_only": True,
            "promotion_allowed": False,
            "output_contract": {
                "schema_version": "marketpin-cuda-shadow-output-v1",
                "kind": "future_price",
                "units": "index_points",
                "prediction_horizon_seconds": 300,
                "research_only": True,
                "promotion_allowed": False,
                "max_abs_move_bps": 500.0,
            },
            "feature_snapshot": {"price": current_price},
        }


class _ExplodingEngine(_FakeCudaEngine):
    def predict_from_live_payload(self, *args, **kwargs):
        raise AssertionError("disabled shadow recorder must not invoke inference")


class _ImplausibleCudaEngine(_FakeCudaEngine):
    def predict_from_live_payload(self, symbol, payload, *, current_price=None):
        result = super().predict_from_live_payload(
            symbol, payload, current_price=current_price
        )
        result["predicted_value"] = 200.0
        return result


class _InvalidArtifactIdentityEngine(_FakeCudaEngine):
    def get_model_load_evidence(self, symbol=None):
        evidence = super().get_model_load_evidence(symbol)
        if symbol:
            evidence["artifact_sha256"] = "NOT-A-SHA"
        else:
            evidence["SPX"]["artifact_sha256"] = "NOT-A-SHA"
        return evidence


def test_disabled_cuda_shadow_is_noop_and_primary_prediction_is_unchanged(
    tmp_path, monkeypatch
):
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    payload = _payload(index_at)
    monkeypatch.setattr(
        "backend.ai_predictor.inference_device", lambda: torch.device("cpu")
    )
    before = build_ai_prediction("SPX", payload)
    path = tmp_path / "cuda-shadow.db"
    recorder = CudaShadowResearchRecorder(
        inference_engine=_ExplodingEngine(),
        journal_path=path,
        enabled=False,
    )

    result = recorder.record(payload, processed_at_utc=index_at)
    after = build_ai_prediction("SPX", payload)

    assert result.skipped_reason == "CUDA_SHADOW_RESEARCH_DISABLED"
    assert recorder.journal is None
    assert not path.exists()
    assert {k: v for k, v in before.items() if k != "timestamp"} == {
        k: v for k, v in after.items() if k != "timestamp"
    }
    assert "cuda_shadow" not in after


def test_disabled_status_still_exposes_artifact_rejection_evidence(monkeypatch, tmp_path):
    engine = _FakeCudaEngine(loaded=False)
    monkeypatch.setenv(
        "CUDA_SHADOW_RESEARCH_DB_PATH", str(tmp_path / "missing-shadow.db")
    )
    monkeypatch.setattr(
        "backend.research.cuda_shadow.get_inference_engine", lambda: engine
    )

    status = cuda_shadow_runtime_status(enabled=False)

    assert status["status"] == "DISABLED"
    assert status["research_only"] is True
    assert status["promotion_allowed"] is False
    assert status["artifact_evidence"]["SPX"]["status"] == "REJECTED"
    assert status["journal_present"] is False
    assert engine.calls == 0


def test_status_reports_partial_core_readiness_per_symbol():
    status = cuda_shadow_runtime_status(engine=_FakeCudaEngine(), enabled=True)

    assert status["status"] == "PARTIAL_READY_SHADOW_ONLY"
    assert status["symbol_readiness"]["SPX"]["ready"] is True
    assert status["symbol_readiness"]["NDX"]["ready"] is False


def test_enabled_shadow_without_valid_model_does_not_create_journal(tmp_path):
    path = tmp_path / "cuda-shadow.db"
    recorder = CudaShadowResearchRecorder(
        inference_engine=_FakeCudaEngine(loaded=False),
        journal_path=path,
        enabled=True,
    )

    result = recorder.record(
        _payload(datetime(2026, 8, 25, 15, 0, tzinfo=UTC))
    )

    assert result.skipped_reason == "MODEL_ARTIFACT_NOT_LOADED"
    assert recorder.journal is None
    assert not path.exists()


def test_valid_cuda_artifact_result_is_shadow_only_and_separately_persisted(tmp_path):
    path = tmp_path / "cuda-shadow.db"
    engine = _FakeCudaEngine()
    recorder = CudaShadowResearchRecorder(
        inference_engine=engine,
        journal_path=path,
        enabled=True,
    )
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)

    result = recorder.record(
        _payload(index_at),
        processed_at_utc=index_at + timedelta(milliseconds=50),
    )

    assert result.error is None
    assert result.saved_count == 1
    assert engine.calls == 1
    prediction = result.prediction
    assert prediction is not None
    assert prediction.predicted_price == 103.0
    assert prediction.mode == "shadow"
    assert prediction.formula.fitted is True
    assert prediction.formula.promotion_allowed is False
    assert prediction.formula.required_raw_features == ("price",)
    assert prediction.formula.version.endswith("a" * 64)
    assert prediction.production_signal_replaced is False
    assert prediction.confidence_kind == "uncalibrated internal score; not a probability"
    assert prediction.provenance["primary_prediction_replaced"] is False
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT mode, production_signal_replaced, formula_id, provenance_json "
            "FROM shadow_prediction_journal"
        ).fetchone()
    assert row[:3] == ("shadow", 0, "shadow-cuda-artifact")
    persisted_provenance = json.loads(row[3])
    assert persisted_provenance["artifact_feature_contract"][
        "required_raw_features"
    ] == ["price"]
    assert persisted_provenance["model_load_evidence"]["artifact_sha256"] == "a" * 64


def test_invalid_live_evidence_abstains_without_calling_cuda(tmp_path):
    engine = _FakeCudaEngine()
    recorder = CudaShadowResearchRecorder(
        inference_engine=engine,
        journal_path=tmp_path / "cuda-shadow.db",
        enabled=True,
    )
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    payload = _payload(index_at)
    payload["processing_clock_telemetry"] = {"status": "unsynchronized"}

    result = recorder.record(
        payload,
        processed_at_utc=index_at + timedelta(milliseconds=50),
    )

    assert engine.calls == 0
    assert result.prediction is not None
    assert result.prediction.abstained is True
    assert result.prediction.predicted_price is None
    assert "PROCESSING_CLOCK_UNSYNCHRONIZED" in result.prediction.abstention_reasons


def test_implausible_model_move_is_persisted_only_as_abstention(tmp_path):
    recorder = CudaShadowResearchRecorder(
        inference_engine=_ImplausibleCudaEngine(),
        journal_path=tmp_path / "cuda-shadow.db",
        enabled=True,
    )
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)

    result = recorder.record(
        _payload(index_at),
        processed_at_utc=index_at + timedelta(milliseconds=50),
    )

    assert result.saved_count == 1
    assert result.prediction is not None
    assert result.prediction.abstained is True
    assert result.prediction.predicted_price is None
    assert "MODEL_MOVE_EXCEEDS_CONTRACT_BOUND" in result.prediction.abstention_reasons


def test_invalid_artifact_identity_never_runs_or_writes_shadow_record(tmp_path):
    path = tmp_path / "cuda-shadow.db"
    engine = _InvalidArtifactIdentityEngine()
    recorder = CudaShadowResearchRecorder(
        inference_engine=engine,
        journal_path=path,
        enabled=True,
    )
    assert recorder.journal is None

    result = recorder.record(
        _payload(datetime(2026, 8, 25, 15, 0, tzinfo=UTC))
    )

    assert result.skipped_reason == "MODEL_ARTIFACT_SHA256_INVALID"
    assert engine.calls == 0
    assert not path.exists()
