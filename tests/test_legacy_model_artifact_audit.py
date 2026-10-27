from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import torch

from backend.legacy_model_artifact_audit import (
    AUDIT_CONTRACT_VERSION,
    audit_legacy_gamma_artifact,
    audit_legacy_model_directory,
)
from tools.audit_legacy_model_artifacts import main as audit_cli_main


def _legacy_state_dict(*, non_finite: bool = False) -> dict[str, torch.Tensor]:
    state = {
        "net.0.weight": torch.zeros((64, 5), dtype=torch.float32),
        "net.0.bias": torch.zeros(64, dtype=torch.float32),
        "net.2.weight": torch.zeros((64, 64), dtype=torch.float32),
        "net.2.bias": torch.zeros(64, dtype=torch.float32),
        "net.4.weight": torch.zeros((1, 64), dtype=torch.float32),
        "net.4.bias": torch.zeros(1, dtype=torch.float32),
    }
    if non_finite:
        state["net.0.weight"][0, 0] = float("nan")
    return state


def _write_bundle(
    root: Path,
    *,
    symbol: str = "SPX",
    non_finite: bool = False,
) -> Path:
    models = root / "models"
    models.mkdir(parents=True)
    checkpoint = models / f"gamma_model_{symbol}.pt"
    torch.save(_legacy_state_dict(non_finite=non_finite), checkpoint)
    (models / f"gamma_model_{symbol}_meta.json").write_text(
        json.dumps(
            {
                "feature_columns": [
                    "spot",
                    "gross_gex",
                    "net_gex",
                    "call_gex",
                    "put_gex",
                ],
                "target": "distance_to_pin",
                "index_name": symbol,
                "rows_total": 200,
                "rows_train": 160,
                "rows_validation": 40,
                "device": "cuda",
                "scaling": {
                    "mean": [1.0, 2.0, 3.0, 4.0, 5.0],
                    "std": [1.0, 1.0, 1.0, 1.0, 1.0],
                },
            }
        ),
        encoding="utf-8",
    )
    (models / "training_report.json").write_text(
        json.dumps(
            {
                "exit_code": 1,
                "include_invalid": True,
                "min_train_rows": 500,
                "trained_symbols": [],
                "skipped_symbols": [
                    {
                        "symbol": symbol,
                        "reason": "usable_rows=200 below min_train_rows=500",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return checkpoint


def test_coherent_legacy_bundle_is_research_only_and_conversion_is_refused(
    tmp_path: Path,
) -> None:
    checkpoint = _write_bundle(tmp_path)

    audit = audit_legacy_gamma_artifact(checkpoint, symbol="SPX")

    assert audit.contract_version == AUDIT_CONTRACT_VERSION
    assert audit.checkpoint_format == "legacy-raw-state-dict"
    assert audit.tensor_contract_valid is True
    assert audit.metadata_contract_valid is True
    assert audit.training_report_authorizes_symbol is False
    assert audit.loadable_by_current_live_inference is False
    assert audit.migration_permitted is False
    assert audit.authority == "RESEARCH_ONLY"
    assert audit.promotion_eligible is False
    assert len(audit.checkpoint_sha256 or "") == 64
    assert "training report records a failed training run" in audit.reasons
    assert "training report does not prove invalid rows were excluded" in audit.reasons
    assert "metadata rows_total=200 is below training minimum=500" in audit.reasons
    assert "legacy checkpoint is not bound to its sidecars by SHA-256" in audit.reasons


def test_checkpoint_is_loaded_with_weights_only_unpickler(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checkpoint = _write_bundle(tmp_path)
    actual_load = torch.load
    observed: dict[str, object] = {}

    def recording_load(*args, **kwargs):
        observed.update(kwargs)
        return actual_load(*args, **kwargs)

    monkeypatch.setattr(torch, "load", recording_load)
    audit = audit_legacy_gamma_artifact(checkpoint, symbol="SPX")

    assert audit.tensor_contract_valid is True
    assert observed["weights_only"] is True
    assert observed["map_location"] == "cpu"


def test_non_finite_checkpoint_is_not_structurally_valid(tmp_path: Path) -> None:
    checkpoint = _write_bundle(tmp_path, non_finite=True)

    audit = audit_legacy_gamma_artifact(checkpoint, symbol="SPX")

    assert audit.checkpoint_format == "legacy-raw-state-dict"
    assert audit.tensor_contract_valid is False
    assert "checkpoint tensor net.0.weight contains non-finite values" in audit.reasons
    assert audit.migration_permitted is False


def test_complex_checkpoint_is_not_a_valid_float_mlp_contract(tmp_path: Path) -> None:
    checkpoint = _write_bundle(tmp_path)
    state = _legacy_state_dict()
    state["net.0.weight"] = state["net.0.weight"].to(torch.complex64)
    torch.save(state, checkpoint)

    audit = audit_legacy_gamma_artifact(checkpoint, symbol="SPX")

    assert audit.tensor_contract_valid is False
    assert "checkpoint tensor net.0.weight is not floating point" in audit.reasons
    assert audit.migration_permitted is False


def test_directory_audit_reports_evidence_shortfall_and_missing_ndx(
    tmp_path: Path,
) -> None:
    _write_bundle(tmp_path)
    readiness = tmp_path / "data" / "closing_tape" / "readiness_snapshot.json"
    readiness.parent.mkdir(parents=True)
    readiness.write_text(
        json.dumps(
            {
                "generated_at_utc": "2026-10-23T00:53:26+00:00",
                "model_evidence_sessions": 14,
                "model_sessions_required": 60,
                "model_training_ready": False,
                "paper_forecast_sessions": 0,
                "paper_sessions_required": 20,
                "paper_evidence_ready": False,
            }
        ),
        encoding="utf-8",
    )

    audit = audit_legacy_model_directory(tmp_path, refresh_readiness=False)

    assert audit.model_evidence_sessions == 14
    assert audit.model_sessions_required == 60
    assert audit.paper_forecast_sessions == 0
    assert audit.paper_sessions_required == 20
    assert audit.model_training_ready is False
    assert audit.paper_evidence_ready is False
    assert audit.symbols["SPX"]["tensor_contract_valid"] is True
    assert audit.symbols["NDX"]["checkpoint_present"] is False
    assert audit.readiness_source == "cached-snapshot"
    assert audit.readiness_current is False
    assert "cached readiness snapshot model evidence sessions 14/60" in audit.reasons
    assert "cached readiness snapshot paper forecast sessions 0/20" in audit.reasons
    assert "NDX legacy checkpoint is missing" in audit.reasons
    assert audit.migration_permitted is False
    assert audit.promotion_eligible is False


def test_cli_strict_mode_emits_json_and_returns_nonzero(
    tmp_path: Path,
    capsys,
) -> None:
    _write_bundle(tmp_path)

    exit_code = audit_cli_main(
        ["--project-root", str(tmp_path), "--snapshot-only", "--strict"]
    )
    emitted = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert emitted["authority"] == "RESEARCH_ONLY"
    assert emitted["migration_permitted"] is False
    assert emitted["symbols"]["SPX"]["checkpoint_format"] == "legacy-raw-state-dict"


def test_current_read_only_readiness_overrides_stale_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_bundle(tmp_path)
    snapshot = tmp_path / "data" / "closing_tape" / "readiness_snapshot.json"
    snapshot.parent.mkdir(parents=True)
    snapshot.write_text(
        json.dumps(
            {
                "generated_at_utc": "2026-10-23T00:53:26+00:00",
                "model_evidence_sessions": 14,
                "model_sessions_required": 60,
                "model_training_ready": False,
                "paper_forecast_sessions": 0,
                "paper_sessions_required": 20,
                "paper_evidence_ready": False,
            }
        ),
        encoding="utf-8",
    )
    live_payload = {
        "generated_at_utc": "2026-10-25T01:00:00+00:00",
        "model_evidence_sessions": 15,
        "model_sessions_required": 60,
        "model_training_ready": False,
        "paper_forecast_sessions": 0,
        "paper_sessions_required": 20,
        "paper_evidence_ready": False,
    }
    monkeypatch.setattr(
        "backend.closing_tape.readiness.audit_training_readiness",
        lambda _root: SimpleNamespace(to_dict=lambda: live_payload),
    )

    audit = audit_legacy_model_directory(tmp_path)

    assert audit.readiness_source == "live-read-only"
    assert audit.readiness_current is True
    assert audit.readiness_generated_at_utc == "2026-10-25T01:00:00+00:00"
    assert audit.model_evidence_sessions == 15
    assert "current read-only readiness model evidence sessions 15/60" in audit.reasons
    assert all("14/60" not in reason for reason in audit.reasons)

