import hashlib
import json
import sqlite3
from datetime import date, datetime, timezone

from backend.closing_tape.dataset import PRODUCTION_FAMILIES
from backend.closing_tape.candidate import (
    CANDIDATE_PACKAGE_CONTRACT_VERSION,
    PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION,
    PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION,
)
from backend.closing_tape.calibration import DEPLOYMENT_CALIBRATION_METHOD
from backend.closing_tape.model_artifact import MODEL_ARTIFACT_FORMAT
from backend.closing_tape.paper_evidence import LIVE_PREFIX_RECEIPT_CONTRACT_VERSION
from backend.closing_tape.paper_shadow import record_paper_shadow_predictions
from backend.closing_tape.surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    build_contract_surface_features,
)
from tests.test_closing_tape_surface import _rows


UTC = timezone.utc


def _canonical_json_bytes(payload):
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8") + b"\n"


def _live_prefix_receipt(root, *, prefix_sha256="a" * 64):
    return {
        "contract_version": LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
        "prefix_sha256": prefix_sha256,
        "cutoff_bytes": 4096,
        "session_id": "s1",
        "feed_name": "opra_options",
        "horizon_id": "cash-close-minus-15m-v1",
        "trading_date": "2026-08-25",
        "feature_available_at_utc": "2026-08-25T19:45:00+00:00",
        "catalog_path": str((root / "catalog.sqlite").resolve()),
        "source_path": str((root / "source.dbn").resolve()),
    }


def _artifact(
    root,
    *,
    receipt_source_hash="a" * 64,
    activation_time="2026-08-25T19:44:00+00:00",
):
    models = root / "models"
    models.mkdir()
    payload = {
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "preprocessing": {
            "imputer_median": [0.0] * len(MODEL_FEATURE_COLUMNS),
            "scaler_mean": [0.0] * len(MODEL_FEATURE_COLUMNS),
            "scaler_scale": [1.0] * len(MODEL_FEATURE_COLUMNS),
        },
        "target": {"mean": 0.0, "scale": 1.0},
        "training": {
            "rows": 1, "epochs": 1, "seed": 17, "device": "cpu",
            "source_sha256s": ["a" * 64],
            "label_source_artifact_sha256s": ["b" * 64],
            "surface_artifact_sha256": "1" * 64,
            "surface_replay_receipt_sha256": "2" * 64,
        },
        "layers": [{"weight": [[0.0] * len(MODEL_FEATURE_COLUMNS)], "bias": [0.0]}],
    }
    artifact = models / "candidate.json"
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    receipt_payload = {
        "contract_version": CANDIDATE_PACKAGE_CONTRACT_VERSION,
        "version": "candidate-1",
        "artifact_path": artifact.name,
        "artifact_sha256": digest,
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "training_rows": 1,
        "training_epochs": 1,
        "training_device": "cpu",
        "source_sha256s": [receipt_source_hash],
        "label_source_artifact_sha256s": ["b" * 64],
        "surface_artifact_sha256": "1" * 64,
        "surface_replay_receipt_sha256": "2" * 64,
        "deployment_calibration": {
            "method": DEPLOYMENT_CALIBRATION_METHOD,
            "alpha": 0.1,
            "target_coverage": 0.9,
            "asof_utc": "2026-08-25T20:01:00+00:00",
            "model_version": "candidate-1",
            "artifact_sha256": digest,
            "eligible_rows": 5,
            "excluded_future_rows": 0,
            "evidence_sha256": "c" * 64,
            "source_sha256s": [receipt_source_hash],
            "label_source_artifact_sha256s": ["b" * 64],
            "family_radii": [
                {"family_root": family, "rows": 1, "sessions": 1,
                 "radius_log_return": 0.01}
                for family in PRODUCTION_FAMILIES
            ],
        },
    }
    receipt_bytes = _canonical_json_bytes(receipt_payload)
    receipt_hash = hashlib.sha256(receipt_bytes).hexdigest()
    receipts = models / "candidate_packages"
    receipts.mkdir()
    (receipts / f"{receipt_hash}.json").write_bytes(receipt_bytes)
    activation_payload = {
        "contract_version": PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION,
        "activated_at_utc": activation_time,
        "candidate_package_path": f"candidate_packages/{receipt_hash}.json",
        "candidate_package_sha256": receipt_hash,
        "model_version": "candidate-1",
        "artifact_sha256": digest,
    }
    activation_bytes = _canonical_json_bytes(activation_payload)
    activation_hash = hashlib.sha256(activation_bytes).hexdigest()
    activations = models / "paper_activations"
    activations.mkdir()
    (activations / f"{activation_hash}.json").write_bytes(activation_bytes)
    (models / "closing_tape_paper_candidate.json").write_text(
        json.dumps(
            {
                "contract_version": PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION,
                "enabled_for_paper": True,
                "activation_receipt_path": f"paper_activations/{activation_hash}.json",
                "activation_receipt_sha256": activation_hash,
            }
        ),
        encoding="utf-8",
    )
    return receipts / f"{receipt_hash}.json"


def test_shadow_noops_without_candidate(tmp_path):
    result = record_paper_shadow_predictions(
        build_contract_surface_features(_rows()), project_root=tmp_path,
        market_db_path=tmp_path / "market.db", trading_day=date(2026, 8, 25),
        session_id="s1",
        live_prefix_receipt=_live_prefix_receipt(tmp_path),
    )
    assert not result.configured
    assert result.recorded == 0


def test_shadow_records_exactly_five_cpu_predictions(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 45, 1, tzinfo=UTC),
    )
    _artifact(tmp_path)
    base = build_contract_surface_features(_rows()).iloc[0].to_dict()
    base["minute_utc"] = __import__("pandas").Timestamp("2026-08-25T19:44:00Z")
    base["feature_available_at_utc"] = __import__("pandas").Timestamp("2026-08-25T19:45:00Z")
    base["cash_close_utc"] = __import__("pandas").Timestamp("2026-08-25T20:00:00Z")
    base["minutes_to_cash_close"] = 15.0
    rows = []
    for family in PRODUCTION_FAMILIES:
        row = dict(base)
        row["family_root"] = family
        for root in PRODUCTION_FAMILIES:
            row[f"family_is_{root.lower()}"] = int(root == family)
        rows.append(row)

    result = record_paper_shadow_predictions(
        __import__("pandas").DataFrame(rows), project_root=tmp_path,
        market_db_path=tmp_path / "market.db", trading_day=date(2026, 8, 25),
        session_id="s1",
        live_prefix_receipt=_live_prefix_receipt(tmp_path),
    )

    assert result.recorded == 5
    with sqlite3.connect(tmp_path / "market.db") as connection:
        assert connection.execute("SELECT COUNT(*) FROM paper_close_forecasts").fetchone()[0] == 5


def test_shadow_rejects_tampered_candidate_receipt(tmp_path):
    receipt = _artifact(tmp_path)
    receipt.write_bytes(receipt.read_bytes() + b" ")

    result = record_paper_shadow_predictions(
        build_contract_surface_features(_rows()), project_root=tmp_path,
        market_db_path=tmp_path / "market.db", trading_day=date(2026, 8, 25),
        session_id="s1",
        live_prefix_receipt=_live_prefix_receipt(tmp_path),
    )

    assert result.recorded == 0
    assert result.reasons == ("paper candidate receipt hash mismatch",)


def test_shadow_rejects_artifact_provenance_outside_receipt(tmp_path):
    _artifact(tmp_path, receipt_source_hash="d" * 64)

    result = record_paper_shadow_predictions(
        build_contract_surface_features(_rows()), project_root=tmp_path,
        market_db_path=tmp_path / "market.db", trading_day=date(2026, 8, 25),
        session_id="s1",
        live_prefix_receipt=_live_prefix_receipt(tmp_path),
    )

    assert result.recorded == 0
    assert result.reasons == ("paper artifact DBN provenance mismatch",)


def test_shadow_rejects_surface_from_before_candidate_activation(tmp_path):
    _artifact(tmp_path, activation_time="2026-08-25T19:46:00+00:00")
    base = build_contract_surface_features(_rows()).iloc[0].to_dict()
    base["minute_utc"] = __import__("pandas").Timestamp("2026-08-25T19:44:00Z")
    base["feature_available_at_utc"] = __import__("pandas").Timestamp(
        "2026-08-25T19:45:00Z"
    )
    base["cash_close_utc"] = __import__("pandas").Timestamp("2026-08-25T20:00:00Z")
    base["minutes_to_cash_close"] = 15.0
    rows = []
    for family in PRODUCTION_FAMILIES:
        row = dict(base)
        row["family_root"] = family
        for root in PRODUCTION_FAMILIES:
            row[f"family_is_{root.lower()}"] = int(root == family)
        rows.append(row)

    result = record_paper_shadow_predictions(
        __import__("pandas").DataFrame(rows),
        project_root=tmp_path,
        market_db_path=tmp_path / "market.db",
        trading_day=date(2026, 8, 25),
        session_id="s1",
        live_prefix_receipt=_live_prefix_receipt(tmp_path),
    )

    assert result.recorded == 0
    assert result.reasons == ("paper surface predates candidate activation",)
