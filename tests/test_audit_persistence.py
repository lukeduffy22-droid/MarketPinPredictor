import json

from app.core import audit_persistence


def test_last_valid_snapshot_skips_explicitly_model_excluded_newer_row(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(audit_persistence, "AUDIT_LOG_DIR", tmp_path)
    audit_dir = tmp_path / "SPX"
    audit_dir.mkdir()
    older = {
        "symbol": "SPX",
        "generated_at_utc": "2026-09-09T14:59:00Z",
        "primary_gamma_pin_strike": 6500.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
    }
    newer_excluded = {
        **older,
        "generated_at_utc": "2026-09-09T15:00:00Z",
        "primary_gamma_pin_strike": 6600.0,
        "gamma_excluded_from_model": True,
    }
    (audit_dir / "20260909-145900.json").write_text(json.dumps(older))
    (audit_dir / "20260909-150000.json").write_text(json.dumps(newer_excluded))

    snapshot = audit_persistence.load_last_valid_snapshot("SPX")

    assert snapshot is not None
    assert snapshot.primary_gamma_pin_strike == 6500.0
    assert snapshot.gamma_excluded_from_model is False
