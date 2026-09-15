import sqlite3
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.closing_tape import paper_evidence
from backend.closing_tape.dataset import PRODUCTION_FAMILIES
from backend.closing_tape.paper import (
    evaluate_paper_forecasts,
    record_paper_candidate_activation,
    record_paper_forecast,
)
from backend.closing_tape.paper_evidence import (
    PaperCampaignCoverage,
    PaperPrefixReplayVerification,
    parse_paper_feature_evidence,
)
from backend.closing_tape.surface import MODEL_FEATURE_COLUMNS
from tests.test_closing_tape_paper_shadow import _artifact, _live_prefix_receipt


UTC = timezone.utc
OFFICIAL_SOURCES = {
    "SPX": ("sp-global-official", "https://www.spglobal.com/spx"),
    "NDX": ("nasdaq-official", "https://www.nasdaq.com/ndx"),
    "RUT": ("ftse-russell-official", "https://www.lseg.com/rut"),
    "VIX": ("cboe-official", "https://www.cboe.com/vix"),
    "SPY": ("nyse-arca-official", "https://www.nyse.com/spy"),
}


def _close_table(path):
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY, symbol TEXT, trading_date TEXT,
                official_close REAL, source TEXT, source_reference TEXT,
                source_verified INTEGER,
                observed_at_utc TEXT, source_artifact_sha256 TEXT,
                correction_of_id INTEGER
            )
            """
        )


def _model_features(family):
    values = {column: 0.0 for column in MODEL_FEATURE_COLUMNS}
    for root in PRODUCTION_FAMILIES:
        values[f"family_is_{root.lower()}"] = float(root == family)
    values["minutes_to_cash_close"] = 15.0
    return values


def _candidate_identity(root):
    receipt_path = _artifact(root)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    activation_path = next((root / "models" / "paper_activations").glob("*.json"))
    activation = json.loads(activation_path.read_text(encoding="utf-8"))
    return {
        "activation_sha256": activation_path.stem,
        "package_sha256": receipt_path.stem,
        "artifact_sha256": str(receipt["artifact_sha256"]),
        "activated_at_utc": datetime.fromisoformat(
            str(activation["activated_at_utc"])
        ),
    }


def _mock_replay_evidence(monkeypatch):
    def audit(forecast_rows, **_kwargs):
        payloads = []
        recorded = {}
        for row in forecast_rows:
            identity = {
                "forecast_key": str(row["forecast_key"]),
                "model_version": str(row["model_version"]),
                "artifact_sha256": str(row["artifact_sha256"]).lower(),
                "source_sha256": str(row["source_sha256"]).lower(),
                "session_id": str(row["session_id"]),
                "family_root": str(row["family_root"]).upper(),
                "trading_date": str(row["trading_date"]),
                "decision_horizon_minutes": int(
                    row["decision_horizon_minutes"]
                ),
                "feature_available_at_utc": str(
                    row["feature_available_at_utc"]
                ),
                "reference_price": float(row["reference_price"]),
                "incumbent_predicted_close": float(
                    row["incumbent_predicted_close"]
                ),
                "feature_contract_hash": str(row["feature_contract_hash"]),
            }
            parsed = parse_paper_feature_evidence(
                str(row["feature_payload_json"]),
                expected_sha256=str(row["feature_payload_sha256"]),
                expected_forecast_identity=identity,
            )
            payloads.append(parsed)
            recorded[parsed.feature_payload_sha256] = str(
                row["recorded_at_utc"]
            )
        return PaperCampaignCoverage(
            payloads=tuple(payloads),
            recorded_at_by_payload=recorded,
            opportunity_count=1,
            receipt_sha256="8" * 64,
        )

    def replay(payloads, **_kwargs):
        return PaperPrefixReplayVerification(
            payload_receipt_sha256s={
                item.feature_payload_sha256: "7" * 64 for item in payloads
            },
            receipt_sha256s=("7" * 64,),
            final_source_sha256s=("6" * 64,),
        )

    monkeypatch.setattr(
        "backend.closing_tape.paper.audit_paper_campaign_coverage", audit
    )
    monkeypatch.setattr(
        "backend.closing_tape.paper.replay_verify_paper_feature_groups", replay
    )
    monkeypatch.setattr(
        "backend.closing_tape.paper.retain_paper_promotion_evidence",
        lambda *_args, **_kwargs: None,
    )


def test_paper_ledger_is_immutable_idempotent_and_scores_complete_sessions(
    tmp_path, monkeypatch
):
    path = tmp_path / "market.db"
    _close_table(path)
    trading_day = date(2026, 8, 25)
    vault = tmp_path / "verified_close_sources"
    available = datetime(2026, 8, 25, 19, 45, tzinfo=UTC)
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 45, 1, tzinfo=UTC),
    )
    _mock_replay_evidence(monkeypatch)
    candidate = _candidate_identity(tmp_path)
    record_paper_candidate_activation(
        path,
        activation_receipt_sha256=candidate["activation_sha256"],
        model_version="candidate-1",
        artifact_sha256=candidate["artifact_sha256"],
        candidate_package_sha256=candidate["package_sha256"],
        activated_at_utc=candidate["activated_at_utc"],
    )
    prefix_hash = "f" * 64
    prefix_receipt = _live_prefix_receipt(
        tmp_path, prefix_sha256=prefix_hash
    )
    for index, family in enumerate(PRODUCTION_FAMILIES):
        reference = 100.0 + index
        key = record_paper_forecast(
            path, model_version="candidate-1",
            artifact_sha256=candidate["artifact_sha256"],
            source_sha256=prefix_hash, session_id="s1",
            family_root=family, trading_day=trading_day,
            decision_horizon_minutes=15, feature_available_at_utc=available,
            reference_price=reference,
            candidate_predicted_log_return=0.0,
            incumbent_predicted_close=reference + 2.0,
            model_features=_model_features(family),
            live_prefix_receipt=prefix_receipt,
        )
        assert key == record_paper_forecast(
            path, model_version="candidate-1",
            artifact_sha256=candidate["artifact_sha256"],
            source_sha256=prefix_hash, session_id="s1",
            family_root=family, trading_day=trading_day,
            decision_horizon_minutes=15, feature_available_at_utc=available,
            reference_price=reference,
            candidate_predicted_log_return=0.0,
            incumbent_predicted_close=reference + 2.0,
            model_features=_model_features(family),
            live_prefix_receipt=prefix_receipt,
        )
        with sqlite3.connect(path) as connection:
            artifact = f"official {family} close".encode()
            artifact_hash = hashlib.sha256(artifact).hexdigest()
            artifact_path = (
                vault / trading_day.isoformat() / family / f"{artifact_hash}.txt"
            )
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_bytes(artifact)
            source, source_reference = OFFICIAL_SOURCES[family]
            connection.execute(
                "INSERT INTO eod_close_observations VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    index + 1, family, trading_day.isoformat(), reference + 0.5,
                    source, source_reference, 1,
                    "2026-08-25T21:01:00+00:00", artifact_hash, None,
                ),
            )

    report = evaluate_paper_forecasts(
        path, project_root=tmp_path, model_version="candidate-1",
        verified_artifact_root=vault
    )

    assert report.complete_sessions == 1
    assert report.eligible_rows == 5
    assert report.candidate_mae == pytest.approx(0.5)
    assert report.incumbent_mae == pytest.approx(1.5)
    assert len(report.evidence_sha256) == 64
    assert report.activation_receipt_sha256s == (
        candidate["activation_sha256"],
    )
    assert report.prefix_replay_receipt_sha256s == ("7" * 64,)
    assert report.final_tape_source_sha256s == ("6" * 64,)
    assert report.campaign_opportunities == 1
    assert report.campaign_coverage_receipt_sha256 == "8" * 64
    assert len(report.close_source_artifact_sha256s) == 5
    assert all(item.candidate_mae < item.incumbent_mae for item in report.family_metrics)
    with sqlite3.connect(path) as connection:
        existing = connection.execute(
            """
            SELECT source, source_reference, source_artifact_sha256
            FROM eod_close_observations WHERE symbol='SPX'
            """
        ).fetchone()
        connection.execute(
            "INSERT INTO eod_close_observations VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                99, "SPX", trading_day.isoformat(), 999.0,
                existing[0], existing[1], 1,
                "2026-08-25T21:02:00+00:00", existing[2], None,
            ),
        )
    with pytest.raises(ValueError, match="conflicting verified closes"):
        evaluate_paper_forecasts(
            path, project_root=tmp_path, model_version="candidate-1",
            verified_artifact_root=vault
        )
    with sqlite3.connect(path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("UPDATE paper_close_forecasts SET reference_price=1")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM paper_close_forecasts")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE paper_forecast_feature_payloads "
                "SET feature_payload_json='{}'"
            )


def test_paper_ledger_rejects_conflicting_duplicate(tmp_path, monkeypatch):
    path = tmp_path / "market.db"
    available = datetime(2026, 8, 25, 19, 45, tzinfo=UTC)
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 45, 1, tzinfo=UTC),
    )
    _mock_replay_evidence(monkeypatch)
    record_paper_candidate_activation(
        path,
        activation_receipt_sha256="d" * 64,
        model_version="candidate-1",
        artifact_sha256="a" * 64,
        candidate_package_sha256="c" * 64,
        activated_at_utc=datetime(2026, 8, 25, 19, 44, tzinfo=UTC),
    )
    arguments = dict(
        model_version="candidate-1", artifact_sha256="a" * 64,
        source_sha256="b" * 64, session_id="s1", family_root="SPX",
        trading_day=date(2026, 8, 25), decision_horizon_minutes=15,
        feature_available_at_utc=available,
        reference_price=100.0, candidate_predicted_log_return=0.0,
        incumbent_predicted_close=102.0,
        model_features=_model_features("SPX"),
        live_prefix_receipt=_live_prefix_receipt(
            tmp_path, prefix_sha256="b" * 64
        ),
    )
    record_paper_forecast(path, **arguments)
    arguments["candidate_predicted_log_return"] = 0.01

    with pytest.raises(ValueError, match="conflicting paper forecast"):
        record_paper_forecast(path, **arguments)
    arguments["candidate_predicted_log_return"] = 0.0
    arguments["model_features"] = dict(arguments["model_features"])
    arguments["model_features"]["observed_log1p_volume"] = 1.0

    with pytest.raises(ValueError, match="conflicting paper model feature payload"):
        record_paper_forecast(path, **arguments)


def test_paper_evaluation_recomputes_and_rejects_caller_prediction(
    tmp_path, monkeypatch
):
    path = tmp_path / "market.db"
    _close_table(path)
    trading_day = date(2026, 8, 25)
    available = datetime(2026, 8, 25, 19, 45, tzinfo=UTC)
    vault = tmp_path / "verified_close_sources"
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 45, 1, tzinfo=UTC),
    )
    _mock_replay_evidence(monkeypatch)
    candidate = _candidate_identity(tmp_path)
    record_paper_candidate_activation(
        path,
        activation_receipt_sha256=candidate["activation_sha256"],
        model_version="candidate-1",
        artifact_sha256=candidate["artifact_sha256"],
        candidate_package_sha256=candidate["package_sha256"],
        activated_at_utc=candidate["activated_at_utc"],
    )
    prefix_hash = "f" * 64
    prefix_receipt = _live_prefix_receipt(
        tmp_path, prefix_sha256=prefix_hash
    )
    for index, family in enumerate(PRODUCTION_FAMILIES):
        reference = 100.0 + index
        record_paper_forecast(
                path,
                model_version="candidate-1",
                artifact_sha256=candidate["artifact_sha256"],
                source_sha256=prefix_hash,
                session_id="s1",
                family_root=family,
                trading_day=trading_day,
                decision_horizon_minutes=15,
                feature_available_at_utc=available,
                reference_price=reference,
                candidate_predicted_log_return=(0.01 if family == "SPX" else 0.0),
                incumbent_predicted_close=reference + 2.0,
                model_features=_model_features(family),
                live_prefix_receipt=prefix_receipt,
        )
        close_artifact = f"official {family} close".encode()
        close_hash = hashlib.sha256(close_artifact).hexdigest()
        close_path = (
            vault / trading_day.isoformat() / family / f"{close_hash}.txt"
        )
        close_path.parent.mkdir(parents=True, exist_ok=True)
        close_path.write_bytes(close_artifact)
        source, source_reference = OFFICIAL_SOURCES[family]
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO eod_close_observations VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    index + 1,
                    family,
                    trading_day.isoformat(),
                    reference + 0.5,
                    source,
                    source_reference,
                    1,
                    "2026-08-25T21:01:00+00:00",
                    close_hash,
                    None,
                ),
            )

    with pytest.raises(ValueError, match="does not match replay"):
        evaluate_paper_forecasts(
            path,
            project_root=tmp_path,
            model_version="candidate-1",
            verified_artifact_root=vault,
        )


def test_paper_forecast_rejects_backfill_without_prior_activation(
    tmp_path, monkeypatch
):
    available = datetime(2026, 8, 25, 19, 45, tzinfo=UTC)
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 45, 1, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="immutable candidate activation"):
        record_paper_forecast(
            tmp_path / "market.db",
            model_version="candidate-1",
            artifact_sha256="a" * 64,
            source_sha256="b" * 64,
            session_id="s1",
            family_root="SPX",
            trading_day=date(2026, 8, 25),
            decision_horizon_minutes=15,
            feature_available_at_utc=available,
            reference_price=100.0,
            candidate_predicted_log_return=0.0,
            incumbent_predicted_close=102.0,
            model_features=_model_features("SPX"),
            live_prefix_receipt=_live_prefix_receipt(
                tmp_path, prefix_sha256="b" * 64
            ),
        )


def test_paper_campaign_binds_one_artifact_per_model_version(tmp_path, monkeypatch):
    path = tmp_path / "market.db"
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 44, 30, tzinfo=UTC),
    )
    record_paper_candidate_activation(
        path,
        activation_receipt_sha256="d" * 64,
        model_version="candidate-1",
        artifact_sha256="a" * 64,
        candidate_package_sha256="c" * 64,
        activated_at_utc=datetime(2026, 8, 25, 19, 44, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="conflicting paper candidate activation"):
        record_paper_candidate_activation(
            path,
            activation_receipt_sha256="e" * 64,
            model_version="candidate-1",
            artifact_sha256="b" * 64,
            candidate_package_sha256="f" * 64,
            activated_at_utc=datetime(2026, 8, 25, 19, 44, tzinfo=UTC),
        )


def test_paper_forecast_rejects_late_decision_horizon_write(tmp_path, monkeypatch):
    path = tmp_path / "market.db"
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 44, 30, tzinfo=UTC),
    )
    record_paper_candidate_activation(
        path,
        activation_receipt_sha256="d" * 64,
        model_version="candidate-1",
        artifact_sha256="a" * 64,
        candidate_package_sha256="c" * 64,
        activated_at_utc=datetime(2026, 8, 25, 19, 44, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "backend.closing_tape.paper._utc_now",
        lambda: datetime(2026, 8, 25, 19, 46, 31, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="bounded decision-horizon"):
        record_paper_forecast(
            path,
            model_version="candidate-1",
            artifact_sha256="a" * 64,
            source_sha256="b" * 64,
            session_id="s1",
            family_root="SPX",
            trading_day=date(2026, 8, 25),
            decision_horizon_minutes=15,
            feature_available_at_utc=datetime(
                2026, 8, 25, 19, 45, tzinfo=UTC
            ),
            reference_price=100.0,
            candidate_predicted_log_return=0.0,
            incumbent_predicted_close=102.0,
            model_features=_model_features("SPX"),
            live_prefix_receipt=_live_prefix_receipt(
                tmp_path, prefix_sha256="b" * 64
            ),
        )


def test_paper_candidate_chain_parses_each_hash_bound_snapshot_once(
    tmp_path, monkeypatch
):
    identity = _candidate_identity(tmp_path)
    activation_path = next(
        (tmp_path / "models" / "paper_activations").glob("*.json")
    ).resolve()
    activation = json.loads(activation_path.read_text(encoding="utf-8"))
    package_path = (
        tmp_path / "models" / str(activation["candidate_package_path"])
    ).resolve()
    package = json.loads(package_path.read_text(encoding="utf-8"))
    artifact_path = (tmp_path / "models" / str(package["artifact_path"])).resolve()
    targets = {activation_path, package_path, artifact_path}
    reads = {target: 0 for target in targets}
    real_read_bytes = Path.read_bytes

    def guarded_read_bytes(path):
        resolved = path.resolve()
        if resolved in reads:
            reads[resolved] += 1
            if reads[resolved] > 1:
                raise AssertionError(f"candidate evidence was reread: {resolved}")
        return real_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    resolved = paper_evidence.resolve_paper_candidate(
        tmp_path,
        activation_receipt_sha256=identity["activation_sha256"],
        candidate_package_sha256=identity["package_sha256"],
        model_version="candidate-1",
        artifact_sha256=identity["artifact_sha256"],
        activated_at_utc=identity["activated_at_utc"].isoformat(),
    )

    assert resolved.artifact_sha256 == identity["artifact_sha256"]
    assert set(reads.values()) == {1}
