from datetime import date, datetime, timezone
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from backend.postclose_scoring import (
    load_prediction_score_selection,
    prepare_prediction_score_selection,
)
from backend.closing_tape.config import official_close_publication_cutoff_utc
from tools.run_verified_close_postclose import latest_pipeline_status, run_settlement


UTC = timezone.utc


def _database(root: Path) -> Path:
    path = root / "data" / "market_data.db"
    path.parent.mkdir(parents=True)
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE prediction_snapshots (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            timestamp_utc TEXT,
            trading_date TEXT,
            is_valid INTEGER,
            current_price REAL,
            predicted_close REAL,
            prediction_mode TEXT,
            model_version TEXT,
            source_payload_json TEXT
        );
        CREATE TABLE gamma_calculation_runs (
            id INTEGER PRIMARY KEY,
            calculation_id TEXT,
            symbol TEXT,
            trading_date TEXT,
            status TEXT
        );
        CREATE TABLE gamma_calculation_input_blobs (
            calculation_run_id INTEGER PRIMARY KEY,
            payload BLOB
        );
        """
    )
    rows = [
        (1, "SPX", "2026-09-18 19:55:00", "calc-old", 7630.0),
        (2, "SPX", "2026-09-18 19:59:00", "calc-final", 7638.0),
        (3, "SPX", "2026-09-18 20:01:00", "calc-late", 7640.0),
        (4, "NDX", "2026-09-18 19:59:30", "calc-ndx", 29550.0),
    ]
    for prediction_id, symbol, timestamp, calculation_id, predicted in rows:
        connection.execute(
            "INSERT INTO prediction_snapshots VALUES (?,?,?,?,1,?,?,?,?,?)",
            (
                prediction_id,
                symbol,
                timestamp,
                "2026-09-18",
                predicted - 5,
                predicted,
                "backend_periodic",
                "model-v1",
                json.dumps({"calculation_id": calculation_id}),
            ),
        )
        connection.execute(
            "INSERT INTO gamma_calculation_runs VALUES (?,?,?,?,?)",
            (prediction_id, calculation_id, symbol, "2026-09-18", "valid"),
        )
        connection.execute(
            "INSERT INTO gamma_calculation_input_blobs VALUES (?,?)",
            (prediction_id, b"evidence"),
        )
    connection.commit()
    connection.close()
    return path


def test_selection_freezes_latest_eligible_preclose_rows(monkeypatch, tmp_path):
    database = _database(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database.as_posix()}")
    observed = datetime(2026, 9, 18, 21, 0, tzinfo=UTC)

    result = prepare_prediction_score_selection(
        tmp_path,
        date(2026, 9, 18),
        observed_at_utc=observed,
    )

    selected = {
        row["symbol"]: row["prediction_id"] for row in result["receipt"]["selections"]
    }
    assert selected == {"SPX": 2, "NDX": 4}
    statuses = {row["symbol"]: row["status"] for row in result["receipt"]["statuses"]}
    assert statuses == {
        "SPX": "SELECTED",
        "NDX": "SELECTED",
        "RUT": "NO_ELIGIBLE_PREDICTION",
        "DJI": "UNSUPPORTED_VERIFIED_CLOSE_SCORING",
    }
    assert Path(result["receipt_path"]).stem == result["receipt_sha256"]
    assert load_prediction_score_selection(tmp_path, date(2026, 9, 18)) == result

    repeated = prepare_prediction_score_selection(
        tmp_path,
        date(2026, 9, 18),
        observed_at_utc=observed.replace(minute=1),
    )
    assert repeated == result

    post_cutoff = prepare_prediction_score_selection(
        tmp_path,
        date(2026, 9, 18),
        observed_at_utc=datetime(2026, 9, 18, 23, 0, tzinfo=UTC),
    )
    assert post_cutoff == result


def test_selection_refuses_preclose_creation(monkeypatch, tmp_path):
    database = _database(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database.as_posix()}")

    with pytest.raises(RuntimeError, match="until the cash session closes"):
        prepare_prediction_score_selection(
            tmp_path,
            date(2026, 9, 18),
            observed_at_utc=datetime(2026, 9, 18, 19, 59, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    "observed",
    (
        datetime(2026, 9, 18, 22, 0, tzinfo=UTC),
        datetime(2026, 9, 18, 23, 0, tzinfo=UTC),
    ),
)
def test_selection_refuses_new_receipt_at_or_after_publication_cutoff(
    monkeypatch, tmp_path, observed
):
    database = _database(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database.as_posix()}")

    with pytest.raises(RuntimeError, match="official publication cutoff"):
        prepare_prediction_score_selection(
            tmp_path,
            date(2026, 9, 18),
            observed_at_utc=observed,
        )

    assert not (tmp_path / "data" / "prediction_score_selections").exists()


def test_official_publication_cutoff_tracks_eastern_dst():
    assert official_close_publication_cutoff_utc(date(2026, 10, 26)) == datetime(
        2026, 10, 26, 22, 0, tzinfo=UTC
    )
    assert official_close_publication_cutoff_utc(date(2026, 11, 9)) == datetime(
        2026, 11, 9, 23, 0, tzinfo=UTC
    )


def test_selection_rejects_existing_receipt_created_after_cutoff(monkeypatch, tmp_path):
    database = _database(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database.as_posix()}")
    receipt = {
        "schema_version": "marketpin-prediction-score-selection.v1",
        "trading_date": "2026-09-18",
        "selected_at_utc": "2026-09-18T22:00:00+00:00",
    }
    rendered = (
        json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")
    digest = hashlib.sha256(rendered).hexdigest()
    directory = tmp_path / "data" / "prediction_score_selections" / "2026-09-18"
    directory.mkdir(parents=True)
    (directory / f"{digest}.json").write_bytes(rendered)

    with pytest.raises(RuntimeError, match="approved window"):
        prepare_prediction_score_selection(
            tmp_path,
            date(2026, 9, 18),
            observed_at_utc=datetime(2026, 9, 18, 23, 0, tzinfo=UTC),
        )


def test_selection_refuses_noncanonical_database(monkeypatch, tmp_path):
    _database(tmp_path)
    other = tmp_path / "market.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{other.as_posix()}")

    with pytest.raises(RuntimeError, match="does not select the canonical"):
        prepare_prediction_score_selection(
            tmp_path,
            date(2026, 9, 18),
            observed_at_utc=datetime(2026, 9, 18, 21, 0, tzinfo=UTC),
        )


def test_settlement_binds_scoring_to_selection_receipt(monkeypatch, tmp_path):
    database = _database(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database.as_posix()}")
    selection = prepare_prediction_score_selection(
        tmp_path,
        date(2026, 9, 18),
        observed_at_utc=datetime(2026, 9, 18, 21, 0, tzinfo=UTC),
    )
    candidate = tmp_path / "candidate_bundle.csv"
    captured = {}

    def fake_fetch(**_kwargs):
        return {
            "candidate_csv": str(candidate),
            "audit": {"ready_for_ingestion": True, "bundle_status": "READY"},
        }

    def fake_ingest(_rows, **kwargs):
        captured.update(kwargs)
        return [
            {
                "symbol": "SPX",
                "scored": 1,
                "score": {"scored": 1},
                "official_close": 7640.0,
            }
        ]

    monkeypatch.setattr(
        "tools.fetch_verified_close_bundle.fetch_verified_close_bundle", fake_fetch
    )
    monkeypatch.setattr(
        "tools.ingest_verified_closes.read_verified_close_csv", lambda _path: [object()]
    )
    monkeypatch.setattr("tools.ingest_verified_closes.ingest", fake_ingest)

    result = run_settlement(
        tmp_path,
        date(2026, 9, 18),
        observed_at_utc=datetime(2026, 9, 18, 22, 5, tzinfo=UTC),
    )

    assert captured["bundle_profile"] == "full"
    assert captured["score_prediction_ids_by_symbol"] == {"SPX": [2], "NDX": [4]}
    assert captured["selection_evidence_sha256_by_symbol"] == {
        "SPX": selection["receipt_sha256"],
        "NDX": selection["receipt_sha256"],
    }
    assert Path(result["result_path"]).is_file()
    status = latest_pipeline_status(tmp_path, date(2026, 9, 18))
    assert status["selection"]["status"] == "RECEIPT_VALID"
    assert status["settlement"]["status"] == "COMPLETE"
