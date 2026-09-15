import hashlib
import sqlite3
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.closing_tape import live_shadow
from backend.closing_tape.live_shadow import (
    _live_open_interest,
    _validate_live_prefix_integrity,
    copy_verified_prefix,
)


UTC = timezone.utc


def _integrity(**overrides):
    values = dict(
        sha256="a" * 64, local_file_intact=True, tcbbo_records=100,
        tcbbo_timestamped_records=100, tcbbo_valid_nbbo_records=95,
        provider_errors=(), slow_reader_warnings=0,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_live_prefix_integrity_accepts_research_grade_tcbbo():
    _validate_live_prefix_integrity(_integrity(), expected_sha256="a" * 64)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"sha256": "b" * 64}, "SHA-256"),
        ({"tcbbo_valid_nbbo_records": 94}, "below 95%"),
        ({"provider_errors": ("provider failure",)}, "provider errors"),
        ({"slow_reader_warnings": 1}, "slow-reader"),
    ],
)
def test_live_prefix_integrity_rejects_untrustworthy_evidence(overrides, message):
    with pytest.raises(ValueError, match=message):
        _validate_live_prefix_integrity(_integrity(**overrides), expected_sha256="a" * 64)


def test_prefix_copy_stops_at_exact_cutoff_and_verifies_hash(tmp_path):
    source = tmp_path / "live.dbn"
    source.write_bytes(b"immutable-prefix" + b"later-records")
    prefix = b"immutable-prefix"
    destination = tmp_path / "prefix.dbn"

    copy_verified_prefix(
        source, destination, cutoff_bytes=len(prefix),
        expected_sha256=hashlib.sha256(prefix).hexdigest(),
    )

    assert destination.read_bytes() == prefix


def test_prefix_copy_removes_mismatched_evidence(tmp_path):
    source = tmp_path / "live.dbn"
    source.write_bytes(b"prefix")
    destination = tmp_path / "prefix.dbn"

    with pytest.raises(ValueError, match="SHA-256"):
        copy_verified_prefix(
            source, destination, cutoff_bytes=6, expected_sha256="0" * 64
        )
    assert not destination.exists()


def test_live_open_interest_filters_by_receive_time_and_fails_closed_for_legacy(tmp_path):
    catalog = tmp_path / "catalog.sqlite"
    with sqlite3.connect(catalog) as connection:
        connection.execute(
            """
            CREATE TABLE tape_open_interest (
                session_id TEXT, feed_name TEXT, instrument_id INTEGER,
                raw_symbol TEXT, family_root TEXT, asof_utc TEXT,
                available_at_utc TEXT, open_interest REAL
            )
            """
        )
        connection.executemany(
            "INSERT INTO tape_open_interest VALUES (?,?,?,?,?,?,?,?)",
            [
                (
                    "s1", "opra_options", 1, "SPX EARLY", "SPX",
                    "2026-08-25T00:00:00+00:00",
                    "2026-08-25T15:00:30+00:00", 10.0,
                ),
                (
                    "s1", "opra_options", 2, "SPX FUTURE", "SPX",
                    "2026-08-25T00:00:00+00:00",
                    "2026-08-25T15:01:30+00:00", 99.0,
                ),
                (
                    "s1", "opra_options", 3, "SPX LEGACY", "SPX",
                    "2026-08-25T00:00:00+00:00", None, 777.0,
                ),
            ],
        )

    result = _live_open_interest(
        catalog, "s1", "opra_options",
        available_before_utc=datetime(2026, 8, 25, 15, 1, tzinfo=UTC),
    )

    assert result["raw_symbol"].tolist() == ["SPX EARLY"]
    assert result.iloc[0]["open_interest"] == 10.0


def test_live_prefix_runs_promoted_output_without_paper_candidate(tmp_path, monkeypatch):
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "closing_tape_model.json").write_text("{}", encoding="utf-8")
    source = tmp_path / "live.dbn"
    source.write_bytes(b"prefix")
    predictions = tuple(SimpleNamespace(family_root=value) for value in ("SPX", "NDX", "RUT", "VIX", "SPY"))
    events = []

    def copy_prefix(_source, destination, **_kwargs):
        events.append("copy")
        destination.write_bytes(b"prefix")
        return destination

    def start_attempt(*_args, **_kwargs):
        events.append("start")
        return "attempt-1"

    def finish_attempt(_path, attempt_key, **kwargs):
        events.append("finish")
        assert attempt_key == "attempt-1"
        assert kwargs["event_type"] == "PREDICTED"
        assert kwargs["prediction_batch"] is predictions
        assert len(kwargs["prediction_keys"]) == 5
        return "terminal-1"

    monkeypatch.setattr(live_shadow, "copy_verified_prefix", copy_prefix)
    monkeypatch.setattr(live_shadow, "start_forecast_attempt", start_attempt)
    monkeypatch.setattr(live_shadow, "finish_forecast_attempt", finish_attempt)
    monkeypatch.setattr(live_shadow, "build_surface_from_live_prefix", lambda *_args, **_kwargs: pd.DataFrame({"x": [1]}))
    monkeypatch.setattr(live_shadow, "load_promoted_model_runtime", lambda _root: object())
    monkeypatch.setattr(live_shadow, "predict_promoted_close", lambda _surface, _runtime: predictions)
    monkeypatch.setattr(
        live_shadow, "record_promoted_close_predictions",
        lambda *_args, **_kwargs: tuple(f"key-{item.family_root}" for item in predictions),
    )

    result = live_shadow.run_live_prefix_paper_shadow(
        project_root=tmp_path, source_path=source, cutoff_bytes=6,
        prefix_sha256=hashlib.sha256(b"prefix").hexdigest(),
        catalog_path=tmp_path / "catalog.db", market_db_path=tmp_path / "market.db",
        session_id="s1", feed_name="opra_options", trading_day=date(2026, 8, 25),
        cash_open_utc=datetime(2026, 8, 25, 13, 30, tzinfo=UTC),
        cash_close_utc=datetime(2026, 8, 25, 20, 0, tzinfo=UTC),
        feature_available_at_utc=datetime(2026, 8, 25, 19, 45, tzinfo=UTC),
    )

    assert not result.configured
    assert result.production_recorded == 5
    assert len(result.production_keys) == 5
    assert not result.production_reasons
    assert events == ["start", "copy", "finish"]


def test_live_paper_shadow_explicitly_derives_oi_from_copied_prefix(
    tmp_path, monkeypatch
):
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "closing_tape_paper_candidate.json").write_text(
        "{}", encoding="utf-8"
    )
    source = tmp_path / "live.dbn"
    source.write_bytes(b"prefix")
    captured = {}

    def copy_prefix(_source, destination, **_kwargs):
        destination.write_bytes(b"prefix")
        return destination

    def build(_prefix, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame({"x": [1]})

    monkeypatch.setattr(live_shadow, "copy_verified_prefix", copy_prefix)
    monkeypatch.setattr(live_shadow, "build_surface_from_live_prefix", build)
    monkeypatch.setattr(
        live_shadow,
        "record_paper_shadow_predictions",
        lambda *_args, **_kwargs: live_shadow.PaperShadowResult(
            True, 5, "candidate-1", "a" * 64, tuple(f"key-{i}" for i in range(5)), ()
        ),
    )

    result = live_shadow.run_live_prefix_paper_shadow(
        project_root=tmp_path,
        source_path=source,
        cutoff_bytes=6,
        prefix_sha256=hashlib.sha256(b"prefix").hexdigest(),
        catalog_path=tmp_path / "catalog.db",
        market_db_path=tmp_path / "market.db",
        session_id="s1",
        feed_name="opra_options",
        trading_day=date(2026, 8, 25),
        cash_open_utc=datetime(2026, 8, 25, 13, 30, tzinfo=UTC),
        cash_close_utc=datetime(2026, 8, 25, 20, 0, tzinfo=UTC),
        feature_available_at_utc=datetime(2026, 8, 25, 19, 45, tzinfo=UTC),
    )

    assert result.recorded == 5
    assert captured["derive_open_interest_from_prefix"] is True
