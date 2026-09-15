import asyncio
from datetime import date, datetime, timedelta, timezone
import inspect
import sqlite3
import threading
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from backend.api.routers import closing_tape as router


PRODUCTION_FAMILIES = ("SPX", "NDX", "RUT", "VIX", "SPY")


def _governed_rows(trading_day: date) -> list[dict[str, object]]:
    return [
        {
            "family_root": family,
            "trading_date": trading_day.isoformat(),
            "prediction_key": f"{family}-governed-key",
            "forecast_id": f"{family}-governed-key",
            "validation_state": "VALID",
            "decision_grade": True,
            "deterministic_replay_verified": True,
            "replay_status": "DETERMINISTIC_REPLAY_VERIFIED",
        }
        for family in PRODUCTION_FAMILIES
    ]


def test_status_api_runs_blocking_catalog_work_off_the_event_loop(
    monkeypatch, tmp_path
):
    scan_started = threading.Event()
    release_scan = threading.Event()
    scan_thread_ids = []

    configured_database = tmp_path / "configured" / "market.db"

    def blocking_status(project_root, *, trading_day, market_db_path):
        scan_thread_ids.append(threading.get_ident())
        assert project_root == tmp_path
        assert trading_day == date(2026, 9, 3)
        assert market_db_path == configured_database
        scan_started.set()
        assert release_scan.wait(timeout=2.0)
        return {"state": "complete"}

    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        router,
        "_configured_api_market_database_path",
        lambda: configured_database,
    )
    monkeypatch.setattr(router, "closing_tape_status", blocking_status)

    app = FastAPI()
    app.include_router(router.router)

    @app.get("/event-loop-probe")
    async def event_loop_probe():
        return {"responsive": True}

    status_route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == "/closing-tape/status"
    )
    assert inspect.iscoroutinefunction(status_route.endpoint) is False

    async def exercise_routes():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            status_task = asyncio.create_task(
                client.get("/closing-tape/status?trading_date=2026-09-03")
            )
            try:
                assert await asyncio.to_thread(scan_started.wait, 1.0)
                probe = await asyncio.wait_for(
                    client.get("/event-loop-probe"), timeout=0.5
                )
                assert probe.status_code == 200
                assert probe.json() == {"responsive": True}
            finally:
                release_scan.set()
            status = await asyncio.wait_for(status_task, timeout=2.0)
            assert status.status_code == 200
            assert status.json() == {"state": "complete"}

    asyncio.run(exercise_routes())

    assert len(scan_thread_ids) == 1
    assert scan_thread_ids[0] != threading.get_ident()


def test_status_api_preserves_invalid_trading_date_error(monkeypatch):
    calls = []
    monkeypatch.setattr(
        router,
        "closing_tape_status",
        lambda *_args, **_kwargs: calls.append(True),
    )

    with pytest.raises(HTTPException) as exc_info:
        router.get_closing_tape_status(trading_date="09-03-2026")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "trading_date must use YYYY-MM-DD"
    assert calls == []


def test_readiness_snapshot_rejects_an_older_response_contract(monkeypatch, tmp_path):
    snapshot = tmp_path / "readiness.json"
    snapshot.write_text(
        '{"cache_contract_version":"closing-tape-readiness-cache.v5",'
        '"generated_at_utc":"2026-08-28T17:00:00+00:00"}',
        encoding="utf-8",
    )
    monkeypatch.setattr(router, "READINESS_SNAPSHOT_PATH", snapshot)

    assert router._load_readiness_snapshot() is None


def test_readiness_refresh_is_producer_owned_and_get_only_reads_retained_evidence(
    monkeypatch, tmp_path
):
    calls = []
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    resolution_id = router.catalog_configuration_resolution_id(tmp_path)
    generated_at = datetime.now(timezone.utc).isoformat()

    configured_database = tmp_path / "configured" / "market.db"

    def audit(_root, *, market_db_path):
        calls.append(True)
        assert market_db_path == configured_database
        return SimpleNamespace(
            to_dict=lambda: {
                "generated_at_utc": generated_at,
                "catalogs": 4,
                "catalog_resolution_id": resolution_id,
            }
        )

    router._reset_readiness_cache()
    monkeypatch.setattr(router, "audit_training_readiness", audit)
    monkeypatch.setattr(
        router,
        "_configured_api_market_database_path",
        lambda: configured_database,
    )
    monkeypatch.setattr(router.time, "monotonic", lambda: 100.0)
    monkeypatch.setattr(router, "READINESS_SNAPSHOT_PATH", tmp_path / "readiness.json")
    monkeypatch.setattr(router, "_spawn_readiness_refresh", router._refresh_training_readiness)

    assert router.request_training_readiness_refresh() is True
    snapshot_mtime = (tmp_path / "readiness.json").stat().st_mtime_ns
    payload = router.get_closing_tape_readiness()

    assert len(calls) == 1
    assert payload["generated_at_utc"] == generated_at
    assert payload["readiness_cache_state"] == "fresh"
    assert payload["cache_ttl_seconds"] == 300.0
    assert payload["refresh_in_progress"] is False
    assert (tmp_path / "readiness.json").is_file()
    assert (tmp_path / "readiness.json").stat().st_mtime_ns == snapshot_mtime
    router._reset_readiness_cache()


def test_readiness_get_does_not_start_an_audit_or_persist(monkeypatch, tmp_path):
    snapshot = tmp_path / "readiness.json"
    router._reset_readiness_cache()
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(router, "READINESS_SNAPSHOT_PATH", snapshot)
    monkeypatch.setattr(
        router,
        "audit_training_readiness",
        lambda _root: (_ for _ in ()).throw(AssertionError("GET started an audit")),
    )
    monkeypatch.setattr(
        router,
        "_spawn_readiness_refresh",
        lambda: (_ for _ in ()).throw(AssertionError("GET spawned a refresh")),
    )

    payload = router.get_closing_tape_readiness()

    assert payload["readiness_cache_state"] == "unavailable"
    assert payload["refresh_in_progress"] is False
    assert not snapshot.exists()
    router._reset_readiness_cache()


def test_readiness_producer_rejects_future_generated_evidence(monkeypatch, tmp_path):
    snapshot = tmp_path / "readiness.json"
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    resolution_id = router.catalog_configuration_resolution_id(tmp_path)
    router._reset_readiness_cache()
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(router, "READINESS_SNAPSHOT_PATH", snapshot)
    monkeypatch.setattr(
        router,
        "_configured_api_market_database_path",
        lambda: tmp_path / "configured" / "market.db",
    )
    monkeypatch.setattr(
        router,
        "audit_training_readiness",
        lambda *_args, **_kwargs: SimpleNamespace(
            to_dict=lambda: {
                "generated_at_utc": future,
                "catalog_resolution_id": resolution_id,
            }
        ),
    )
    monkeypatch.setattr(router, "_spawn_readiness_refresh", router._refresh_training_readiness)

    assert router.request_training_readiness_refresh() is True
    payload = router.get_closing_tape_readiness()

    assert payload["readiness_cache_state"] == "unavailable"
    assert payload["refresh_error_type"] == "ValueError"
    assert not snapshot.exists()
    router._reset_readiness_cache()


def test_readiness_snapshot_rejects_catalog_configuration_mismatch(
    monkeypatch,
    tmp_path,
):
    snapshot = tmp_path / "readiness.json"
    snapshot.write_text(
        '{"cache_contract_version":"closing-tape-readiness-cache.v6",'
        '"generated_at_utc":"2026-08-28T17:00:00+00:00",'
        '"catalog_resolution_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}',
        encoding="utf-8",
    )
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(router, "READINESS_SNAPSHOT_PATH", snapshot)

    assert router._load_readiness_snapshot() is None


def test_refresh_error_disables_cached_ready_gates(monkeypatch, tmp_path):
    router._reset_readiness_cache()
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    resolution_id = router.catalog_configuration_resolution_id(tmp_path)
    monkeypatch.setattr(
        router,
        "_readiness_cache_payload",
        {
            "generated_at_utc": "2026-08-28T17:00:00+00:00",
            "catalog_resolution_id": resolution_id,
            "capture_gate_ready": True,
            "model_training_ready": True,
            "paper_evidence_ready": True,
        },
    )
    monkeypatch.setattr(router, "_readiness_cache_created_monotonic", 99.0)
    monkeypatch.setattr(router, "_readiness_refresh_error", "RuntimeError")
    monkeypatch.setattr(router.time, "monotonic", lambda: 100.0)

    payload = router._cached_training_readiness()

    assert payload["readiness_cache_state"] == "stale_error"
    assert payload["capture_gate_ready"] is False
    assert payload["model_training_ready"] is False
    assert payload["paper_evidence_ready"] is False
    assert "disabled" in payload["unavailable_reason"]
    router._reset_readiness_cache()


def test_stale_cached_readiness_disables_ready_gates(monkeypatch, tmp_path):
    router._reset_readiness_cache()
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    resolution_id = router.catalog_configuration_resolution_id(tmp_path)
    monkeypatch.setattr(
        router,
        "_readiness_cache_payload",
        {
            "generated_at_utc": "2020-08-28T17:00:00+00:00",
            "catalog_resolution_id": resolution_id,
            "capture_gate_ready": True,
            "model_training_ready": True,
            "paper_evidence_ready": True,
        },
    )
    monkeypatch.setattr(router, "_readiness_cache_created_monotonic", 1.0)
    monkeypatch.setattr(router.time, "monotonic", lambda: 1000.0)
    payload = router._cached_training_readiness()

    assert payload["readiness_cache_state"] == "stale"
    assert payload["capture_gate_ready"] is False
    assert payload["model_training_ready"] is False
    assert payload["paper_evidence_ready"] is False
    assert "disabled" in payload["unavailable_reason"]
    router._reset_readiness_cache()


def test_production_prediction_api_marks_incompatible_evidence_unavailable(monkeypatch):
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))
    monkeypatch.setattr(
        router, "load_governed_promoted_close_predictions",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("immutable ledger schema is incompatible")
        ),
    )

    payload = asyncio.run(router.get_promoted_closing_tape_predictions(trading_date=None))

    assert payload["available"] is False
    assert payload["rows"] == []
    assert payload["is_estimate"] is False
    assert payload["prediction_authority"]["authority_state"] == "unavailable"
    assert payload["prediction_authority"]["tcbbo_promoted"] is False
    assert payload["requested_trading_date"] == "2026-09-03"
    assert payload["current_trading_date"] == "2026-09-03"
    assert payload["is_current_session"] is True
    assert "schema is incompatible" in payload["reason"]


def test_production_prediction_api_rejects_when_approval_cannot_be_verified(
    monkeypatch,
):
    calls = []
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))
    monkeypatch.setattr(
        router,
        "load_governed_promoted_close_predictions",
        lambda *_args, **_kwargs: (
            calls.append(True),
            (_ for _ in ()).throw(RuntimeError("approval receipt is unavailable")),
        )[1],
    )

    payload = asyncio.run(
        router.get_promoted_closing_tape_predictions(trading_date=None)
    )

    assert payload["available"] is False
    assert payload["prediction_authority"]["promotion_verified"] is False
    assert payload["rows"] == []
    assert "approval receipt is unavailable" in payload["reason"]
    assert calls == [True]


def test_production_prediction_api_marks_successful_empty_read_unavailable(monkeypatch):
    captured = {}
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))

    def load(_path, *, project_root, trading_day):
        captured["project_root"] = project_root
        captured["trading_day"] = trading_day
        return []

    monkeypatch.setattr(
        router, "load_governed_promoted_close_predictions", load
    )

    payload = asyncio.run(router.get_promoted_closing_tape_predictions(trading_date=None))

    assert payload["available"] is False
    assert payload["read_succeeded"] is True
    assert payload["rows"] == []
    assert payload["is_estimate"] is False
    assert payload["reason"] == "no promoted TCBBO estimate is available"
    assert payload["prediction_authority"]["tcbbo_promoted"] is False
    assert payload["requested_trading_date"] == "2026-09-03"
    assert payload["current_trading_date"] == "2026-09-03"
    assert payload["is_current_session"] is True
    assert captured["trading_day"] == date(2026, 9, 3)
    assert captured["project_root"] == router.PROJECT_ROOT


def test_production_prediction_api_promotes_only_a_validated_current_batch(monkeypatch):
    rows = _governed_rows(date(2026, 9, 3))
    captured = {}
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))

    def load(_path, *, project_root, trading_day):
        captured["trading_day"] = trading_day
        captured["project_root"] = project_root
        return rows

    monkeypatch.setattr(router, "load_governed_promoted_close_predictions", load)

    payload = asyncio.run(router.get_promoted_closing_tape_predictions(trading_date=None))

    assert captured == {
        "trading_day": date(2026, 9, 3),
        "project_root": router.PROJECT_ROOT,
    }
    assert payload["available"] is True
    assert payload["read_succeeded"] is True
    assert payload["prediction_authority"]["tcbbo_promoted"] is True
    assert payload["prediction_authority"]["promotion_verified"] is True
    assert payload["requested_trading_date"] == "2026-09-03"
    assert payload["current_trading_date"] == "2026-09-03"
    assert payload["is_current_session"] is True
    assert payload["rows"] == rows
    assert payload["deterministic_replay_verified"] is True
    assert payload["replay_status"] == "DETERMINISTIC_REPLAY_VERIFIED"
    assert all(row["decision_grade"] is True for row in payload["rows"])
    assert all(
        row["deterministic_replay_verified"] is True for row in payload["rows"]
    )


def test_production_prediction_api_exposes_explicit_historical_context(monkeypatch):
    rows = _governed_rows(date(2026, 9, 2))
    captured = {}
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))

    def load(_path, *, project_root, trading_day):
        captured["trading_day"] = trading_day
        captured["project_root"] = project_root
        return rows

    monkeypatch.setattr(router, "load_governed_promoted_close_predictions", load)

    payload = asyncio.run(
        router.get_promoted_closing_tape_predictions(trading_date="2026-09-02")
    )

    assert captured["trading_day"] == date(2026, 9, 2)
    assert payload["available"] is True
    assert payload["prediction_authority"]["promotion_verified"] is True
    assert payload["requested_trading_date"] == "2026-09-02"
    assert payload["current_trading_date"] == "2026-09-03"
    assert payload["is_current_session"] is False


def test_production_prediction_api_rejects_a_partial_batch(monkeypatch):
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))
    monkeypatch.setattr(
        router,
        "load_governed_promoted_close_predictions",
        lambda *_args, **_kwargs: [
            _governed_rows(date(2026, 9, 3))[0]
        ],
    )

    payload = asyncio.run(router.get_promoted_closing_tape_predictions(trading_date=None))

    assert payload["available"] is False
    assert payload["read_succeeded"] is False
    assert payload["rows"] == []
    assert payload["is_estimate"] is False
    assert payload["prediction_authority"]["tcbbo_promoted"] is False
    assert "exactly five" in payload["reason"]


def test_production_prediction_api_rejects_identity_only_replay_evidence(monkeypatch):
    rows = _governed_rows(date(2026, 9, 3))
    rows[0] = {
        **rows[0],
        "deterministic_replay_verified": False,
        "replay_status": "IDENTITY_VERIFIED_NOT_REPLAYED",
    }
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))
    monkeypatch.setattr(
        router,
        "load_governed_promoted_close_predictions",
        lambda *_args, **_kwargs: rows,
    )

    payload = asyncio.run(
        router.get_promoted_closing_tape_predictions(trading_date=None)
    )

    assert payload["available"] is False
    assert payload["rows"] == []
    assert "lacks deterministic replay verification" in payload["reason"]


def test_production_prediction_api_rejects_a_valid_batch_for_the_wrong_date(monkeypatch):
    rows = _governed_rows(date(2026, 9, 2))
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))
    monkeypatch.setattr(
        router,
        "load_governed_promoted_close_predictions",
        lambda *_args, **_kwargs: rows,
    )

    payload = asyncio.run(router.get_promoted_closing_tape_predictions(trading_date=None))

    assert payload["available"] is False
    assert payload["rows"] == []
    assert payload["prediction_authority"]["promotion_verified"] is False
    assert "requested trading date" in payload["reason"]


def test_production_prediction_api_never_exposes_unregistered_raw_rows(
    monkeypatch, tmp_path
):
    database = tmp_path / "data" / "market_data.db"
    database.parent.mkdir(parents=True)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE promoted_close_predictions (
                prediction_key TEXT PRIMARY KEY,
                trading_date TEXT NOT NULL,
                family_root TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO promoted_close_predictions VALUES (?,?,?)",
            ("legacy-raw-row", "2026-09-03", "SPX"),
        )
    monkeypatch.setattr(router, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(router, "_configured_api_market_database_path", lambda: database)
    monkeypatch.setattr(router, "_current_trading_date", lambda: date(2026, 9, 3))

    payload = asyncio.run(
        router.get_promoted_closing_tape_predictions(trading_date=None)
    )

    assert payload["read_succeeded"] is True
    assert payload["available"] is False
    assert payload["rows"] == []
    assert payload["prediction_authority"]["decision_grade"] is False
    assert payload["deterministic_replay_verified"] is False


def test_scorecard_api_is_read_only_and_forwards_evidence_filters(monkeypatch):
    captured = {}
    configured_database = SimpleNamespace(name="configured-market-database")

    def scorecard(path, **filters):
        captured["path"] = path
        captured.update(filters)
        return {"available": False, "reason": "minimum evidence not met"}

    monkeypatch.setattr(router, "build_closing_tape_scorecard", scorecard)
    monkeypatch.setattr(
        router,
        "_configured_api_market_database_path",
        lambda: configured_database,
    )

    payload = asyncio.run(
        router.get_closing_tape_scorecard(
            model_version="candidate-1",
            family_root="SPX",
            decision_horizon_minutes=15,
            volatility_regime=None,
            minimum_scored_sessions=20,
        )
    )

    assert payload == {"available": False, "reason": "minimum evidence not met"}
    assert captured == {
        "path": configured_database,
        "project_root": router.PROJECT_ROOT,
        "model_version": "candidate-1",
        "family_root": "SPX",
        "decision_horizon_minutes": 15,
        "volatility_regime": None,
        "minimum_scored_sessions": 20,
    }


def test_scorecard_api_rejects_invalid_evidence_filter(monkeypatch):
    monkeypatch.setattr(
        router,
        "build_closing_tape_scorecard",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("unsupported production family: BAD")
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            router.get_closing_tape_scorecard(
                model_version=None,
                family_root="BAD",
                decision_horizon_minutes=15,
                volatility_regime=None,
                minimum_scored_sessions=5,
            )
        )

    assert exc_info.value.status_code == 400
    assert "unsupported production family" in exc_info.value.detail


def test_scorecard_api_schema_failure_has_complete_unavailable_shape(monkeypatch):
    monkeypatch.setattr(
        router,
        "build_closing_tape_scorecard",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("immutable score schema mismatch")
        ),
    )

    payload = asyncio.run(
        router.get_closing_tape_scorecard(
            model_version=None,
            family_root=None,
            decision_horizon_minutes=None,
            volatility_regime=None,
            minimum_scored_sessions=5,
        )
    )

    assert payload["available"] is False
    assert payload["overall"]["metrics_available"] is False
    assert payload["overall"]["candidate_mae"] is None
    assert payload["overall"]["interval_empirical_coverage"] is None
    assert payload["by_family"] == []
    assert payload["by_symbol"] == []
    assert payload["by_horizon"] == []
    assert payload["by_regime"] == []
    assert payload["by_model"] == []
    assert payload["by_slice"] == []
    assert payload["outcome_resolution"]["resolution_rate"] is None


def test_scorecard_api_retained_evidence_corruption_is_unavailable(monkeypatch):
    monkeypatch.setattr(
        router,
        "build_closing_tape_scorecard",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "closing-tape governance evidence is invalid: forecast event record hash"
            )
        ),
    )

    payload = asyncio.run(
        router.get_closing_tape_scorecard(
            model_version=None,
            family_root=None,
            decision_horizon_minutes=None,
            volatility_regime=None,
            minimum_scored_sessions=5,
        )
    )

    assert payload["available"] is False
    assert "record hash" in payload["reason"]
    assert payload["overall"]["metrics_available"] is False
