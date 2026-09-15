import asyncio
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.api.routers import health as health_router
from backend.monitor_opening_acceptance import (
    MonitorOpeningAcceptanceError,
    _validate_loaded_code_fingerprint,
)
from backend.opening_code_fingerprint import (
    LOADED_CODE_CAPTURE_SEMANTICS,
    LOADED_CODE_FINGERPRINT_SCHEMA,
    LOADED_CODE_HASH_ALGORITHM,
    OPENING_CRITICAL_SOURCE_PATHS,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRANSITIVE_CALENDAR_DEPENDENCIES = {
    "app/utils/market_time.py",
    "app/utils/time_et.py",
    "app/utils/market_calendar.py",
    "config/us_cash_equity_calendar.json",
}
ACTIVE_LIFECYCLE_DEPENDENCIES = {
    "backend/ai_predictor.py",
    "backend/historical_context.py",
    "backend/inference.py",
    "backend/prediction_passport.py",
    "backend/research/live_shadow.py",
    "backend/research/shadow_formula.py",
}
OPENING_PROJECTION_DEPENDENCIES = {
    "backend/prediction_authority.py",
    "backend/api/schemas.py",
}
OPENING_LAUNCHER_OWNERS = {
    "market_app_supervisor.psm1",
    "market_calendar.psm1",
    "start_market_day.ps1",
    "ensure_market_app.ps1",
    "start_databento_app.ps1",
    "start_closing_tape.ps1",
    "tools/prepare_databento_universe_cache.py",
}
DASHBOARD_STARTUP_SOURCE_DEPENDENCIES = {
    ".streamlit/config.toml",
    "app.py",
    "app/services/dashboard_status.py",
    "app/services/databento_symbol_catalog.py",
    "app/services/live_data_client.py",
    "app/services/live_panel_view.py",
    "app/services/orb_view.py",
    "app/services/retained_parity_view.py",
    "app/services/sidebar_live_state.py",
    "app/utils/opra_parity_history.py",
    "app/utils/snapshot_history.py",
}
CLOSING_TAPE_STARTUP_DEPENDENCIES = {
    "backend/closing_tape/catalog.py",
    "backend/closing_tape/config.py",
    "backend/closing_tape/live_recorder.py",
    "backend/closing_tape/start_gate.py",
}
OPENING_ACCEPTANCE_OWNER_DEPENDENCIES = {
    "backend/monitor_data_quality_outbox.py",
    "backend/monitor_notification_outbox.py",
    "backend/monitor_opening_acceptance.py",
    "backend/monitor_scan_ledger.py",
    "backend/monitor_session_rollover.py",
    "tools/ack_market_monitor_opening_acceptance.py",
    "tools/commit_market_monitor_opening_acceptance.py",
    "tools/inspect_opening_capture.py",
    "tools/preflight_opening_acceptance.py",
    "tools/prepare_market_monitor_session.py",
}
EXPANDED_OPENING_DEPENDENCIES = {
    *ACTIVE_LIFECYCLE_DEPENDENCIES,
    *CLOSING_TAPE_STARTUP_DEPENDENCIES,
    *DASHBOARD_STARTUP_SOURCE_DEPENDENCIES,
    *OPENING_ACCEPTANCE_OWNER_DEPENDENCIES,
    *OPENING_PROJECTION_DEPENDENCIES,
    *OPENING_LAUNCHER_OWNERS,
    *TRANSITIVE_CALENDAR_DEPENDENCIES,
}


class _FingerprintHealthStreamer:
    is_running = False

    @staticmethod
    def get_all_latest():
        return {}


def test_loaded_code_capture_is_stable_after_source_files_change(tmp_path):
    relative_paths = (
        "backend/databento_streamer.py",
        "backend/underlying_validator.py",
        "config/us_cash_equity_calendar.json",
    )
    original_bytes = {
        relative_paths[0]: b"streamer-at-process-start\n",
        relative_paths[1]: b"validator-at-process-start\n",
        relative_paths[2]: b'{"calendar":"at-process-start"}\n',
    }
    for relative_path, content in original_bytes.items():
        source = tmp_path / relative_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(content)

    captured = health_router._capture_loaded_code_sha256(tmp_path, relative_paths)
    expected = tuple(
        (relative_path, hashlib.sha256(original_bytes[relative_path]).hexdigest())
        for relative_path in relative_paths
    )

    for relative_path in relative_paths:
        (tmp_path / relative_path).write_bytes(b"changed-after-process-start\n")

    assert captured == expected
    assert captured != health_router._capture_loaded_code_sha256(
        tmp_path,
        relative_paths,
    )


def test_health_exposes_cached_canonical_loaded_code_hashes_without_disk_reads(
    monkeypatch,
):
    streamer = _FingerprintHealthStreamer()
    monkeypatch.setattr(health_router, "get_streamer", lambda: streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "provider": "databento",
            "stream_progressing": False,
            "websocket": "inactive",
            "messages_received": 0,
            "symbols_requested": [],
            "connection_limit_rejections_total": 0,
            "connection_limit_consecutive": 0,
            "connection_limit_circuit_state": "closed",
            "connection_limit_retry_not_before_utc": None,
            "connection_limit_cooldown_remaining_seconds": 0.0,
            "last_client_close_status": "not_attempted",
            "last_client_close_elapsed_seconds": None,
            "pre_auth_transport_guard_status": "installed",
            "pre_auth_transport_aborts_total": 0,
            "pre_auth_transport_abort_failures_total": 0,
            "last_pre_auth_transport_event": None,
            "last_pre_auth_transport_reason": None,
            "last_pre_auth_transport_event_utc": None,
        },
    )
    monkeypatch.setattr(
        health_router,
        "streamer_runtime_context",
        lambda _streamer: {
            "subscription_epoch_id": None,
            "subscription_generation": None,
            "handoff_status": "inactive",
        },
    )
    monkeypatch.setattr(health_router, "inference_device", lambda: "cpu")
    monkeypatch.setattr(health_router, "runtime_control_health", lambda: {})

    def fail_if_source_is_reread(_path):
        raise AssertionError("/health must not reread source files")

    monkeypatch.setattr(health_router.Path, "read_bytes", fail_if_source_is_reread)

    payload = asyncio.run(health_router.health_status())
    fingerprint = payload["loaded_code_fingerprint"]

    assert payload["pre_auth_transport_guard_status"] == "installed"
    assert payload["connection_limit_circuit_state"] == "closed"
    assert payload["connection_limit_rejections_total"] == 0
    assert payload["last_client_close_status"] == "not_attempted"

    assert fingerprint["schema_version"] == "marketpin-loaded-code-fingerprint.v1"
    assert fingerprint["capture_semantics"] == "captured_once_at_health_router_import"
    assert fingerprint["current_on_disk_recomputed"] is False
    assert (
        fingerprint["current_on_disk_comparison"]
        == "not_performed_by_health_endpoint"
    )
    assert tuple(fingerprint["files"]) == OPENING_CRITICAL_SOURCE_PATHS
    assert {
        "backend/api/routers/health.py",
        "backend/api/routers/orb.py",
        "backend/api/lifecycle.py",
        "backend/databento_streamer.py",
        "backend/market_structure.py",
        "backend/underlying_validator.py",
        *ACTIVE_LIFECYCLE_DEPENDENCIES,
        *CLOSING_TAPE_STARTUP_DEPENDENCIES,
        *DASHBOARD_STARTUP_SOURCE_DEPENDENCIES,
        *OPENING_ACCEPTANCE_OWNER_DEPENDENCIES,
        *OPENING_PROJECTION_DEPENDENCIES,
        *OPENING_LAUNCHER_OWNERS,
        *TRANSITIVE_CALENDAR_DEPENDENCIES,
    }.issubset(fingerprint["files"])
    for source in fingerprint["files"].values():
        assert re.fullmatch(r"[0-9a-f]{64}", source["loaded_at_startup_sha256"])


@pytest.mark.parametrize("omitted_path", sorted(EXPANDED_OPENING_DEPENDENCIES))
def test_opening_validator_rejects_omitted_expanded_opening_dependency(
    omitted_path,
):
    captured_at = "2026-09-08T13:00:00+00:00"
    fingerprint = {
        "schema_version": LOADED_CODE_FINGERPRINT_SCHEMA,
        "capture_semantics": LOADED_CODE_CAPTURE_SEMANTICS,
        "captured_at_utc": captured_at,
        "hash_algorithm": LOADED_CODE_HASH_ALGORITHM,
        "current_on_disk_recomputed": False,
        "current_on_disk_comparison": "not_performed_by_health_endpoint",
        "files": {
            relative_path: {
                "loaded_at_startup_sha256": hashlib.sha256(
                    (PROJECT_ROOT / relative_path).read_bytes()
                ).hexdigest()
            }
            for relative_path in OPENING_CRITICAL_SOURCE_PATHS
            if relative_path != omitted_path
        },
    }

    with pytest.raises(
        MonitorOpeningAcceptanceError,
        match="loaded_code_fingerprint_source_set_invalid",
    ):
        _validate_loaded_code_fingerprint(
            {"loaded_code_fingerprint": fingerprint},
            observed_utc=datetime(2026, 9, 8, 13, 0, tzinfo=timezone.utc),
            session_date="2026-09-08",
            project_root=PROJECT_ROOT,
        )
