"""Pure contract for the backend source captured at opening-process startup.

The health endpoint snapshots these files once when its router is imported.
Opening acceptance independently compares that immutable snapshot with the
current on-disk files, so a prior-day or stale-code process cannot be promoted
merely because its data universe rolled forward.
"""

from __future__ import annotations


LOADED_CODE_FINGERPRINT_SCHEMA = "marketpin-loaded-code-fingerprint.v1"
LOADED_CODE_CAPTURE_SEMANTICS = "captured_once_at_health_router_import"
LOADED_CODE_HASH_ALGORITHM = "sha256"

# Keep this tuple deterministic and deliberately bounded to source and static
# configuration that own process startup, subscription/gamma calculation,
# opening-reference capture, persistence, calendar decisions, the dashboard
# projections used at the open, and the receipt owners used by acceptance.
#
# The health router captures the bytes of every path at backend import time.
# That is loaded-code evidence for backend modules, but only a startup source
# snapshot for launchers, the Streamlit application, and auxiliary processes.
# A new owned PID plus its exact endpoint/process postconditions is still
# required before those separately loaded components are called active.
OPENING_CRITICAL_SOURCE_PATHS = (
    "server.py",
    "market_app_supervisor.psm1",
    "market_calendar.psm1",
    "start_market_day.ps1",
    "ensure_market_app.ps1",
    "start_databento_app.ps1",
    "start_closing_tape.ps1",
    ".streamlit/config.toml",
    "app.py",
    "app/services/dashboard_status.py",
    "app/services/databento_symbol_catalog.py",
    "app/services/live_data_client.py",
    "app/services/live_panel_view.py",
    "app/services/orb_view.py",
    "app/services/retained_parity_view.py",
    "app/services/sidebar_live_state.py",
    "app/utils/market_time.py",
    "app/utils/opra_parity_history.py",
    "app/utils/snapshot_history.py",
    "app/utils/time_et.py",
    "app/utils/market_calendar.py",
    "config/us_cash_equity_calendar.json",
    "backend/ai_predictor.py",
    "backend/app.py",
    "backend/config.py",
    "backend/database.py",
    "backend/database_target.py",
    "backend/databento_streamer.py",
    "backend/closing_tape/catalog.py",
    "backend/closing_tape/config.py",
    "backend/closing_tape/live_recorder.py",
    "backend/closing_tape/start_gate.py",
    "backend/historical_context.py",
    "backend/inference.py",
    "backend/market_structure.py",
    "backend/monitor_completed_final_delivery.py",
    "backend/monitor_data_quality_outbox.py",
    "backend/monitor_notification_outbox.py",
    "backend/monitor_opening_acceptance.py",
    "backend/monitor_scan_ledger.py",
    "backend/monitor_session_rollover.py",
    "backend/optional_family_canary.py",
    "backend/prediction_authority.py",
    "backend/prediction_passport.py",
    "backend/research/live_shadow.py",
    "backend/research/shadow_formula.py",
    "backend/runtime_controls.py",
    "backend/snapshot_export.py",
    "backend/streamer.py",
    "backend/underlying_validator.py",
    "backend/workstation.py",
    "backend/api/helpers.py",
    "backend/api/bounded_runtime_read.py",
    "backend/api/lifecycle.py",
    "backend/api/schemas.py",
    "backend/api/routers/gex.py",
    "backend/api/routers/health.py",
    "backend/api/routers/orb.py",
    "backend/api/routers/predict.py",
    "backend/opening_code_fingerprint.py",
    "tools/ack_market_monitor_opening_acceptance.py",
    "tools/build_market_monitor_data_quality_ack_request.py",
    "tools/commit_market_monitor_opening_acceptance.py",
    "tools/inspect_opening_capture.py",
    "tools/preflight_opening_acceptance.py",
    "tools/prepare_databento_universe_cache.py",
    "tools/prepare_market_monitor_session.py",
)


__all__ = [
    "LOADED_CODE_CAPTURE_SEMANTICS",
    "LOADED_CODE_FINGERPRINT_SCHEMA",
    "LOADED_CODE_HASH_ALGORITHM",
    "OPENING_CRITICAL_SOURCE_PATHS",
]
