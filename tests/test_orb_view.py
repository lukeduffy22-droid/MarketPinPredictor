from pathlib import Path

import app.services.orb_view as orb_view
from app.services.orb_view import (
    fetch_opening_ranges,
    initialize_live_autorefresh,
    opening_range_rows,
)


EPOCH = "e" * 64


def test_live_autorefresh_is_disabled_for_new_and_existing_sessions():
    session_state = {}

    initialize_live_autorefresh(session_state)

    assert session_state == {
        "auto_refresh_live": False,
        "_live_autorefresh_initialized": True,
    }

    # Existing pages can retain the former default and initialization marker.
    session_state["auto_refresh_live"] = True
    initialize_live_autorefresh(session_state)

    assert session_state["auto_refresh_live"] is False


def test_dashboard_keeps_manual_refresh_without_a_background_timer():
    source = (Path(__file__).resolve().parents[1] / "app.py").read_text("utf-8")

    assert "render_opening_range_panel()" in source
    assert 'st.button("Refresh live status now"' in source
    assert "render_opening_range_live_fragment" not in source
    assert "run_every=" not in Path(orb_view.__file__).read_text("utf-8")
    assert "streamlit:rerun" not in source
    assert "streamlit.components.v1" not in source


def _live_envelope(symbols):
    normalized = {}
    for symbol, raw_state in symbols.items():
        state = dict(raw_state)
        state["schema_version"] = "marketpin-reference-orb.v2"
        reference = state.get("reference_semantics")
        reference = dict(reference) if isinstance(reference, dict) else {}
        reference.setdefault("kind", "same_day_index_option_parity")
        reference.setdefault("directional_base_eligible", True)
        state["reference_semantics"] = reference
        state.setdefault("current_reference_fresh", True)
        provenance = state.get("provenance")
        provenance = dict(provenance) if isinstance(provenance, dict) else {}
        provenance.update(
            {
                "runtime_binding_applied": True,
                "active_runtime_epoch_aligned": True,
                "active_subscription_epoch_id": EPOCH,
                "active_subscription_generation": 7,
                "active_handoff_status": "active",
            }
        )
        state["provenance"] = provenance
        normalized[symbol] = state
    return {
        "schema_version": "marketpin-reference-orb.collection.v2",
        "runtime_binding_applied": True,
        "runtime_context_stable": True,
        "active_runtime_context": {
            "subscription_epoch_id": EPOCH,
            "subscription_generation": 7,
            "handoff_status": "active",
        },
        "symbols": normalized,
    }


def test_opening_range_rows_preserves_unavailable_and_provenance_states():
    rows = opening_range_rows(
        _live_envelope(
            {
                "SPX": {
                    "capture_status": "forming",
                    "opening_price": 6500.0,
                    "orb_high": 6510.0,
                    "orb_low": 6490.0,
                    "current_price": 6505.0,
                    "breakout_direction": "forming",
                    "reference_semantics": {
                        "kind": "same_day_index_option_parity"
                    },
                    "directional_evidence_eligible": False,
                    "combined_structure_directional_evidence_eligible": False,
                    "opening_ranges": {
                        "5m": {
                            "capture_status": "complete",
                            "orb_low": 6495.0,
                            "orb_high": 6505.0,
                            "breakout_direction": "inside",
                            "directional_evidence_eligible": True,
                        },
                        "15m": {"capture_status": "forming"},
                        "60m": {"capture_status": "forming"},
                    },
                    "capture_evidence": {"sample_count": 12, "capture_ratio": 0.8},
                    "pin_behavior": {
                        "gamma_pin": 6520.0,
                        "gamma_pin_change_from_open": 20.0,
                        "max_pain": 6480.0,
                        "max_pain_change_from_open": 0.0,
                    },
                    "provenance": {
                        "subscription_generations": [7],
                        "current_vs_range_aligned": True,
                        "structure_reference_status": "aligned",
                        "latest_source_timestamp_utc": "2026-09-04T13:31:00Z",
                    },
                },
                "RUT": {
                    "capture_status": "not_configured",
                    "capture_evidence": {"sample_count": 0},
                    "pin_behavior": {},
                    "provenance": {},
                },
            }
        )
    )

    assert rows[0]["Symbol"] == "SPX"
    assert rows[0]["Coverage %"] == 80.0
    assert rows[0]["Provenance"] == "aligned"
    assert rows[0]["Reference"] == "same_day_index_option_parity"
    assert rows[0]["ORB eligible"] is False
    assert rows[0]["Pin alignment"] == "aligned"
    assert rows[0]["ORB + structure eligible"] is False
    assert rows[0]["Pin move from open"] == 20.0
    assert rows[0]["Generation"] == "7"
    assert rows[0]["Epoch"] == "eeeeeeee…eeeeeeee"
    assert rows[0]["Epoch full"] == EPOCH
    assert rows[0]["Epoch aligned"] is True
    assert rows[0]["Live contract"] == "v2 current"
    assert rows[0]["5m ORB"] == "6,495.00–6,505.00 | inside"
    assert rows[0]["15m ORB"] == "forming"
    assert rows[0]["30m ORB"] == "unavailable"
    assert rows[0]["60m ORB"] == "forming"
    assert rows[1]["Symbol"] == "RUT"
    assert rows[1]["Capture"] == "not_configured"
    assert rows[1]["Provenance"] == "pending"
    assert rows[1]["Pin alignment"] == "unavailable"
    assert rows[1]["ORB high"] is None


def test_fetch_opening_ranges_fails_closed_on_bad_backend_payload(monkeypatch):
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return ["not", "a", "mapping"]

    monkeypatch.setattr(
        "app.services.orb_view.requests.get",
        lambda *_args, **_kwargs: Response(),
    )
    assert fetch_opening_ranges() == {}


def test_fetch_opening_ranges_preserves_bounded_runtime_timeout_reason(monkeypatch):
    class Response:
        status_code = 503

        @staticmethod
        def json():
            return {"detail": {"reason": "RUNTIME_READ_TIMEOUT"}}

    monkeypatch.setattr(
        "app.services.orb_view.requests.get",
        lambda *_args, **_kwargs: Response(),
    )

    assert fetch_opening_ranges() == {
        "_marketpin_orb_fetch_failure": {
            "reason": "RUNTIME_READ_TIMEOUT",
            "status_code": 503,
        }
    }


def test_fetch_opening_ranges_sanitizes_untrusted_http_detail(monkeypatch):
    class Response:
        status_code = 503

        @staticmethod
        def json():
            return {"detail": {"reason": "raw internal error text"}}

    monkeypatch.setattr(
        "app.services.orb_view.requests.get",
        lambda *_args, **_kwargs: Response(),
    )

    assert fetch_opening_ranges() == {
        "_marketpin_orb_fetch_failure": {
            "reason": "ORB_HTTP_ERROR",
            "status_code": 503,
        }
    }


def test_fetch_opening_ranges_distinguishes_client_timeout(monkeypatch):
    def raise_timeout(*_args, **_kwargs):
        raise orb_view.requests.Timeout("late")

    monkeypatch.setattr("app.services.orb_view.requests.get", raise_timeout)

    assert fetch_opening_ranges() == {
        "_marketpin_orb_fetch_failure": {
            "reason": "ORB_REQUEST_TIMEOUT",
            "status_code": None,
        }
    }


def test_opening_range_rows_fails_closed_on_malformed_nested_values():
    rows = opening_range_rows(
        {
            "symbols": {
                "SPX": {
                    "capture_status": "complete",
                    "capture_evidence": {
                        "sample_count": "not-a-count",
                        "capture_ratio": "not-a-ratio",
                    },
                    "pin_behavior": ["malformed"],
                    "provenance": {
                        "subscription_generations": "not-a-list",
                    },
                    "reference_semantics": ["malformed"],
                    "opening_ranges": ["malformed"],
                    "warnings": "not-a-list",
                }
            }
        }
    )

    assert rows[0]["Samples"] == 0
    assert rows[0]["Coverage %"] is None
    assert rows[0]["Generation"] == ""
    assert rows[0]["Gamma pin"] is None
    assert rows[0]["5m ORB"] == "unavailable"
    assert rows[0]["Warnings"] == "ORB_V2_LIVE_CONTRACT_UNVERIFIED"


def test_opening_range_rows_never_labels_stale_structure_as_current_pin():
    rows = opening_range_rows(
        _live_envelope(
            {
                "SPX": {
                    "capture_status": "complete",
                    "pin_behavior": {
                        "gamma_pin": 6520.0,
                        "gamma_pin_change_from_open": 20.0,
                        "max_pain": 6480.0,
                        "max_pain_change_from_open": 10.0,
                    },
                    "provenance": {
                        "structure_reference_status": "stale",
                    },
                }
            }
        )
    )

    assert rows[0]["Pin alignment"] == "stale"
    assert rows[0]["ORB + structure eligible"] is False
    assert rows[0]["Gamma pin"] is None
    assert rows[0]["Pin move from open"] is None
    assert rows[0]["Max pain"] is None
    assert rows[0]["Pain move from open"] is None


def test_legacy_or_unbound_orb_is_context_only_never_current():
    rows = opening_range_rows(
        {
            "schema_version": "marketpin-reference-orb.collection.v1",
            "symbols": {
                "SPX": {
                    "schema_version": "marketpin-reference-orb.v1",
                    "capture_status": "complete",
                    "opening_price": 6500.0,
                    "orb_low": 6490.0,
                    "orb_high": 6510.0,
                    "current_price": 6520.0,
                    "current_reference_fresh": True,
                    "breakout_direction": "bullish",
                    "directional_evidence_eligible": True,
                    "combined_structure_directional_evidence_eligible": True,
                    "reference_semantics": {
                        "kind": "same_day_index_option_parity",
                        "directional_base_eligible": True,
                    },
                    "opening_ranges": {
                        "5m": {
                            "capture_status": "complete",
                            "orb_low": 6495.0,
                            "orb_high": 6505.0,
                            "breakout_direction": "bullish",
                            "directional_evidence_eligible": True,
                        }
                    },
                    "pin_behavior": {
                        "gamma_pin": 6525.0,
                        "max_pain": 6480.0,
                    },
                    "provenance": {"structure_reference_status": "aligned"},
                }
            },
        }
    )

    assert rows[0]["Open"] == 6500.0
    assert rows[0]["ORB high"] == 6510.0
    assert rows[0]["Current"] is None
    assert rows[0]["Breakout"] == "unavailable"
    assert rows[0]["5m ORB"] == "6,495.00–6,505.00 | context only"
    assert rows[0]["ORB eligible"] is False
    assert rows[0]["ORB + structure eligible"] is False
    assert rows[0]["Gamma pin"] is None
    assert rows[0]["Max pain"] is None
    assert rows[0]["Live contract"] == "context only"


def test_noncanonical_runtime_epoch_cannot_authorize_current_orb_display():
    payload = _live_envelope(
        {
            "SPX": {
                "capture_status": "complete",
                "current_price": 6505.0,
                "directional_evidence_eligible": True,
                "provenance": {},
            }
        }
    )
    payload["active_runtime_context"]["subscription_epoch_id"] = "E" * 64
    payload["symbols"]["SPX"]["provenance"][
        "active_subscription_epoch_id"
    ] = "E" * 64

    row = opening_range_rows(payload)[0]

    assert row["Current"] is None
    assert row["ORB eligible"] is False
    assert row["Epoch"] == "unavailable"
    assert row["Epoch full"] is None
    assert row["Epoch aligned"] is False
