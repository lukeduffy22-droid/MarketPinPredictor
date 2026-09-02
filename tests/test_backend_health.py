"""Tests for Streamlit backend-health formatting."""

from app.services.backend_health import build_symbol_health_rows


def test_build_symbol_health_rows_surfaces_orb_and_model_blockers():
    rows = build_symbol_health_rows(
        {
            "symbols": {
                "SPX": {
                    "fresh": True,
                    "orb": {"recorded": True, "complete": True},
                    "gamma": {"supported": True, "ready": True},
                    "prediction_model": {"ready": True, "issues": []},
                },
                "NDX": {
                    "fresh": False,
                    "orb": {"recorded": False, "complete": False},
                    "gamma": {"supported": True, "ready": False},
                    "prediction_model": {
                        "ready": False,
                        "issues": ["live_oi_unavailable"],
                    },
                },
                "VIX": {"fresh": True},
            }
        }
    )

    assert rows == [
        {
            "Symbol": "SPX",
            "Market Data": "Ready",
            "ORB": "Complete",
            "Gamma Pin": "Ready",
            "Gamma/Flow Model": "Ready",
            "Issues": "None",
        },
        {
            "Symbol": "NDX",
            "Market Data": "Stale",
            "ORB": "Waiting",
            "Gamma Pin": "Waiting",
            "Gamma/Flow Model": "Blocked",
            "Issues": "live_oi_unavailable",
        },
    ]
