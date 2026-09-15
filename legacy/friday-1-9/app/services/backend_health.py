"""Formatting helpers for backend health shown in Streamlit."""

from typing import Any, Dict, List


def build_symbol_health_rows(health_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Convert backend health details into concise dashboard rows."""
    rows = []
    for symbol, details in health_data.get("symbols", {}).items():
        model = details.get("prediction_model")
        orb = details.get("orb")
        if model is None or orb is None:
            continue
        issues = model.get("issues") or []
        rows.append(
            {
                "Symbol": symbol,
                "Market Data": "Ready" if details.get("fresh") else "Stale",
                "ORB": (
                    "Complete"
                    if orb.get("complete")
                    else "Forming"
                    if orb.get("recorded")
                    else "Waiting"
                ),
                "Gamma Pin": (
                    "Ready"
                    if details.get("gamma", {}).get("ready")
                    else "Unsupported"
                    if not details.get("gamma", {}).get("supported", True)
                    else "Waiting"
                ),
                "Gamma/Flow Model": "Ready" if model.get("ready") else "Blocked",
                "Issues": ", ".join(issues) if issues else "None",
            }
        )
    return rows
