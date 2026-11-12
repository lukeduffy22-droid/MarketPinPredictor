"""Presentation for the separately versioned structural research estimate."""
from __future__ import annotations

import math


def render_structural_regime(prediction: dict | None) -> None:
    import streamlit as st

    st.markdown("**Structural Regime Prediction · 0DTE**")
    result = prediction if isinstance(prediction, dict) else {}
    values = [result.get(key) for key in ("predicted_close", "band_low", "band_high", "confidence")]
    valid_numbers = all(type(value) in (int, float) and math.isfinite(value) for value in values)
    if (result.get("status") != "AVAILABLE" or not valid_numbers
            or values[1] > values[2] or not 0 <= values[3] <= 1):
        reasons = result.get("reasons") or ["Awaiting a current, validated same-day structural snapshot"]
        if reasons == ["COMMITTED_CALCULATION_ID_MISSING"]:
            st.info("Awaiting the next saved calculation. Structural estimates require saved inputs; live quote updates can arrive between captures.")
        else:
            st.info("Structural estimate unavailable: " + "; ".join(map(str, reasons)))
        return
    st.warning("Research-only structural estimate — not decision-grade.")
    if result.get("source_binding") == "independent_committed_calculation.v1":
        st.caption("Uses the saved calculation and observation time below; it can be older than the live EOD estimate and expires under the same freshness limit.")
    st.metric("Structural predicted close", f"{values[0]:,.2f}")
    st.caption(f"Structural band: {values[1]:,.2f}–{values[2]:,.2f} · not a calibrated prediction interval")
    label = "high" if values[3] >= 0.75 else "medium" if values[3] >= 0.5 else "low"
    st.caption(f"Heuristic confidence score: {values[3]:.3f} / 1 ({label}); not a probability")
    regime = result.get("regime") or {}
    st.write(str(regime.get("structural_label") or "Unclassified").replace("_", " "))
    st.caption("Regime names are rule labels. The zero-gamma input is a strike-curve crossing; dealer positioning is not established.")
    inputs = result.get("inputs") or {}
    st.caption(f"Volatility context: {'valid' if inputs.get('vol_valid') is True else 'unavailable or invalid'} · {inputs.get('session_state', 'unknown')} · As of {result.get('as_of_utc', 'unavailable')}")
    if result.get("journal_status") == "FAILED":
        st.warning("Research journal write failed; this estimate may be missing from later close comparisons.")
    with st.expander("Structural inputs and formula evidence", expanded=False):
        st.json({key: result.get(key) for key in (
            "model_version", "prediction_id", "inputs", "regime", "configuration",
            "gex_unit", "model_lean_version", "target_close_utc", "journal_status",
        )})
