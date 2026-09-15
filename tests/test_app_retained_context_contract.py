from pathlib import Path


APP_SOURCE = (Path(__file__).resolve().parents[1] / "app.py").read_text(
    encoding="utf-8"
)


def test_no_forecast_context_is_abstaining_and_presentation_only():
    start = APP_SOURCE.index("forecast_symbols =")
    end = APP_SOURCE.index("# Display predictions", start)
    context_block = APP_SOURCE[start:end]

    assert "End-of-day close estimate: Unavailable — ABSTAIN" in context_block
    assert "render_retained_opra_parity_history" in context_block
    assert "symbols_requiring_retained_context" in context_block
    for forbidden in (
        "close-overlay",
        "create_price_chart(",
        "analyze_prediction(",
        "get_risk_assessment(",
        "save_alert(",
        "export_to_csv(",
    ):
        assert forbidden not in context_block


def test_retained_renderer_is_reused_and_post_attempt_prompt_abstains():
    assert APP_SOURCE.count("render_retained_opra_parity_history(") == 2
    assert 'st.metric("Current OPRA parity reference"' in APP_SOURCE
    assert "last_analysis_attempt_symbols" in APP_SOURCE
    assert "Analysis completed, but no eligible end-of-day close forecast" in APP_SOURCE
    assert "Click 'Analyze & Predict' to generate predictions for selected indexes" in APP_SOURCE
