from ai_analysis import _confidence_context


def test_confidence_context_labels_data_quality_score_as_uncalibrated():
    message = _confidence_context(
        82.5,
        confidence_kind="data_quality_heuristic",
        confidence_calibrated=False,
    )

    assert "82.5/100" in message
    assert "not an empirical forecast probability" in message


def test_confidence_context_preserves_unavailable_instead_of_zero():
    assert _confidence_context(None) == "Model quality score: unavailable"
