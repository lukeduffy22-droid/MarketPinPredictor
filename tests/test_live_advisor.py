from app.services.live_advisor import _advisor_live_eligibility, build_advisor_report


EPOCH = "e" * 64


def _pipeline(*, handoff_status: str = "active", epoch_is_current: bool = True):
    return {
        "prediction_pipeline_ok": True,
        "handoff_status": handoff_status,
        "subscription_epoch_id": EPOCH,
        "subscription_epoch_valid": True,
        "epoch_mismatch_symbols": [] if epoch_is_current else ["SPX"],
        "generation_mismatch_symbols": [],
        "required_epoch_mismatch_symbols": [],
        "required_generation_mismatch_symbols": [],
        "symbol_status": {
            "SPX": {
                "subscription_epoch_id": EPOCH if epoch_is_current else "f" * 64,
                "active_subscription_epoch_id": EPOCH,
                "epoch_is_current": epoch_is_current,
                "subscription_generation": 7,
                "active_generation": 7,
                "generation_is_current": True,
                "usable_for_prediction": True,
                "is_stale": False,
                "fresh_quote_count": 8,
            }
        },
    }


def _context(pipeline):
    return {
        "requested_symbols": ["SPX"],
        "snapshot": {"source": "sse"},
        "health": {
            "websocket": "active",
            "buffer_health": "healthy",
            "symbols_subscribed": 1200,
        },
        "pipeline": pipeline,
        "universe": {"symbols_subscribed": 1200},
        "dashboards": {"SPX": {"status": "live"}},
    }


def test_advisor_current_heading_requires_complete_current_symbol_identity():
    report = build_advisor_report(_context(_pipeline()))

    assert report["live_state_eligible"] is True
    assert report["live_state_heading"] == "Current Live State"
    assert report["current_eligible_symbols"] == ["SPX"]
    assert report["current_live_state"][0].startswith("Decision: CURRENT")


def test_advisor_epoch_mismatch_is_explicit_abstention_even_with_live_dashboard():
    report = build_advisor_report(_context(_pipeline(epoch_is_current=False)))

    assert report["live_state_eligible"] is False
    assert report["live_state_heading"] == "Live State Evidence — ABSTAIN"
    assert report["current_eligible_symbols"] == []
    assert report["current_live_state"][0].startswith("Decision: ABSTAIN")
    assert "subscription epoch mismatch" in report["current_live_state"][0]


def test_advisor_stopped_handoff_cannot_be_labeled_current():
    eligible, current_symbols, reasons = _advisor_live_eligibility(
        _pipeline(handoff_status="stopped"),
        ["SPX"],
    )

    assert eligible is False
    assert current_symbols == []
    assert "stream handoff is not active" in reasons
