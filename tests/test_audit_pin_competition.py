from app.core.audit_persistence import dict_to_audit_snapshot


def test_audit_snapshot_round_trips_pin_competition_fields():
    snapshot = dict_to_audit_snapshot({
        "symbol": "NDX",
        "primary_gamma_pin_strike": 29370.0,
        "primary_gamma_pin_abs_gex": 28.17,
        "pin_runner_up_strike": 29310.0,
        "pin_runner_up_abs_gex": 27.86,
        "pin_lead_abs_gex": 0.31,
        "pin_lead_ratio": 0.011,
        "pin_competition_threshold": 0.10,
        "pin_is_contested": True,
        "pin_competition_reason": "PIN_CONTESTED: test",
        "pin_competition_formula_version": "pin-competition-v1-top-two-abs-net-gex",
    })

    assert snapshot.pin_runner_up_strike == 29310.0
    assert snapshot.pin_lead_ratio == 0.011
    assert snapshot.pin_is_contested is True
    assert snapshot.pin_competition_reason == "PIN_CONTESTED: test"
