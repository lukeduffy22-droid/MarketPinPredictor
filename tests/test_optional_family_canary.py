from backend.optional_family_canary import RUTOptionalFamilyCanary


def _status(*, age: float = 1.0, lag: float = 0.1, valid: bool = True) -> dict:
    return {
        "data_age_seconds": age,
        "receive_to_process_lag_p95_seconds": lag,
        "validation_is_valid": valid,
        "primary_pair_coverage_ratio": 1.0,
        "fresh_quote_count": 100,
        "expected_quote_count": 100,
    }


def test_rut_canary_never_implicitly_adds_a_subscription():
    guard = RUTOptionalFamilyCanary(enabled=True, requested_families=("SPX", "NDX"))

    decision = guard.evaluate({"SPX": _status(), "NDX": _status()})

    assert decision.state == "blocked"
    assert decision.rollback_required is False
    assert decision.reasons == ("RUT_NOT_REQUESTED_NO_IMPLICIT_SUBSCRIPTION",)


def test_rut_canary_observes_only_inside_fixed_budgets():
    guard = RUTOptionalFamilyCanary(
        enabled=True,
        requested_families=("SPX", "NDX", "RUT"),
    )

    decision = guard.evaluate(
        {"SPX": _status(), "NDX": _status(), "RUT": _status(age=2.0, lag=0.2)}
    )

    assert decision.state == "observing"
    assert decision.rollback_required is False
    assert decision.budgets["core_freshness_seconds"] == 15.0
    assert decision.budgets["core_lag_seconds"] == 2.0
    assert decision.budgets["core_primary_pair_coverage_ratio"] == 0.10
    assert decision.budgets["core_fresh_quote_coverage_ratio"] == 0.50


def test_rut_canary_status_does_not_consume_warmup_as_failure():
    guard = RUTOptionalFamilyCanary(
        enabled=True,
        requested_families=("SPX", "NDX", "RUT"),
    )

    assert guard.status().state == "armed"
    assert guard.status().rollback_required is False


def test_rut_canary_rolls_back_permanently_when_core_degrades():
    guard = RUTOptionalFamilyCanary(
        enabled=True,
        requested_families=("SPX", "NDX", "RUT"),
    )

    first = guard.evaluate(
        {
            "SPX": _status(age=16.0),
            "NDX": _status(),
            "RUT": _status(),
        }
    )
    second = guard.evaluate(
        {"SPX": _status(), "NDX": _status(), "RUT": _status()}
    )

    assert first.state == "rolled_back"
    assert first.rollback_required is True
    assert "CORE_FRESHNESS_BUDGET:SPX" in first.reasons
    assert "RUT" not in first.allowed_families
    assert first.allowed_families == ("NDX", "SPX")
    assert second.state == "rolled_back"
    assert second.reasons == first.reasons


def test_rut_canary_fails_closed_when_core_coverage_metrics_are_missing():
    guard = RUTOptionalFamilyCanary(
        enabled=True, requested_families=("SPX", "NDX", "RUT")
    )
    incomplete = _status()
    incomplete.pop("primary_pair_coverage_ratio")
    incomplete.pop("fresh_quote_count")

    decision = guard.evaluate(
        {"SPX": incomplete, "NDX": _status(), "RUT": _status()}
    )

    assert decision.rollback_required
    assert "CORE_PRIMARY_PAIR_COVERAGE:SPX" in decision.reasons
    assert "CORE_FRESH_QUOTE_COVERAGE:SPX" in decision.reasons


def test_rut_canary_preview_reports_invalid_without_latching_rollback():
    guard = RUTOptionalFamilyCanary(
        enabled=True, requested_families=("SPX", "NDX", "RUT")
    )

    preview = guard.preview(
        {
            "SPX": _status(),
            "NDX": _status(),
            "RUT": _status(valid=False),
        }
    )

    assert preview.rollback_required is True
    assert "CANARY_INVALID:RUT" in preview.reasons
    assert guard.status().state == "armed"
    assert guard.status().rollback_required is False
