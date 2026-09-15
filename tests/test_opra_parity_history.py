from app.utils.opra_parity_history import (
    OPRA_PARITY_HISTORY_NOTICE,
    build_opra_parity_gamma_history,
    latest_contiguous_parity_reference,
)


def _valid_record(
    timestamp: str,
    *,
    epoch: str = "a" * 64,
    generation: int = 1,
    spot: object = 100.0,
    pin: object = 101.0,
) -> dict[str, object]:
    return {
        "generated_at_utc": timestamp,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot_source": "databento_opra_put_call_parity",
        "spot_last": spot,
        "primary_gamma_pin_strike": pin,
        "subscription_epoch_id": epoch,
        "subscription_generation": generation,
        "calculation_id": f"calc-{timestamp}",
    }


def test_history_uses_only_valid_identity_bearing_opra_parity_snapshots():
    valid = _valid_record("2026-09-09T13:30:01Z")
    invalid = {
        **_valid_record("2026-09-09T13:31:01Z"),
        "validation_is_valid": False,
    }
    excluded = {
        **_valid_record("2026-09-09T13:32:01Z"),
        "gamma_excluded_from_model": True,
    }
    non_parity = {
        **_valid_record("2026-09-09T13:33:01Z"),
        "spot_source": "official_exchange_ohlcv",
    }
    missing_identity = {
        **_valid_record("2026-09-09T13:34:01Z"),
        "subscription_epoch_id": None,
    }
    non_finite = _valid_record("2026-09-09T13:35:01Z", spot=float("nan"))

    history = build_opra_parity_gamma_history(
        [valid, invalid, excluded, non_parity, missing_identity, non_finite]
    )

    assert history.included_records == 1
    assert history.excluded_records == 5
    assert len(history.segments) == 1
    point = history.segments[0].points[0]
    assert point.parity_spot == 100.0
    assert point.gamma_pin == 101.0
    assert point.timestamp_utc.isoformat() == "2026-09-09T13:30:01+00:00"


def test_epoch_and_generation_changes_are_discrete_ordered_segments():
    records = [
        _valid_record(
            "2026-09-09T13:33:00Z", epoch="b" * 64, generation=2, spot=104
        ),
        _valid_record(
            "2026-09-09T13:30:00Z", epoch="a" * 64, generation=1, spot=100
        ),
        _valid_record(
            "2026-09-09T13:31:00Z", epoch="a" * 64, generation=1, spot=101
        ),
        _valid_record(
            "2026-09-09T13:32:00Z", epoch="b" * 64, generation=1, spot=103
        ),
        _valid_record(
            "2026-09-09T13:34:00Z", epoch="b" * 64, generation=1, spot=105
        ),
    ]

    history = build_opra_parity_gamma_history(records)

    assert [
        (segment.subscription_epoch_id[0], segment.subscription_generation)
        for segment in history.segments
    ] == [("a", 1), ("b", 1), ("b", 2), ("b", 1)]
    assert [point.parity_spot for point in history.segments[0].points] == [100.0, 101.0]
    assert [len(segment.points) for segment in history.segments] == [2, 1, 1, 1]


def test_research_notice_disclaims_price_and_forecast_authority():
    notice = OPRA_PARITY_HISTORY_NOTICE.lower()

    assert "research only" in notice
    assert "not official exchange ohlcv" in notice
    assert "not a validated current forecast" in notice
    assert "no interpolation" in notice


def test_reference_envelope_uses_only_latest_contiguous_segment():
    records = [
        _valid_record("2026-09-09T13:30:00Z", epoch="a" * 64, spot=90),
        _valid_record("2026-09-09T13:31:00Z", epoch="a" * 64, spot=110),
        _valid_record("2026-09-09T13:32:00Z", epoch="b" * 64, spot=200),
        _valid_record("2026-09-09T13:33:00Z", epoch="a" * 64, spot=100),
        _valid_record("2026-09-09T13:34:00Z", epoch="a" * 64, spot=102),
    ]

    reference = latest_contiguous_parity_reference(
        build_opra_parity_gamma_history(records)
    )

    assert reference is not None
    assert reference.subscription_epoch_id == "a" * 64
    assert reference.subscription_generation == 1
    assert reference.sample_count == 2
    assert reference.range_low == 100
    assert reference.range_high == 102
    assert reference.reference_value is None
    assert reference.first_timestamp_utc.isoformat() == "2026-09-09T13:33:00+00:00"
    assert reference.last_timestamp_utc.isoformat() == "2026-09-09T13:34:00+00:00"


def test_singleton_latest_segment_is_reference_only_not_a_range():
    history = build_opra_parity_gamma_history(
        [
            _valid_record("2026-09-09T13:30:00Z", epoch="a" * 64, spot=100),
            _valid_record("2026-09-09T13:31:00Z", epoch="a" * 64, spot=101),
            _valid_record(
                "2026-09-09T13:32:00Z",
                epoch="b" * 64,
                generation=2,
                spot=205,
            ),
        ]
    )

    reference = latest_contiguous_parity_reference(history)

    assert reference is not None
    assert reference.sample_count == 1
    assert reference.reference_value == 205
    assert reference.range_low is None
    assert reference.range_high is None
