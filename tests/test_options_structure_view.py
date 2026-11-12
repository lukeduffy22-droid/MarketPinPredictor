import copy
from datetime import datetime, timedelta, timezone

import pytest

from app.services.live_data_client import build_expiration_profile_view
from app.services.options_structure_view import (
    close_overlay_unavailable_message,
    expiration_gamma_unavailable_message,
    oi_max_pain_context,
    shadow_expiration_table_rows,
)


def _payload():
    return {
        "primary_expiration": "2026-09-25",
        "max_pain_source": "full-oi-universe",
        "universe_provenance": {
            "source_date": "2026-09-25", "trading_date": "2026-09-25",
            "is_fallback": False, "mode": "current_day_cache",
            "cache_metadata_status": "verified", "source_sha256": "a" * 64,
            "provider_statistics_end": "2026-09-25T11:50:00+00:00",
        },
        "subscription_expirations": [{"expiration": "2026-09-28", "role": "next-listed", "contracts": 100}],
        "expiration_profiles": [],
        "oi_expiration_profiles": [{"expiration": "2026-09-28", "max_pain": 7710,
                                    "total_open_interest": 195053, "contracts": 482, "strikes": 241}],
    }


def test_independent_oi_value_survives_missing_gamma_without_promoting_authority():
    payload = _payload()
    before = copy.deepcopy(payload)
    view = build_expiration_profile_view(payload)
    row = next(row for row in view["rows"] if row["expiration"] == "2026-09-28")
    assert row["gamma_pin"] is None
    assert row["max_pain"] is None  # No synthetic GEX profile is created.
    assert row["validation_status"] == "unavailable"
    assert row["validation_is_valid"] is False
    assert row["oi_max_pain_context"]["value"] == 7710
    assert row["open_interest_as_of_utc"] is None  # Query cutoff is not an OI timestamp.
    rendered = shadow_expiration_table_rows(view)[0]
    assert rendered["Max Pain (OI context)"] == "$7,710"
    assert rendered["Gamma Pin"] == "Unavailable"
    assert rendered["Gamma status"] == "unavailable"
    assert rendered["Statistics cutoff (UTC)"] == "2026-09-25T11:50:00+00:00"
    assert payload == before


@pytest.mark.parametrize("key,value", [
    ("is_fallback", True), ("source_date", "2026-09-24"),
    ("trading_date", None), ("mode", "prior_cache_filtered"),
    ("cache_metadata_status", "unverified"), ("source_sha256", "invalid"),
])
def test_unverified_or_prior_day_oi_does_not_fill_missing_fields(key, value):
    payload = _payload()
    payload["universe_provenance"][key] = value
    assert oi_max_pain_context(payload, "2026-09-28")["value"] is None


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), None])
def test_nonpositive_or_malformed_oi_estimate_is_withheld(value):
    payload = _payload()
    payload["oi_expiration_profiles"][0]["max_pain"] = value
    assert oi_max_pain_context(payload, "2026-09-28")["value"] is None


def test_wrong_date_and_duplicate_oi_profiles_cannot_supply_value():
    payload = _payload()
    assert oi_max_pain_context(payload, "2026-09-29")["value"] is None
    payload["oi_expiration_profiles"] *= 2
    assert oi_max_pain_context(payload, "2026-09-28")["value"] is None


@pytest.mark.parametrize("detail,expected", [
    ("No passport-backed close-overlay prediction is available for SPX", "No verified close forecast"),
    ("Close-overlay payload is stale for SPX (age=90s)", "source is stale"),
    ({"reason": "RUNTIME_READ_TIMEOUT"}, "RUNTIME_READ_TIMEOUT"),
    ({"reason": []}, "HTTP 503"),
])
def test_failed_forecast_read_never_claims_a_background_computation(detail, expected):
    message = close_overlay_unavailable_message(503, {"detail": detail})
    assert expected in message
    assert "Computing signals" not in message
    assert "pending" not in message


def _staging_snapshot(now):
    identity = {"subscription_epoch_id": "a" * 64, "subscription_generation": 1}
    return {"timestamp_utc": now.isoformat(), "pipeline": {**identity, "stream_connected": True},
            "health": {"provider": "databento", "websocket": "active", "subscription_staging": {
                **identity, "full_selected_contract_count": 2530, "active_contract_count": 1442,
                "deferred_contract_count": 1088, "promotion_reasons": ["QUEUE_FULL_DELTA_FREEZES_PROMOTION"]}}}


def test_expiration_warning_separates_connected_feed_from_deferred_contracts():
    now = datetime(2026, 10, 6, 19, tzinfo=timezone.utc)
    snapshot = _staging_snapshot(now)
    before = copy.deepcopy(snapshot)
    message = expiration_gamma_unavailable_message(
        [{"expiration": "2026-10-07", "validation_status": "unavailable"}], snapshot, now=now)
    assert "2026-10-07" in message
    assert "overall live connection was connected" in message
    assert "2,530 contracts selected, 1,442 active, 1,088 deferred" in message
    assert "QUEUE_FULL_DELTA_FREEZES_PROMOTION" in message
    assert "Across all markets" in message
    assert snapshot == before


@pytest.mark.parametrize("change", [
    lambda p, now: p.update(timestamp_utc=(now-timedelta(seconds=16)).isoformat()),
    lambda p, now: p.update(timestamp_utc=(now+timedelta(seconds=1)).isoformat()),
    lambda p, now: p["health"]["subscription_staging"].update(subscription_generation=2),
    lambda p, now: p["pipeline"].update(subscription_epoch_id="b"*64),
])
def test_unknown_or_mismatched_staging_never_claims_current_contract_counts(change):
    now = datetime(2026, 10, 6, 19, tzinfo=timezone.utc)
    snapshot = _staging_snapshot(now)
    change(snapshot, now)
    message = expiration_gamma_unavailable_message([], snapshot, now=now)
    assert "contracts selected" not in message
    assert "QUEUE_FULL_DELTA_FREEZES_PROMOTION" not in message


def test_expiration_warning_sse_without_staging_does_not_invent_deferred_counts():
    now = datetime(2026, 10, 6, 19, tzinfo=timezone.utc)
    snapshot = _staging_snapshot(now)
    snapshot["health"].pop("subscription_staging")
    message = expiration_gamma_unavailable_message([], snapshot, now=now)
    assert "overall live connection was connected" in message
    assert "contracts selected" not in message
    assert "can be planned" in message
    assert "Missing gamma is withheld" in message


def test_expiration_warning_without_live_snapshot_preserves_unknown_transport():
    message = expiration_gamma_unavailable_message([])
    assert "was connected" not in message
    assert "does not establish the status" in message
