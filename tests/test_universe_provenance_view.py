from __future__ import annotations

import copy

import pytest

from app.services.live_data_client import _source_metadata, build_expiration_profile_view
from app.services.universe_provenance_view import universe_provenance_view


def _payload(*, session_date: str, source_date: str, is_fallback: bool) -> dict:
    return {
        "provider": "databento",
        "source_label": "Databento Live",
        "universe_provenance": {
            "mode": "current_day_cache",
            "trading_date": session_date,
            "source_date": source_date,
            "is_fallback": is_fallback,
            "source_sha256": "a" * 64,
        },
    }


def test_prior_session_date_mismatch_overrides_false_producer_flag_for_display():
    payload = _payload(
        session_date="2026-10-20",
        source_date="2026-10-19",
        is_fallback=False,
    )
    before = copy.deepcopy(payload)

    view = universe_provenance_view(payload)

    assert view["reported_is_fallback"] is False
    assert view["is_fallback"] is True
    assert view["date_mismatch"] is True
    assert view["prior_session"] is True
    assert view["authority_status"] == "non_authoritative_prior_session"
    assert "non-authoritative prior-session fallback context" in view["warning"]
    assert "not current-session data" in view["warning"]
    assert payload == before


def test_future_source_date_is_mismatched_and_never_labeled_current():
    view = universe_provenance_view(
        _payload(
            session_date="2026-10-20",
            source_date="2026-10-21",
            is_fallback=False,
        )
    )

    assert view["is_fallback"] is True
    assert view["prior_session"] is False
    assert view["authority_status"] == "non_authoritative_date_mismatch"
    assert "mismatched-date fallback context" in view["warning"]


def test_explicit_runtime_date_takes_precedence_over_stale_cache_metadata_date():
    payload = _payload(
        session_date="2026-10-19",
        source_date="2026-10-19",
        is_fallback=False,
    )
    payload["current_trading_date"] = "2026-10-20"

    view = universe_provenance_view(payload)

    assert view["trading_date"] == "2026-10-20"
    assert view["source_date"] == "2026-10-19"
    assert view["is_fallback"] is True
    assert view["authority_status"] == "non_authoritative_prior_session"


@pytest.mark.parametrize(
    ("session_date", "source_date", "expected_status"),
    [
        ("2026-10-20", "2026-10-20", "source_date_matches_session"),
        ("not-a-date", "2026-10-20", "unverified"),
        ("2026-10-20", "not-a-date", "unverified"),
    ],
)
def test_only_exact_date_mismatch_changes_effective_fallback(
    session_date: str,
    source_date: str,
    expected_status: str,
):
    view = universe_provenance_view(
        _payload(
            session_date=session_date,
            source_date=source_date,
            is_fallback=False,
        )
    )

    assert view["is_fallback"] is False
    assert view["date_mismatch"] is False
    assert view["authority_status"] == expected_status
    assert view["warning"] is None


def test_source_metadata_overrides_live_label_with_explicit_non_authoritative_warning():
    source = _source_metadata(
        _payload(
            session_date="2026-10-20",
            source_date="2026-10-19",
            is_fallback=False,
        )
    )

    assert source["is_fallback"] is True
    assert source["display_mode"] == "fallback"
    assert source["source_label"] == (
        "Prior-session universe context only (non-authoritative)"
    )
    assert source["market_data_source_label"] == source["source_label"]
    assert source["universe_session_trading_date"] == "2026-10-20"
    assert source["universe_source_date"] == "2026-10-19"
    assert "not current-session data" in source["provenance_warning"]


def test_expiration_view_exposes_effective_and_reported_fallback_separately():
    payload = _payload(
        session_date="2026-10-20",
        source_date="2026-10-19",
        is_fallback=False,
    )
    payload.update(
        {
            "primary_expiration": "2026-10-20",
            "subscription_expirations": [],
            "expiration_profiles": [],
        }
    )

    provenance = build_expiration_profile_view(payload)["provenance"]

    assert provenance["trading_date"] == "2026-10-20"
    assert provenance["source_date"] == "2026-10-19"
    assert provenance["reported_is_fallback"] is False
    assert provenance["is_fallback"] is True
    assert provenance["authority_status"] == "non_authoritative_prior_session"
    assert provenance["warning"] is not None
