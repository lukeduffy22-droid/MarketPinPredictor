from datetime import datetime, timedelta, timezone
from threading import Event

import pandas as pd

from backend import underlying_validator


def test_unknown_symbol_is_unavailable():
    result = underlying_validator.validate_underlying("COMP", 100.0)

    assert result["underlying_validation_status"] == "unavailable"
    assert result["underlying_price"] is None


def test_eq_underlying_bar_is_cached_and_non_blocking(monkeypatch):
    calls = []
    refreshed = Event()

    def fake_bar(proxy):
        calls.append(proxy)
        refreshed.set()
        return {
            "underlying_validation_status": "bar_available",
            "underlying_proxy_symbol": proxy,
            "underlying_price": 500.0,
            "underlying_timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "underlying_age_seconds": 1.0,
            "underlying_divergence_points": None,
            "underlying_divergence_pct": None,
            "underlying_validation_reason": None,
        }

    monkeypatch.setattr(underlying_validator, "_latest_proxy_bar", fake_bar)
    underlying_validator._latest.clear()
    underlying_validator._latest_checked.clear()
    underlying_validator._refreshing.clear()
    underlying_validator._structural_denials.clear()

    first = underlying_validator.validate_underlying("SPX", 5000.0)
    second = underlying_validator.validate_underlying("SPX", 5000.0)

    assert first["underlying_validation_status"] == "unavailable"
    assert first["underlying_validation_reason"] == "EQUS validation warming"
    assert refreshed.wait(timeout=2)
    second = underlying_validator.validate_underlying("SPX", 5000.0)
    assert second["underlying_price"] == 500.0
    same_proxy = underlying_validator.validate_underlying("SPY", 500.0)
    assert same_proxy["underlying_price"] == 500.0
    assert calls == ["SPY"]


def test_proxy_bar_window_on_monday_includes_friday_and_current_day(monkeypatch):
    monday_now = datetime(2026, 8, 24, 15, 30, 26, tzinfo=timezone.utc)
    monkeypatch.setattr(underlying_validator, "LOOKBACK_DAYS", 2)

    start, end_exclusive = underlying_validator._proxy_bar_query_window(monday_now)

    assert start == "2026-08-21"
    assert end_exclusive == "2026-08-24T15:30:26Z"
    assert end_exclusive != "2026-08-24"


def test_latest_proxy_bar_uses_exclusive_current_instant_without_network(monkeypatch):
    monday_now = datetime(2026, 8, 24, 15, 30, 26, tzinfo=timezone.utc)
    current_bar = pd.Timestamp("2026-08-24T15:29:00Z")
    calls = []

    class _Result:
        @staticmethod
        def to_df():
            return pd.DataFrame({"ts_event": [current_bar], "close": [650.25]})

    class _Timeseries:
        @staticmethod
        def get_range(**kwargs):
            calls.append(kwargs)
            return _Result()

    class _Historical:
        def __init__(self, api_key):
            assert api_key == "test-key"
            self.timeseries = _Timeseries()

    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    monkeypatch.setattr(underlying_validator, "LOOKBACK_DAYS", 2)
    monkeypatch.setattr(underlying_validator, "_utcnow", lambda: monday_now)
    monkeypatch.setattr(underlying_validator.db, "Historical", _Historical)

    result = underlying_validator._latest_proxy_bar("SPY")

    assert calls == [
        {
            "dataset": "EQUS.MINI",
            "symbols": ["SPY"],
            "schema": "ohlcv-1m",
            "start": "2026-08-21",
            "end": "2026-08-24T15:30:26Z",
        }
    ]
    assert result["underlying_validation_status"] == "bar_available"
    assert result["underlying_price"] == 650.25
    assert result["underlying_timestamp_utc"] == "2026-08-24T15:29:00Z"


def test_latest_proxy_bar_retries_once_at_provider_available_end(monkeypatch):
    now = datetime(2026, 8, 24, 15, 30, 26, tzinfo=timezone.utc)
    current_bar = pd.Timestamp("2026-08-24T15:29:00Z")
    calls = []

    class _Result:
        @staticmethod
        def to_df():
            return pd.DataFrame({"ts_event": [current_bar], "close": [650.25]})

    class _Timeseries:
        @staticmethod
        def get_range(**kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                raise RuntimeError(
                    "422 data_end_after_available_end\n"
                    "The dataset EQUS.MINI has data available up to "
                    "'2026-08-24 15:30:00+00:00'. The `end` in the query "
                    "('2026-08-24 15:30:26+00:00') is after the available range."
                )
            return _Result()

    class _Historical:
        def __init__(self, api_key):
            assert api_key == "test-key"
            self.timeseries = _Timeseries()

    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    monkeypatch.setattr(underlying_validator, "LOOKBACK_DAYS", 2)
    monkeypatch.setattr(underlying_validator, "_utcnow", lambda: now)
    monkeypatch.setattr(underlying_validator.db, "Historical", _Historical)

    result = underlying_validator._latest_proxy_bar("SPY")

    assert [call["end"] for call in calls] == [
        "2026-08-24T15:30:26Z",
        "2026-08-24T15:30:00Z",
    ]
    assert result["underlying_validation_status"] == "bar_available"
    assert result["underlying_price"] == 650.25


def test_stale_available_end_remains_unavailable_with_proxy_identity(monkeypatch):
    now = datetime(2026, 8, 24, 15, 30, 26, tzinfo=timezone.utc)
    stale_bar = pd.Timestamp("2026-08-24T10:09:00Z")
    calls = []

    class _Result:
        @staticmethod
        def to_df():
            return pd.DataFrame({"ts_event": [stale_bar], "close": [648.0]})

    class _Timeseries:
        @staticmethod
        def get_range(**kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                raise RuntimeError(
                    "422 data_end_after_available_end\n"
                    "The dataset EQUS.MINI has data available up to "
                    "'2026-08-24 10:10:00+00:00'."
                )
            return _Result()

    class _Historical:
        def __init__(self, _api_key):
            self.timeseries = _Timeseries()

    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    monkeypatch.setattr(underlying_validator, "_utcnow", lambda: now)
    monkeypatch.setattr(underlying_validator.db, "Historical", _Historical)

    result = underlying_validator._latest_proxy_bar("SPY")

    assert len(calls) == 2
    assert result["underlying_validation_status"] == "unavailable"
    assert result["underlying_proxy_symbol"] == "SPY"
    assert "stale" in result["underlying_validation_reason"]


def test_available_end_parser_rejects_unrelated_or_malformed_errors():
    requested = "2026-08-24T15:30:26Z"

    assert (
        underlying_validator._available_end_from_error(
            RuntimeError("503 gateway unavailable"), requested
        )
        is None
    )
    assert (
        underlying_validator._available_end_from_error(
            RuntimeError(
                "422 data_end_after_available_end; available up to 'not-a-timestamp'"
            ),
            requested,
        )
        is None
    )


def test_unavailable_refresh_reason_is_cached(monkeypatch):
    monkeypatch.setattr(
        underlying_validator,
        "_latest_proxy_bar",
        lambda proxy: underlying_validator._unavailable(proxy, "provider cutoff is stale"),
    )
    underlying_validator._latest.clear()
    underlying_validator._latest_checked.clear()
    underlying_validator._refreshing.clear()
    underlying_validator._structural_denials.clear()

    cache_key = underlying_validator._validation_request_identity("SPY")
    underlying_validator._refresh_validation(cache_key, cache_key[:3], "SPY")

    cached = underlying_validator.validate_underlying("SPX", 5000.0)
    assert cached["underlying_validation_status"] == "unavailable"
    assert cached["underlying_proxy_symbol"] == "SPY"
    assert cached["underlying_validation_reason"] == "provider cutoff is stale"


def test_structural_license_denial_backs_off_equivalent_proxy_requests(monkeypatch):
    clock = {"now": datetime(2026, 9, 8, 14, 38, tzinfo=timezone.utc)}
    calls = []

    class _ImmediateThread:
        def __init__(self, *, target, args, **_kwargs):
            self.target = target
            self.args = args

        def start(self):
            self.target(*self.args)

    def license_denied(proxy):
        calls.append(proxy)
        return underlying_validator._unavailable(
            proxy,
            "403 license_not_found_unauthorized\n"
            "A live data license is required to access EQUS.MINI data.",
        )

    monkeypatch.setenv("DATABENTO_API_KEY", "unlicensed-test-key")
    monkeypatch.setattr(underlying_validator, "_utcnow", lambda: clock["now"])
    monkeypatch.setattr(underlying_validator, "_latest_proxy_bar", license_denied)
    monkeypatch.setattr(underlying_validator.threading, "Thread", _ImmediateThread)
    underlying_validator._latest.clear()
    underlying_validator._latest_checked.clear()
    underlying_validator._refreshing.clear()
    underlying_validator._structural_denials.clear()

    underlying_validator.validate_underlying("SPX", 7500.0)
    spx = underlying_validator.validate_underlying("SPY", 750.0)
    qqq = underlying_validator.validate_underlying("NDX", 25000.0)

    assert calls == ["SPY"]
    assert spx["underlying_validation_status"] == "unavailable"
    assert spx["underlying_price"] is None
    assert spx["underlying_validation_failure_class"] == (
        "structural_license_denial"
    )
    assert qqq["underlying_validation_status"] == "unavailable"
    assert qqq["underlying_proxy_symbol"] == "QQQ"
    assert qqq["underlying_validation_backoff_seconds"] == 1_800.0
    assert qqq["underlying_validation_retry_after_utc"] == (
        "2026-09-08T15:08:00Z"
    )

    clock["now"] += timedelta(seconds=1_799)
    underlying_validator.validate_underlying("QQQ", 600.0)
    assert calls == ["SPY"]

    clock["now"] += timedelta(seconds=1)
    underlying_validator.validate_underlying("QQQ", 600.0)
    assert calls == ["SPY", "QQQ"]


def test_structural_license_classifier_does_not_backoff_unrelated_403():
    assert underlying_validator._is_structural_license_denial(
        "403 license_not_found_unauthorized"
    )
    assert not underlying_validator._is_structural_license_denial(
        "403 forbidden for one symbol mapping"
    )
