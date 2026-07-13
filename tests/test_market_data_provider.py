"""Regression tests for provider fallback and major-index options coverage."""

import asyncio

import pandas as pd

import export_all_indices
from app.ingest.options_websocket_stream import OptionsWebSocketStream, get_gamma_tracker
from app.services import streamlit_market_data as smd
from app.utils.settings import Settings


def _sample_market_df(source: str, ticker_used: str) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1, 2],
        }
    )
    df.attrs["data_source"] = source
    df.attrs["ticker_used"] = ticker_used
    return df


def _fail_if_called(*_args, **_kwargs):
    raise AssertionError("Polygon should not be used")


def test_settings_accept_polygon_api_key_alias(monkeypatch):
    monkeypatch.delenv("Massive_API", raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "polygon-from-env")

    settings = Settings()

    assert settings.polygon_api_key == "polygon-from-env"


def test_fetch_market_data_uses_databento_when_polygon_is_unset(monkeypatch):
    databento_df = _sample_market_df("databento_futures_proxy", "ES.c.0")

    monkeypatch.setattr(smd, "_resolve_market_data_provider", lambda *_args, **_kwargs: "databento")
    monkeypatch.setattr(smd, "_fetch_databento_market_data", lambda *_args, **_kwargs: databento_df)
    monkeypatch.setattr(smd, "_fetch_polygon_market_data", _fail_if_called)

    result = smd.fetch_market_data(
        api_key="",
        ticker="SPX",
        days=30,
        use_index=True,
        databento_api_key="db-key",
    )

    assert result is databento_df
    assert result.attrs["data_source"] == "databento_futures_proxy"
    assert result.attrs["ticker_used"] == "ES.c.0"


def test_options_websocket_tracks_dji_proxy_and_rut():
    stream = OptionsWebSocketStream()
    tracker = get_gamma_tracker()
    tracker.reset_daily()

    assert "T.O:DIA*" in stream.subscriptions
    assert "T.O:RUT*" in stream.subscriptions

    asyncio.run(
        stream._process_options_trade(
            {"sym": "O:DIA250118C00400000", "p": 1.0, "s": 2, "t": 1_725_000_000_000}
        )
    )
    asyncio.run(
        stream._process_options_trade(
            {"sym": "O:RUT250118P02100000", "p": 1.5, "s": 1, "t": 1_725_000_001_000}
        )
    )

    assert tracker.trade_counts["DJI"] == 1
    assert tracker.trade_counts["RUT"] == 1
    assert tracker.volume_by_strike["DJI"][400] == 200.0
    assert tracker.volume_by_strike["RUT"][2100] == 150.0


def test_export_all_indices_keeps_dji_rows():
    df = pd.DataFrame(
        {
            "symbol": ["DJI"],
            "generated_at_utc": ["2025-07-10T19:30:00Z"],
            "spot_last": [44000.0],
            "primary_gamma_pin_strike": [43950.0],
            "gross_gex": [1.2],
            "net_gex": [0.4],
            "call_gex": [0.7],
            "put_gex": [0.5],
            "validation_is_valid": [True],
        }
    )

    result = export_all_indices.extract_fields(df)

    assert list(result["symbol"]) == ["DJI"]
