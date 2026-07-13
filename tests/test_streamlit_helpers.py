"""Regression tests for extracted Streamlit helper modules."""

from datetime import datetime, timedelta

import pandas as pd

from app.features.technical_indicators import calculate_technical_indicators
from app.services.streamlit_predictions import predict_eod_price
from app.visualization.price_chart import create_price_chart


def _sample_ohlcv(rows: int = 30) -> pd.DataFrame:
    base = datetime(2025, 1, 1)
    return pd.DataFrame({
        "timestamp": [base + timedelta(days=i) for i in range(rows)],
        "open": [100 + i for i in range(rows)],
        "high": [101 + i for i in range(rows)],
        "low": [99 + i for i in range(rows)],
        "close": [100 + i for i in range(rows)],
        "volume": [1000 + i for i in range(rows)],
    })


def test_calculate_technical_indicators_adds_expected_columns():
    df = calculate_technical_indicators(_sample_ohlcv())

    expected = {
        "SMA_5", "SMA_10", "SMA_20", "EMA_5", "EMA_10",
        "RSI", "MACD", "Signal_Line", "Momentum", "ROC",
        "Volume_Ratio", "BB_Upper", "BB_Lower", "VWAP", "AMA",
    }

    assert expected.issubset(df.columns)
    assert not df[list(expected)].isna().all().any()


def test_predict_eod_price_handles_insufficient_data():
    predicted, confidence, df_clean, current_price, error = predict_eod_price(_sample_ohlcv(10))

    assert predicted is None
    assert confidence is None
    assert df_clean is None
    assert current_price is None
    assert error == "Initial data check failed (need 25+ rows)"


def test_predict_eod_price_returns_prediction_without_tensorflow():
    predicted, confidence, df_clean, current_price, error = predict_eod_price(_sample_ohlcv(60))

    assert error is None
    assert predicted is not None
    assert confidence is not None
    assert df_clean is not None
    assert current_price == 159


def test_predict_eod_price_supports_ensemble_with_gamma_pin():
    predicted, confidence, _, current_price, error = predict_eod_price(
        _sample_ohlcv(60),
        model_type="Ensemble",
        gex_data={"pin_strike": 158.5, "pull_strength": 75},
    )

    assert error is None
    assert predicted is not None
    assert confidence >= 40
    assert abs(predicted - current_price) / current_price < 0.02


def test_predict_eod_price_ignores_invalid_gamma_pin():
    predicted, confidence, _, current_price, error = predict_eod_price(
        _sample_ohlcv(60),
        gex_data={"pin_strike": "not-a-number", "pull_strength": "strong"},
    )

    assert error is None
    assert predicted is not None
    assert confidence is not None
    assert current_price == 159


def test_create_price_chart_returns_three_panel_figure():
    df = calculate_technical_indicators(_sample_ohlcv())

    fig = create_price_chart(df, predicted_price=130.0, ticker_name="Test Index")

    assert len(fig.data) >= 7
    assert fig.layout.height == 800
    assert "Test Index" in fig.layout.annotations[0].text
