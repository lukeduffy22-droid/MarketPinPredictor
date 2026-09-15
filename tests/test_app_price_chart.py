import ast
from datetime import timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def _load_create_price_chart():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "create_price_chart"
    )
    namespace = {
        "go": go,
        "make_subplots": make_subplots,
        "timedelta": timedelta,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(app_path), "exec"), namespace)
    return namespace["create_price_chart"]


def _load_gex_formatter():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "_fmt_gex_units"
    )
    namespace = {"Any": object}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(app_path), "exec"), namespace)
    return namespace["_fmt_gex_units"]


def test_create_price_chart_accepts_backend_frame_without_bollinger_columns():
    backend_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-07-22 14:00", periods=3, freq="min"),
            "open": [100.0, 100.5, 101.0],
            "high": [101.0, 101.5, 102.0],
            "low": [99.0, 99.5, 100.0],
            "close": [100.5, 101.0, 101.5],
            "SMA_20": [100.5, 100.75, 101.0],
            "RSI": [50.0, 55.0, 60.0],
            "MACD": [0.0, 0.2, 0.4],
            "Signal_Line": [0.0, 0.1, 0.25],
        }
    )

    figure = _load_create_price_chart()(backend_df, 102.0, "SPX")

    trace_names = {trace.name for trace in figure.data}
    assert "Price" in trace_names
    assert "Prediction" in trace_names
    assert "BB Upper" not in trace_names
    assert "BB Lower" not in trace_names


def test_gex_formatter_labels_raw_units_without_dollar_claim():
    formatter = _load_gex_formatter()

    assert formatter(1_250_000, 2) == "1.25M raw units"
    assert formatter(-5_000, 1) == "-5.0K raw units"
    assert "$" not in formatter(1_000_000_000)
