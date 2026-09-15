import ast
from contextlib import nullcontext
from datetime import datetime
from io import BytesIO, StringIO
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
import zipfile

import pandas as pd
import pytest

from app.utils.snapshot_history import (
    RESEARCH_RECORDED_SUBSCRIPTION_IDENTITY,
    DIAGNOSTIC_INVALID_SNAPSHOT,
    HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY,
    gamma_snapshot_provenance_status,
    partition_snapshot_evidence,
    snapshot_research_export_fields,
)
from backend.workstation import payload_has_fallback_provenance


def _load_export_to_csv():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "export_to_csv"
    )
    namespace = {
        "datetime": datetime,
        "pd": pd,
        "ZERO_GAMMA_DISPLAY_LABEL": "First strike-bucket GEX sign crossing",
        "payload_has_fallback_provenance": payload_has_fallback_provenance,
    }
    exec(
        compile(ast.Module(body=[function], type_ignores=[]), str(app_path), "exec"),
        namespace,
    )
    return namespace["export_to_csv"]


def _observed_indicator_frame():
    return pd.DataFrame(
        {
            "timestamp": ["2026-09-04T19:00:00Z"],
            "RSI": [50.0],
            "MACD": [1.0],
            "Signal_Line": [0.5],
            "SMA_20": [6500.0],
            "Momentum": [2.0],
            "VWAP": [6501.0],
            "AMA": [6502.0],
        }
    )


def _load_snapshot_export_functions(records):
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    names = {"load_daily_pin_history", "create_eod_zip_export"}
    functions = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    viewer_timezone = SimpleNamespace(name="UTC")
    selection = SimpleNamespace(records=tuple(records))
    namespace = {
        "Optional": Optional,
        "DisplayTimezone": object,
        "pd": pd,
        "json": json,
        "DISPLAY_TIMEZONE": viewer_timezone,
        "DIAGNOSTIC_INVALID_SNAPSHOT": DIAGNOSTIC_INVALID_SNAPSHOT,
        "HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY": (
            HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY
        ),
        "gamma_snapshot_provenance_status": gamma_snapshot_provenance_status,
        "partition_snapshot_evidence": partition_snapshot_evidence,
        "snapshot_research_export_fields": snapshot_research_export_fields,
        "current_local_date": lambda _timezone: "2026-09-04",
        "list_gamma_snapshot_symbols": lambda _root: ["SPX"],
        "_cached_local_snapshot_selection": lambda *_args: selection,
        "_snapshot_timestamp_value": lambda snap: snap.get("generated_at_utc"),
        "format_display_timestamp": lambda value, *_args, **_kwargs: value,
        "_is_number": lambda value: isinstance(value, (int, float)),
        "_fmt_gex_units": lambda value, _digits: str(value),
    }
    exec(
        compile(ast.Module(body=functions, type_ignores=[]), str(app_path), "exec"),
        namespace,
    )
    return namespace, viewer_timezone


def _snapshot_records():
    common = {
        "symbol": "SPX",
        "generated_at_utc": "2026-09-04T19:00:00Z",
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "gamma_pin_strike": 6500.0,
        "spot_last": 6501.0,
        "gross_gex": 1000.0,
        "net_gex": 100.0,
        "call_gex_total": 600.0,
        "put_gex_total": -500.0,
    }
    return [
        {
            **common,
            "id": "current",
            "subscription_epoch_id": "a" * 64,
            "subscription_generation": 2,
        },
        {
            **common,
            "id": "legacy",
            "generated_at_utc": "2026-09-04T19:01:00Z",
            "subscription_generation": 1,
        },
        {
            **common,
            "id": "invalid",
            "generated_at_utc": "2026-09-04T19:02:00Z",
            "validation_is_valid": False,
        },
    ]


def test_full_export_handles_partial_indicator_availability_without_mixed_types():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "current_price": 6500.0,
            "predicted_price": 6510.0,
            "df": _observed_indicator_frame(),
            "indicators_available": True,
            "indicator_provenance": {
                "is_observed": True,
                "is_synthetic": False,
                "required_source_kind": "observed_intraday_ohlcv",
            },
        },
        "NASDAQ 100": {
            "ticker": "NDX",
            "current_price": 23000.0,
            "predicted_price": 23025.0,
            "df": pd.DataFrame(),
            "indicators_available": False,
            "indicator_provenance": {
                "is_observed": False,
                "is_synthetic": False,
            },
        },
    }

    exported = pd.read_csv(StringIO(export_to_csv(predictions, include_indicators=True)))

    assert len(exported) == 2
    assert set(exported["record_type"].dropna()) == {"observed_indicator_frame"}
    assert set(exported["Record Type"].dropna()) == {"prediction_record"}


def test_full_export_rejects_unprovenanced_retained_indicator_frame():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "current_price": 6500.0,
            "predicted_price": 6510.0,
            "df": _observed_indicator_frame(),
        }
    }

    exported = pd.read_csv(StringIO(export_to_csv(predictions, include_indicators=True)))

    assert exported.loc[0, "Record Type"] == "prediction_record"
    assert not bool(exported.loc[0, "Indicators Available"])


def test_prediction_export_omits_invalid_databento_diagnostic_rows():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "provider": "databento",
            "current_price": 0.0,
            "predicted_price": 0.0,
            "forecast_state": "ABSTAIN",
            "pin_payload": {
                "provider": "databento",
                "price": 0.0,
                "gamma_pin": 0.0,
                "validation_is_valid": False,
                "gamma_excluded_from_model": True,
            },
        }
    }

    assert export_to_csv(predictions, include_indicators=False) is None


def test_prediction_export_rejects_top_level_historical_fallback():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "provider": "historical-fallback",
            "current_price": 6500.0,
            "predicted_price": 6510.0,
            "historical_context_only": True,
        }
    }

    assert export_to_csv(predictions, include_indicators=False) is None


def test_prediction_export_rejects_nested_fallback_provenance():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "provider": "databento",
            "current_price": 6500.0,
            "predicted_price": 6510.0,
            "pin_payload": {
                "provider": "databento",
                "validation_is_valid": True,
                "gamma_excluded_from_model": False,
                "universe_provenance": {"is_fallback": True},
            },
        }
    }

    assert export_to_csv(predictions, include_indicators=False) is None


def test_prediction_export_rejects_zero_numeric_shell_even_if_marked_valid():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "provider": "databento",
            "current_price": 0.0,
            "predicted_price": 0.0,
            "pin_payload": {
                "provider": "databento",
                "validation_is_valid": True,
                "gamma_excluded_from_model": False,
            },
        }
    }

    assert export_to_csv(predictions, include_indicators=False) is None


def test_prediction_export_includes_canonical_databento_process_identity():
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "provider": "databento",
            "current_price": 6500.0,
            "predicted_price": 6510.0,
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
            "subscription_epoch_id": "a" * 64,
            "subscription_generation": 2,
        }
    }

    exported = pd.read_csv(StringIO(export_to_csv(predictions, include_indicators=False)))

    assert exported.loc[0, "Subscription Epoch ID"] == "a" * 64
    assert exported.loc[0, "Subscription Generation"] == 2


@pytest.mark.parametrize(
    ("epoch_id", "generation"),
    [
        (None, 1),
        ("A" * 64, 1),
        ("a" * 63, 1),
        ("a" * 64, None),
        ("a" * 64, 0),
        ("a" * 64, True),
    ],
)
def test_prediction_export_rejects_databento_without_canonical_process_identity(
    epoch_id,
    generation,
):
    export_to_csv = _load_export_to_csv()
    predictions = {
        "S&P 500": {
            "ticker": "SPX",
            "provider": "databento",
            "current_price": 6500.0,
            "predicted_price": 6510.0,
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
            "subscription_epoch_id": epoch_id,
            "subscription_generation": generation,
        }
    }

    assert export_to_csv(predictions, include_indicators=False) is None


def test_daily_pin_history_retains_legacy_identity_as_unverified_context():
    namespace, viewer_timezone = _load_snapshot_export_functions(_snapshot_records())

    history = namespace["load_daily_pin_history"](
        "SPX", "2026-09-04", viewer_timezone
    )

    assert len(history) == 2
    assert history["_subscription_epoch_id"].tolist() == ["a" * 64, None]
    assert history["_subscription_generation"].tolist() == [2, 1]
    assert history["_provenance_status"].tolist() == [
        RESEARCH_RECORDED_SUBSCRIPTION_IDENTITY,
        HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY,
    ]
    assert history["_current_live_eligible"].tolist() == [False, False]


def test_eod_zip_separates_recorded_and_legacy_process_identity_as_research():
    namespace, viewer_timezone = _load_snapshot_export_functions(_snapshot_records())

    archive_bytes = namespace["create_eod_zip_export"](
        "2026-09-04", viewer_timezone
    )

    with zipfile.ZipFile(BytesIO(archive_bytes)) as archive:
        assert "SPX_2026-09-04.ndjson" not in archive.namelist()
        with zipfile.ZipFile(BytesIO(archive.read("SPX_2026-09-04.validation-groups.zip"))) as grouped:
            rows = [json.loads(line) for name in grouped.namelist()
                    if name.endswith('.ndjson')
                    for line in grouped.read(name).decode('utf-8').splitlines()]
        assert len(rows) == 1
        assert rows[0]['review_validation_method'] == 'METHOD_UNRECORDED'
        assert rows[0]['review_revalidated'] is False
        historical_name = "SPX_2026-09-04.historical_unverified.ndjson"
        assert historical_name in archive.namelist()
        historical = json.loads(archive.read(historical_name).decode("utf-8"))
        assert historical["id"] == "legacy"
        assert "subscription_epoch_id" not in historical
        assert historical["subscription_generation"] == 1
        assert historical["export_provenance_status"] == (
            HISTORICAL_UNVERIFIED_SUBSCRIPTION_IDENTITY
        )
        csv_names = [name for name in archive.namelist()
                     if name.endswith('/SPX_pin_history_2026-09-04.csv')]
        assert csv_names
        frames = [pd.read_csv(BytesIO(archive.read(name))) for name in csv_names]
        assert all(frame['review_validation_group'].nunique() == 1 for frame in frames)
        history_csv = pd.concat(frames, ignore_index=True)
    assert history_csv["subscription_generation"].tolist() == [2, 1]
    assert history_csv["current_live_eligible"].tolist() == [False, False]


def _load_zip_download_renderer():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    function = next(
        node for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "_render_eod_zip_download"
    )
    observed = {"clicked": False, "scans": 0, "builds": [], "downloads": [], "errors": []}

    def list_symbols(_root):
        observed["scans"] += 1
        return ["SPX"]

    def build_archive(day, timezone):
        observed["builds"].append((day, timezone.name))
        return f"archive:{day}:{timezone.name}".encode()

    st = SimpleNamespace(
        session_state={},
        button=lambda *_args, **_kwargs: observed["clicked"],
        spinner=lambda *_args: nullcontext(),
        caption=lambda *_args: None,
        info=lambda *_args: None,
        error=lambda message: observed["errors"].append(message),
        download_button=lambda *args, **kwargs: observed["downloads"].append((args, kwargs)),
    )
    namespace = {
        "DisplayTimezone": object,
        "st": st,
        "list_gamma_snapshot_symbols": list_symbols,
        "_cached_local_snapshot_selection": lambda *_args: SimpleNamespace(records=({},)),
        "gamma_snapshot_provenance_status": lambda _record: "research",
        "DIAGNOSTIC_INVALID_SNAPSHOT": DIAGNOSTIC_INVALID_SNAPSHOT,
        "create_eod_zip_export": build_archive,
        "utc_iso": lambda: "2026-09-08T19:25:00Z",
        "format_display_timestamp": lambda value, _timezone: value,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(app_path), "exec"), namespace)
    return namespace, observed, st


def test_research_zip_only_builds_on_request_and_reuses_prepared_bytes():
    namespace, observed, _st = _load_zip_download_renderer()
    render = namespace["_render_eod_zip_download"]
    timezone = SimpleNamespace(name="America/Chicago")

    render("2026-09-08", timezone, key="frozen")
    assert observed["scans"] == 0
    assert observed["builds"] == []
    assert observed["downloads"] == []

    observed["clicked"] = True
    render("2026-09-08", timezone, key="frozen")
    observed["clicked"] = False
    render("2026-09-08", timezone, key="exports")

    assert observed["scans"] == 1
    assert observed["builds"] == [("2026-09-08", "America/Chicago")]
    assert len(observed["downloads"]) == 2
    for args, kwargs in observed["downloads"]:
        assert args[1] == b"archive:2026-09-08:America/Chicago"
        assert args[2] == "gamma_data_2026-09-08.zip"
        assert kwargs["on_click"] == "ignore"


@pytest.mark.parametrize(
    "new_date,new_timezone",
    [("2026-09-07", "America/Chicago"), ("2026-09-08", "UTC")],
)
def test_research_zip_never_offers_bytes_for_another_date_or_timezone(new_date, new_timezone):
    namespace, observed, _st = _load_zip_download_renderer()
    render = namespace["_render_eod_zip_download"]
    observed["clicked"] = True
    render("2026-09-08", SimpleNamespace(name="America/Chicago"), key="exports")
    observed["clicked"] = False
    observed["downloads"].clear()

    render(new_date, SimpleNamespace(name=new_timezone), key="exports")
    assert observed["downloads"] == []
    assert len(observed["builds"]) == 1

    observed["clicked"] = True
    render(new_date, SimpleNamespace(name=new_timezone), key="exports")
    assert observed["builds"][-1] == (new_date, new_timezone)
    assert observed["downloads"][-1][0][1] == f"archive:{new_date}:{new_timezone}".encode()


def test_failed_research_zip_rebuild_does_not_offer_old_archive():
    namespace, observed, st = _load_zip_download_renderer()
    render = namespace["_render_eod_zip_download"]
    timezone = SimpleNamespace(name="UTC")
    observed["clicked"] = True
    render("2026-09-08", timezone, key="exports")
    observed["downloads"].clear()

    def fail_build(*_args):
        raise OSError("read failed")

    namespace["create_eod_zip_export"] = fail_build
    render("2026-09-08", timezone, key="exports")
    observed["clicked"] = False
    render("2026-09-08", timezone, key="exports")
    assert observed["downloads"] == []
    assert "_prepared_gamma_research_zip" not in st.session_state
    assert observed["errors"] == ["Error creating ZIP: read failed"]


def test_research_zip_without_calculation_valid_records_stays_unavailable():
    namespace, observed, st = _load_zip_download_renderer()
    namespace["gamma_snapshot_provenance_status"] = lambda _record: DIAGNOSTIC_INVALID_SNAPSHOT
    observed["clicked"] = True

    namespace["_render_eod_zip_download"](
        "2026-09-08", SimpleNamespace(name="UTC"), key="exports",
    )

    assert observed["scans"] == 1
    assert observed["builds"] == []
    assert observed["downloads"] == []
    assert "_prepared_gamma_research_zip" not in st.session_state
