"""Exercise the dashboard's actual backtest guards without importing the app."""

import ast
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"
BLOCKED_CONNECTIONS = [
    ("databento", ""),
    ("databento", "synthetic-legacy-key"),
    ("polygon", ""),
    ("polygon", " \t\n "),
    ("unknown", "synthetic-legacy-key"),
]


@pytest.fixture(scope="module")
def app_tree():
    return ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))


def _assigns(node, name):
    return isinstance(node, ast.Assign) and any(
        isinstance(target, ast.Name) and target.id == name for target in node.targets
    )


def _sidebar_block(tree):
    for parent in ast.walk(tree):
        for _field, statements in ast.iter_fields(parent):
            if not isinstance(statements, list):
                continue
            starts = [
                index for index, node in enumerate(statements)
                if _assigns(node, "backtest_available")
            ]
            if not starts:
                continue
            assert len(starts) == 1
            start = starts[0]
            stop = next(
                index for index in range(start + 1, len(statements))
                if isinstance(statements[index], ast.If)
                and isinstance(statements[index].test, ast.Name)
                and statements[index].test.id == "run_backtest_btn"
            )
            return ast.Module(body=statements[start:stop + 1], type_ignores=[])
    raise AssertionError("Actual sidebar backtest availability block was not found")


def _execution_block(tree):
    candidates = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and {"run_backtest_btn", "run_backtest_analysis"}.issubset({
            child.id for child in ast.walk(node.test) if isinstance(child, ast.Name)
        })
    ]
    assert len(candidates) == 1
    return candidates[0]


class SidebarUI:
    def __init__(self, provider, api_key):
        self.session_state = SimpleNamespace(data_provider=provider, api_key=api_key)
        self.checkboxes = []
        self.messages = []
        self.configuration_calls = []

    def checkbox(self, label, **kwargs):
        self.checkboxes.append((label, kwargs))
        # Simulate Streamlit retaining a previously enabled widget value even
        # after the connection changes and the control becomes disabled.
        return True

    def info(self, message):
        self.messages.append(message)

    def caption(self, message):
        self.messages.append(message)

    def subheader(self, label):
        self.configuration_calls.append(("subheader", label))

    def columns(self, count):
        self.configuration_calls.append(("columns", count))
        return [nullcontext() for _ in range(count)]

    def date_input(self, label, **kwargs):
        self.configuration_calls.append(("date_input", label))
        return kwargs["value"]

    def selectbox(self, label, choices, **_kwargs):
        self.configuration_calls.append(("selectbox", label))
        return choices[0]

    def button(self, label, **_kwargs):
        self.configuration_calls.append(("button", label))
        return True


def _run_sidebar(tree, provider, api_key):
    ui = SidebarUI(provider, api_key)
    namespace = {"st": ui, "datetime": datetime, "timedelta": timedelta}
    exec(compile(_sidebar_block(tree), str(APP_PATH), "exec"), namespace)
    return ui, namespace


@pytest.mark.parametrize("provider,api_key", BLOCKED_CONNECTIONS)
def test_sidebar_disables_legacy_backtest_despite_retained_true_widget(
    app_tree, provider, api_key,
):
    ui, namespace = _run_sidebar(app_tree, provider, api_key)

    assert len(ui.checkboxes) == 1
    assert ui.checkboxes[0][1]["disabled"] is True
    assert namespace["backtest_available"] is False
    assert namespace["run_backtest_btn"] is False
    assert ui.configuration_calls == []
    assert "run_backtest_analysis" not in namespace
    assert "backtest_start" not in namespace
    assert ui.messages
    if provider == "databento":
        assert any("unavailable" in message and "Databento" in message for message in ui.messages)


def test_sidebar_keeps_explicitly_configured_legacy_mode_available(app_tree):
    ui, namespace = _run_sidebar(app_tree, "polygon", " synthetic-legacy-key ")

    assert ui.checkboxes[0][1]["disabled"] is False
    assert namespace["backtest_available"] is True
    assert namespace["run_backtest_btn"] is True
    assert namespace["run_backtest_analysis"] is True
    assert [name for name, _ in ui.configuration_calls].count("date_input") == 2
    assert any(name == "button" for name, _ in ui.configuration_calls)


class NoResultsUI:
    def __init__(self, provider, api_key):
        self.session_state = SimpleNamespace(data_provider=provider, api_key=api_key)
        self.render_calls = []

    def __getattr__(self, name):
        def forbidden(*_args, **_kwargs):
            self.render_calls.append(name)
            raise AssertionError("Blocked connection entered backtest result rendering")
        return forbidden


def _execution_namespace(provider, api_key):
    ui = NoResultsUI(provider, api_key)
    requests = []

    def forbidden_request(*args, **kwargs):
        requests.append((args, kwargs))
        raise AssertionError("Blocked connection entered the legacy provider request")

    return {
        "st": ui,
        "backtest_available": True,
        "run_backtest_btn": True,
        "run_backtest_analysis": True,
        "selected_indexes": ["S&P 500 (SPX)"],
        "backtest_start": date(2026, 9, 1),
        "backtest_end": date(2026, 9, 9),
        "backtest_model": "linear_regression",
        "run_backtest": forbidden_request,
    }, requests


@pytest.mark.parametrize("provider,api_key", BLOCKED_CONNECTIONS)
def test_execution_independently_blocks_stale_true_flags_without_requests_or_results(
    app_tree, provider, api_key,
):
    namespace, requests = _execution_namespace(provider, api_key)
    block = ast.Module(body=[_execution_block(app_tree)], type_ignores=[])

    exec(compile(block, str(APP_PATH), "exec"), namespace)

    assert requests == []
    assert namespace["st"].render_calls == []
    assert "all_backtest_results" not in namespace
    assert not hasattr(namespace["st"].session_state, "backtest_results")


def test_execution_condition_accepts_configured_legacy_positive_control(app_tree):
    namespace, requests = _execution_namespace("polygon", " synthetic-legacy-key ")
    actual_condition = ast.Expression(body=_execution_block(app_tree).test)

    assert bool(eval(compile(actual_condition, str(APP_PATH), "eval"), namespace)) is True
    assert requests == []
    assert namespace["st"].render_calls == []
