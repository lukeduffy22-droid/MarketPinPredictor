import ast
import math
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace


def _top_level_literal_dict(module: ast.Module, name: str) -> dict[str, str]:
    assignment = next(
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
    )
    return ast.literal_eval(assignment.value)


def _run_frozen_gamma_tab(*, historical_choice=None):
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    block = next(
        node for node in ast.walk(module)
        if isinstance(node, ast.With)
        and any(
            isinstance(item.context_expr, ast.Name)
            and item.context_expr.id == "frozen_gamma_tab"
            for item in node.items
        )
    )
    loaded = []
    checkbox_calls = []

    def checkbox(label, *, value, key, **_kwargs):
        checkbox_calls.append((label, value, key))
        return value if historical_choice is None else historical_choice

    def load_snapshot(symbol):
        loaded.append(symbol)
        return None

    no_op = lambda *_args, **_kwargs: None
    st = SimpleNamespace(
        markdown=no_op, info=no_op, caption=no_op, warning=no_op,
        subheader=no_op, divider=no_op, checkbox=checkbox,
    )
    namespace = {
        "st": st,
        "frozen_gamma_tab": nullcontext(),
        "get_freeze_status": lambda: (False, None),
        "selected_indexes": ["S&P 500 (SPX)"],
        "INDEXES": {"S&P 500 (SPX)": "SPX"},
        "load_last_valid_snapshot": load_snapshot,
        "_current_snapshot_display_date": lambda: "2026-09-08",
        "DISPLAY_TIMEZONE": SimpleNamespace(name="America/Chicago"),
        "_render_eod_zip_download": no_op,
    }
    exec(compile(ast.Module(body=[block], type_ignores=[]), str(app_path), "exec"), namespace)
    return loaded, checkbox_calls


def test_frozen_gamma_does_not_read_historical_audit_by_default():
    loaded, controls = _run_frozen_gamma_tab()

    assert loaded == []
    assert controls == [("Load historical audit", False, "load_historical_audit")]


def test_frozen_gamma_loads_existing_validated_reader_only_when_requested():
    loaded, _controls = _run_frozen_gamma_tab(historical_choice=True)
    assert loaded == ["SPX"]

    loaded, _controls = _run_frozen_gamma_tab(historical_choice=False)
    assert loaded == []


def _load_positioning_helpers():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    helper_names = {"_expiration_scope_label", "_structural_distance_from_spot"}
    helpers = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in helper_names
    ]
    assert {node.name for node in helpers} == helper_names
    namespace = {
        "Any": object,
        "_is_number": lambda value: (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
        ),
    }
    exec(
        compile(ast.Module(body=helpers, type_ignores=[]), str(app_path), "exec"),
        namespace,
    )
    return namespace


def test_every_databento_label_resolves_through_shared_index_registry():
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    shared = _top_level_literal_dict(module, "INDEXES")
    databento = _top_level_literal_dict(module, "DATABENTO_INDEXES")

    has_registry_merge = any(
        isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and isinstance(node.value.func.value, ast.Name)
        and node.value.func.value.id == "INDEXES"
        and node.value.func.attr == "update"
        and len(node.value.args) == 1
        and isinstance(node.value.args[0], ast.Name)
        and node.value.args[0].id == "DATABENTO_INDEXES"
        for node in module.body
    )
    if has_registry_merge:
        shared.update(databento)

    assert set(databento).issubset(shared)


def test_databento_live_ui_does_not_offer_a_fake_history_slider():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8"
    )

    assert 'st.slider(\n        "Historical Data (days)"' not in app_source
    assert "Historical lookback is not a live Databento control" in app_source


def test_app_never_fabricates_option_levels_from_spot_when_gamma_is_missing():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8"
    )

    assert "'pin_expiry': datetime.now()" not in app_source
    assert "'zero_gamma': spot_price" not in app_source
    assert "'pin_strike': spot_price" not in app_source
    assert "'data_unavailable': True" in app_source


def test_app_exposes_exact_expiration_dates_and_context_disclaimer():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8"
    )

    assert "Expiration structure (exact dates)" in app_source
    assert "Tomorrow, max-pain, and other cross-expiration values" in app_source
    assert "ZERO_GAMMA_DISPLAY_LABEL" in app_source


def test_positioning_helpers_label_primary_and_shadow_dates_without_direction():
    helpers = _load_positioning_helpers()

    assert helpers["_expiration_scope_label"](
        "2026-09-08", is_primary=True
    ) == "PRIMARY expiration 2026-09-08"
    assert helpers["_expiration_scope_label"](
        "2026-09-16", is_primary=True, context_only=True
    ) == (
        "PRIMARY forward-context expiration 2026-09-16 "
        "— not 0DTE authority"
    )
    assert helpers["_expiration_scope_label"](
        "2026-09-11", is_primary=False
    ) == "SHADOW expiration 2026-09-11 — analytical context only"
    above = helpers["_structural_distance_from_spot"](6520.0, 6500.0)
    below = helpers["_structural_distance_from_spot"](6480.0, 6500.0)

    assert above == "0.31% above spot; structural distance only, not a price forecast."
    assert below == "0.31% below spot; structural distance only, not a price forecast."
    assert not any(symbol in above + below for symbol in ("⬆️", "⬇️", "↔️"))


def test_databento_positioning_panels_do_not_render_levels_as_directional_targets():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8"
    )
    positioning = app_source.split(
        "# Display non-directional options-positioning structure if available.", 1
    )[1].split(
        "# Verified lifecycle/promoted close forecast remains a separate surface.", 1
    )[0]
    gamma_summary = app_source.split(
        "# Main non-directional gamma and max-pain structure.", 1
    )[1].split("expiration_rows =", 1)[0]

    for panel in (positioning, gamma_summary):
        assert "primary_target" not in panel
        assert "max_pain_direction" not in panel
        assert "pull_strength" not in panel
        assert "gex['direction']" not in panel
        assert "⬆️" not in panel
        assert "⬇️" not in panel
        assert "↔️" not in panel
    assert "Options Positioning Levels — Non-Directional Structure" in positioning
    assert "Shadow expiration structure (exact dates)" in positioning
    assert "_expiration_scope_label" in positioning


def test_current_price_button_uses_lifecycle_state_instead_of_raw_buffer_values():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8"
    )

    assert 'requests.get(f"http://localhost:8000/buffer/latest/{ticker}"' not in app_source
    assert "price_batch = fetch_sidebar_symbol_states(" in app_source
    assert "if not state.usable:" in app_source


def test_invalid_symbol_evidence_is_rendered_in_a_separate_non_prediction_section():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(
        encoding="utf-8"
    )

    assert "Excluded symbol diagnostics — not predictions" in app_source
    assert "_render_sidebar_diagnostic_evidence(state)" in app_source
    assert "excluded from predictions, AI analysis" in app_source
    assert "same final lifecycle check used for the " in app_source
    assert "final_lifecycle_batch.results if final_lifecycle_batch is not None" in app_source
    assert "zero placeholders " in app_source
    assert "are intentionally hidden" in app_source
