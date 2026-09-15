"""Execute the actual optional tab bodies with readers that record every call."""
import ast
from pathlib import Path


class Session(dict):
    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value


class UI:
    def __init__(self, *, analyze=False, debug=False):
        self.analyze = analyze
        self.debug = debug
        self.session_state = Session()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def button(self, label, **_):
        return label == "Analyze Live State" and self.analyze

    def checkbox(self, *_, **__):
        return self.debug

    def selectbox(self, _, choices, **__):
        return choices[0]

    def expander(self, *_, **__):
        return self

    def __getattr__(self, _):
        return lambda *a, **k: None


def execute_tab(tab, ui, reads):
    source = Path(__file__).resolve().parents[1] / "app.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.With)
             and any(isinstance(i.context_expr, ast.Name)
                     and i.context_expr.id == tab for i in n.items)]
    assert len(nodes) == 1
    def fetch(symbols):
        reads.append(symbols)
        return {"timestamp_utc": "2026-09-09T14:00:00Z"}
    namespace = {
        "st": ui, tab: ui, "selected_indexes": ["S&P"], "INDEXES": {"S&P": "SPX"},
        "fetch_advisor_context": fetch, "build_advisor_report": lambda _: {},
        "load_latest_snapshot_data": lambda symbol: reads.append(symbol),
        "dict_to_audit_snapshot": lambda _: None,
        "format_display_timestamp": lambda *a, **k: a[0], "DISPLAY_TIMEZONE": "UTC",
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)


def test_hidden_advisor_does_not_fetch_on_initial_load_or_retained_render(monkeypatch, tmp_path):
    from app.services import advisor_view

    ui, reads = UI(), []

    def fetch(symbols):
        reads.append(symbols)
        return {"timestamp_utc": "2026-09-09T14:00:00Z"}

    monkeypatch.setattr(advisor_view, "st", ui)
    monkeypatch.setattr(
        advisor_view,
        "inspect_capabilities",
        lambda: {"fingerprint": "test-source", "implemented": {}},
    )
    monkeypatch.setattr(advisor_view, "load_selection", lambda _: None)
    monkeypatch.setattr(advisor_view, "fetch_advisor_context", fetch)
    monkeypatch.setattr(advisor_view, "build_advisor_report", lambda *_: {})

    preferences_path = tmp_path / "advisor.json"
    advisor_view.render_advisor(["SPX"], preferences_path=preferences_path)
    assert reads == []
    ui.analyze = True
    advisor_view.render_advisor(["SPX"], preferences_path=preferences_path)
    assert reads == [["SPX"]]
    ui.analyze = False
    advisor_view.render_advisor(["SPX"], preferences_path=preferences_path)
    assert reads == [["SPX"]]


def test_debug_audit_only_reads_when_requested_and_stops_when_disabled():
    ui, reads = UI(), []
    execute_tab("debug_snapshot_tab", ui, reads)
    assert reads == []
    ui.debug = True
    execute_tab("debug_snapshot_tab", ui, reads)
    assert reads == ["SPX"]
    ui.debug = False
    execute_tab("debug_snapshot_tab", ui, reads)
    assert reads == ["SPX"]
