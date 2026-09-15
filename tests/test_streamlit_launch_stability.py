from pathlib import Path
import re


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_all_production_dashboard_launches_disable_source_watching():
    for relative_path in ("start_databento_app.ps1", "ensure_market_app.ps1"):
        source = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
        assert re.search(
            r"['\"]--server\.fileWatcherType['\"]\s*,\s*['\"]none['\"]",
            source,
        ), relative_path


def test_project_default_disables_source_watching_for_direct_streamlit_runs():
    config = (PROJECT_ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8")
    assert re.search(r"(?m)^\s*fileWatcherType\s*=\s*['\"]none['\"]\s*$", config)


def test_service_checker_routes_recovery_through_guarded_full_stack_launcher():
    source = (PROJECT_ROOT / "check_services.py").read_text(encoding="utf-8")
    assert "start_databento_app.ps1" in source
    assert "streamlit run" not in source
    assert "python -m backend.app" not in source
    assert "python live_gamma_agent.py" not in source
