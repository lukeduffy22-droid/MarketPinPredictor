import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_frontend_has_no_active_polygon_imports():
    tree = ast.parse((PROJECT_ROOT / "app.py").read_text(encoding="utf-8"))
    imported_modules = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_modules.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )

    assert not any(name == "polygon" or name.startswith("polygon.") for name in imported_modules)
    assert "options_gamma" not in imported_modules
    assert "backtesting" not in imported_modules
    assert "websocket_streaming" not in imported_modules


def test_runtime_dependency_manifests_do_not_install_polygon_namespace():
    pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8").lower()
    requirements = (PROJECT_ROOT / "requirements_backend.txt").read_text(encoding="utf-8").lower()

    assert '"polygon' not in pyproject
    assert "polygon-api-client" not in requirements
