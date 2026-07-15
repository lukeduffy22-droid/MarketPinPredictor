"""Tests for deprecated Streamlit entrypoint guards."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_script(path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(path)],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_app_new_entrypoint_is_disabled() -> None:
    result = _run_script(REPO_ROOT / "app_new.py")
    assert result.returncode != 0
    assert "app_new.py is deprecated and intentionally disabled" in result.stderr


def test_app_backup_entrypoint_is_disabled() -> None:
    result = _run_script(REPO_ROOT / "app_backup.py")
    assert result.returncode != 0
    assert "app_backup.py is deprecated and intentionally disabled" in result.stderr


def test_clean_app_entrypoint_is_disabled() -> None:
    path = REPO_ROOT / "clean_app" / "app.py"
    if not path.exists():
        return

    result = _run_script(path)
    assert result.returncode != 0
    assert "clean_app/app.py is deprecated and intentionally disabled" in result.stderr
