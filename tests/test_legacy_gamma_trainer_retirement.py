from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import train_gamma_model


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_legacy_gamma_trainer_function_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="legacy CSV gamma trainer is retired"):
        train_gamma_model.train_model("legacy.csv", "SPX", "legacy.pt", "legacy.json")


def test_legacy_gamma_trainer_cli_fails_closed() -> None:
    result = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "train_gamma_model.py")],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "legacy CSV gamma trainer is retired" in result.stderr
