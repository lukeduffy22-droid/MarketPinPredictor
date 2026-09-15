import os
import re
import subprocess
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
START_MARKET_DAY = PROJECT_ROOT / "start_market_day.ps1"


def _windows_powershell() -> Path:
    system_root = os.environ.get("SystemRoot")
    if not system_root:
        pytest.skip("Windows PowerShell is unavailable")
    executable = (
        Path(system_root)
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    if not executable.is_file():
        pytest.skip("Windows PowerShell is unavailable")
    return executable


def _check_only_action(candidate: str) -> str:
    result = subprocess.run(
        [
            str(_windows_powershell()),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(START_MARKET_DAY),
            "-Date",
            candidate,
            "-CheckOnly",
            "-EnableRutCanary",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    match = re.search(r"^Action\s*:\s*(\S+)\s*$", result.stdout, re.MULTILINE)
    assert match is not None, result.stdout
    return match.group(1)


@pytest.mark.parametrize(
    ("candidate", "expected_action"),
    [
        ("2026-09-09T07:39:59-05:00", "would_clock_and_universe_prestage"),
        ("2026-09-09T07:40:00-05:00", "would_launch"),
        ("2026-09-09T07:44:59-05:00", "would_launch"),
        ("2026-09-09T07:45:00-05:00", "would_launch"),
    ],
)
def test_check_only_routes_0740_and_later_away_from_prestage(
    candidate: str, expected_action: str
):
    assert _check_only_action(candidate) == expected_action


def test_early_prestage_keeps_clock_failure_and_child_exit_fail_closed():
    source = START_MARKET_DAY.read_text(encoding="utf-8")

    early_branch = source.index("if ($clockPreflightOnly) {")
    clock_failure = source.index(
        "if ($clockResult -and $clockResult.Status -ne 'synchronized') {",
        early_branch,
    )
    clock_failure_exit = source.index("exit 1", clock_failure)
    prestage_child = source.index("& powershell.exe @prestageArguments", clock_failure_exit)
    child_exit = source.index("exit $LASTEXITCODE", prestage_child)
    normal_launch = source.index("if ($rutCanaryEligible) {", child_exit)

    assert early_branch < clock_failure < clock_failure_exit < prestage_child
    assert prestage_child < child_exit < normal_launch
