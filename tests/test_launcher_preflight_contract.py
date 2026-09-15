"""The launchers must accept the actual preflight manifest, without stale counts."""
import ast
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("launcher", ["ensure_market_app.ps1", "start_databento_app.ps1"])
@pytest.mark.parametrize("manifest_name,launcher_name", [
    ("REQUIRED_FILES", "OpeningAcceptancePreflightRequiredFileCount"),
    ("REQUIRED_MODULES", "OpeningAcceptancePreflightRequiredModuleCount"),
])
def test_launcher_checks_complete_current_preflight_manifest(launcher, manifest_name, launcher_name):
    # Parse constants only; this test must not import the live application or
    # execute the schema/bootstrap preflight.
    module = ast.parse((ROOT / "tools/preflight_opening_acceptance.py").read_text(encoding="utf-8"))
    manifests = [
        ast.literal_eval(node.value)
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == manifest_name for target in node.targets)
    ]
    assert len(manifests) == 1
    match = re.search(rf"(?m)^\${launcher_name}\s*=\s*(\d+)\s*$", (ROOT / launcher).read_text(encoding="utf-8-sig"))
    assert match is not None
    assert int(match.group(1)) == len(manifests[0])


@pytest.mark.parametrize(
    ("launcher", "root_variable"),
    [
        ("ensure_market_app.ps1", "ProjectRoot"),
        ("start_databento_app.ps1", "AppDir"),
    ],
)
def test_production_launcher_passes_absolute_canonical_entrypoints(
    launcher, root_variable
):
    source = (ROOT / launcher).read_text(encoding="utf-8-sig")

    assert (
        "$BackendEntrypoint = [System.IO.Path]::GetFullPath("
        f"(Join-Path ${root_variable} 'server.py'))"
    ) in source
    assert (
        "$DashboardEntrypoint = [System.IO.Path]::GetFullPath("
        f"(Join-Path ${root_variable} 'app.py'))"
    ) in source
    assert re.search(
        r"-ArgumentList\s+@\(\s*\('\"'\s*\+\s*\$BackendEntrypoint\s*\+\s*'\"'\)\s*\)",
        source,
    )
    assert re.search(
        r"['\"]run['\"]\s*,\s*\('\"'\s*\+\s*\$DashboardEntrypoint\s*\+\s*'\"'\)",
        source,
    )
