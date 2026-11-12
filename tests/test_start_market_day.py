import os
import json
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


def _ps_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _run_fixture(tmp_path: Path, source: str) -> subprocess.CompletedProcess:
    harness = tmp_path / "fixture.ps1"
    harness.write_text(source, encoding="utf-8")
    return subprocess.run(
        [str(_windows_powershell()), "-NoProfile", "-NonInteractive",
         "-ExecutionPolicy", "Bypass", "-File", str(harness)],
        cwd=tmp_path, capture_output=True, text=True, timeout=15, check=False,
    )


@pytest.mark.parametrize(
    "candidate,clock_ok,rut_ok,enable_rut,skip_clock,child_code,expected_exit,prestage",
    [
        ("07:00:00", False, False, True, False, 0, 1, True),
        ("07:15:00", False, False, True, False, 7, 7, True),
        ("07:15:00", True, True, True, False, 0, 0, True),
        ("07:15:00", False, True, True, False, 0, 1, True),
        ("07:15:00", False, False, False, False, 0, 1, True),
        ("07:15:00", False, False, True, True, 0, 1, True),
        ("07:45:00", False, False, True, False, 0, 1, False),
        ("07:45:00", False, False, True, True, 0, 1, False),
        ("08:15:00", True, True, True, False, 0, 0, False),
    ],
)
def test_wrapper_prestages_without_promoting_unsafe_clock(
    tmp_path, candidate, clock_ok, rut_ok, enable_rut, skip_clock,
    child_code, expected_exit, prestage, recover_on_sync=False,
):
    """Run the real wrapper control flow with every external effect replaced.

    Only the pure action selector is loaded from the production functions.
    No scheduler, clock, provider, or real launcher can run in this harness.
    """
    source = START_MARKET_DAY.read_text(encoding="utf-8")
    control_flow = source[source.index(
        "$marketCalendarStatus = Get-UsCashEquityMarketCalendarStatus"
    ):]
    child_receipt = tmp_path / "child.json"
    clock_receipt = tmp_path / "clock.txt"
    dummy_ensure = tmp_path / "ensure.ps1"
    dummy_ensure.write_text("throw 'The real child must never execute'", encoding="utf-8")
    harness = f"""
$ErrorActionPreference = 'Stop'
$Date = [datetime]'2026-09-22T{candidate}'
$CheckOnly = $false
$EnableRutCanary = ${str(enable_rut).lower()}
$SkipClockSync = ${str(skip_clock).lower()}
$EnsureScript = {_ps_literal(dummy_ensure)}
$ProjectRoot = {_ps_literal(tmp_path)}
$BootstrapMutexTimeoutMilliseconds = 1
$clockFixtureOkay = ${str(clock_ok).lower()}
$rutFixtureOkay = ${str(rut_ok).lower()}
$childReceipt = {_ps_literal(child_receipt)}
$clockReceipt = {_ps_literal(clock_receipt)}
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_literal(START_MARKET_DAY)}, [ref]$null, [ref]$null)
$definition = $ast.Find({{
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Get-MarketDayInvocationAction'
}}, $true)
Invoke-Expression $definition.Extent.Text
function Get-UsCashEquityMarketCalendarStatus {{
    [pscustomobject]@{{MarketOpen=$true;Supported=$true;Reason='fixture'}}
}}
function Enter-MarketDayBootstrapLock {{ [pscustomobject]@{{Acquired=$true}} }}
function Exit-MarketDayBootstrapLock {{ $script:lockReleased = $true }}
function Repair-MarketStartupBootRecovery {{ [pscustomobject]@{{Status='ready'}} }}
function Repair-MarketWatchdogRecoveryAuthority {{ [pscustomobject]@{{Status='ready'}} }}
function Sync-MarketSystemClock {{
    Add-Content -LiteralPath $clockReceipt -Value 'sync_attempt'
    if (${str(recover_on_sync).lower()}) {{ $script:rutFixtureOkay = $true }}
    [pscustomobject]@{{Status=$(if ($clockFixtureOkay) {{'synchronized'}} else {{'warning'}})}}
}}
function Get-RutCanaryClockEligibility {{
    [pscustomobject]@{{Eligible=$rutFixtureOkay;Status='fixture';Error='unsafe fixture clock'}}
}}
function Write-MarketClockLog {{ param($Result)
    Add-Content -LiteralPath $clockReceipt -Value $Result.Status
}}
function powershell.exe {{
    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$ChildArguments)
    if (-not $script:lockReleased) {{ throw 'Child invoked before bootstrap lock release' }}
    ConvertTo-Json -InputObject $ChildArguments | Set-Content -LiteralPath $childReceipt
    $global:LASTEXITCODE = {child_code}
}}
""" + control_flow
    result = _run_fixture(tmp_path, harness)
    assert result.returncode == expected_exit, result.stdout + result.stderr
    if prestage or (rut_ok and clock_ok) or recover_on_sync:
        arguments = json.loads(child_receipt.read_text(encoding="utf-8-sig"))
        assert ("-PrepareUniverseOnly" in arguments) is prestage
        assert ("-EnableRutCanary" in arguments) is enable_rut
        assert not any(arg in arguments for arg in ("-RestartBackend", "-RestartDashboard"))
    else:
        assert not child_receipt.exists(), "Unsafe live launch reached a child"
    clock_log = clock_receipt.read_text(encoding="utf-8-sig")
    expected_syncs = int(not skip_clock or (enable_rut and not rut_ok))
    assert clock_log.count("sync_attempt") == expected_syncs
    expected_clock_failure = prestage and child_code == 0 and expected_exit == 1
    assert ("universe_prestage_complete_clock_unready" in clock_log) is expected_clock_failure


@pytest.mark.parametrize("candidate", ["07:50:00", "08:25:00", "09:20:00"])
def test_watchdog_recovers_unsafe_clock_before_launch(tmp_path, candidate):
    test_wrapper_prestages_without_promoting_unsafe_clock(
        tmp_path, candidate, True, False, True, True, 0, 0, False,
        recover_on_sync=True,
    )


def test_watchdog_does_not_resync_healthy_clock(tmp_path):
    test_wrapper_prestages_without_promoting_unsafe_clock(
        tmp_path, "07:50:00", True, True, True, True, 0, 0, False,
    )


_RECENT_SYNC = (
    "\nPoll Interval: 6 (64s)\nLast Sync Error: 0 (success)"
    "\nTime since Last Good Sync Time: 4.5s"
)


@pytest.mark.parametrize("status,expected", [
    ("Leap Indicator: 0(no warning)\nStratum: 2" + _RECENT_SYNC, True),
    ("Leap Indicator: 1\nStratum: 15" + _RECENT_SYNC, True),
    ("Leap Indicator: 2\nStratum: 1" + _RECENT_SYNC, True),
    ("Leap Indicator: 0\nStratum: 2", False),
    ("Leap Indicator: 0\nStratum: 2" + _RECENT_SYNC.replace("Error: 0", "Error: 2"), False),
    ("Leap Indicator: 0\nStratum: 2" + _RECENT_SYNC.replace("4.5s", "129s"), False),
    ("Leap Indicator: 0\nStratum: 2" + _RECENT_SYNC.replace("4.5s", "128s"), True),
    ("Leap Indicator: 0\nStratum: 2" + _RECENT_SYNC.replace("4.5s", "-1s"), False),
    ("Leap Indicator: 0\nStratum: 2" + _RECENT_SYNC.replace("4.5s", "NaNs"), False),
    ("Leap Indicator: 3\nStratum: 2", False),
    ("Leap Indicator: 0\nStratum: 16", False),
    ("Leap Indicator: 0\nStratum: 0", False),
    ("Leap Indicator: 0", False),
    ("The command completed successfully.", False),
    ("", False),
])
def test_clock_status_requires_positive_synchronization_proof(tmp_path, status, expected):
    harness = f"""
$ErrorActionPreference = 'Stop'
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_literal(START_MARKET_DAY)}, [ref]$null, [ref]$null)
$definition = $ast.Find({{
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Test-MarketClockStatusSynchronized'
}}, $true)
Invoke-Expression $definition.Extent.Text
Test-MarketClockStatusSynchronized -StatusText {_ps_literal(status)}
"""
    result = _run_fixture(tmp_path, harness)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(expected)


@pytest.mark.parametrize("recovery", [True, False])
def test_queued_clock_sync_waits_for_verified_status(tmp_path, recovery):
    harness = f"""
$ErrorActionPreference = 'Stop'
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_literal(START_MARKET_DAY)}, [ref]$null, [ref]$null)
$definitions = $ast.FindAll({{
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -in @('Sync-MarketSystemClock', 'Test-MarketClockStatusSynchronized')
}}, $true)
foreach ($definition in $definitions) {{ Invoke-Expression $definition.Extent.Text }}
$script:queries = 0
function Get-Service {{ [pscustomobject]@{{StartType='Automatic';Status='Running'}} }}
function Set-Service {{ throw 'Unexpected service mutation' }}
function Start-Service {{ throw 'Unexpected service mutation' }}
function w32tm {{
    $global:LASTEXITCODE = 0
    if ($args -contains '/resync') {{
        if ($args -notcontains '/nowait') {{ throw 'Unbounded resync request' }}
        return 'Request accepted'
    }}
    $script:queries += 1
    if (${str(recovery).lower()} -and $script:queries -ge 2) {{
        "Leap Indicator: 0`nStratum: 2`nPoll Interval: 6 (64s)`nLast Sync Error: 0`nTime since Last Good Sync Time: 0s"
    }} else {{
        # Old good flags must not count as confirmation of the queued request.
        "Leap Indicator: 0`nStratum: 2`nPoll Interval: 6 (64s)`nLast Sync Error: 0`nTime since Last Good Sync Time: 30s"
    }}
}}
$result = Sync-MarketSystemClock -ConfirmationTimeoutSeconds 2
@{{status=$result.Status;queries=$script:queries}} | ConvertTo-Json
"""
    result = _run_fixture(tmp_path, harness)
    assert result.returncode == 0, result.stderr
    evidence = json.loads(result.stdout)
    assert evidence["status"] == ("synchronized" if recovery else "warning")
    assert evidence["queries"] >= 2


def test_clock_recovery_backoff_survives_processes_and_resets(tmp_path):
    module = PROJECT_ROOT / "tools" / "clock_recovery.psm1"
    state = tmp_path / "clock-retry.json"
    # Distinct PowerShell processes exercise durable state, not a module cache.
    first = _run_fixture(tmp_path, f"""
Import-Module {_ps_literal(module)} -Force
Set-MarketClockRecoveryState -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-08T18:00:00Z')
""")
    assert first.returncode == 0, first.stderr
    second = _run_fixture(tmp_path, f"""
$ErrorActionPreference = 'Stop'
Import-Module {_ps_literal(module)} -Force
$early = Get-MarketClockRecoveryDecision -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-08T18:04:00Z')
$due = Get-MarketClockRecoveryDecision -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-08T18:05:00Z')
Set-MarketClockRecoveryState -Path {_ps_literal(state)} -PreviousFailures $due.Failures -NowUtc ([datetime]'2026-10-08T18:05:00Z')
$secondRetry = Get-MarketClockRecoveryDecision -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-08T18:06:00Z')
Set-MarketClockRecoveryState -Path {_ps_literal(state)} -Synchronized -NowUtc ([datetime]'2026-10-08T18:07:00Z')
$reset = Get-MarketClockRecoveryDecision -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-08T18:07:01Z')
@{{early=$early;due=$due;secondRetry=$secondRetry;reset=$reset}} | ConvertTo-Json -Depth 4
""")
    assert second.returncode == 0, second.stderr
    result = json.loads(second.stdout)
    assert result["early"]["Allowed"] is False
    assert result["early"]["RetryAfterSeconds"] == 60
    assert result["due"]["Allowed"] is True
    assert result["secondRetry"]["RetryAfterSeconds"] == 540
    assert result["reset"]["Allowed"] is True
    assert result["reset"]["Failures"] == 0


def test_clock_cooldown_cannot_mask_failure_or_block_natural_recovery(tmp_path):
    module = PROJECT_ROOT / "tools" / "clock_recovery.psm1"
    state = tmp_path / "clock-retry.json"
    result = _run_fixture(tmp_path, f"""
$ErrorActionPreference = 'Stop'
$ProjectRoot = {_ps_literal(PROJECT_ROOT)}
Import-Module {_ps_literal(module)} -Force
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_literal(START_MARKET_DAY)}, [ref]$null, [ref]$null)
$definitions = $ast.FindAll({{ param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
    $node.Name -in @('Sync-MarketSystemClock','Test-MarketClockStatusSynchronized')
}}, $true)
foreach ($definition in $definitions) {{ Invoke-Expression $definition.Extent.Text }}
function Get-Service {{ throw 'Cooldown must not mutate service' }}
function Set-Service {{ throw 'Cooldown must not mutate service' }}
function Start-Service {{ throw 'Cooldown must not mutate service' }}
$script:healthy = $false
$script:oldSuccess = $false
function w32tm {{
    if ($args -contains '/resync') {{ throw 'Cooldown repeated a repair' }}
    $global:LASTEXITCODE = 0
    if ($script:oldSuccess) {{
        "Leap Indicator: 0`nStratum: 2`nPoll Interval: 10 (1024s)`nLast Sync Error: 0`nTime since Last Good Sync Time: 200s"
    }} elseif ($script:healthy) {{
        "Leap Indicator: 0`nStratum: 2`nPoll Interval: 6 (64s)`nLast Sync Error: 0`nTime since Last Good Sync Time: 2s"
    }} else {{ 'Leap Indicator: 3' }}
}}
Set-MarketClockRecoveryState -Path {_ps_literal(state)} -NowUtc ([datetime]::UtcNow.AddSeconds(-10))
$failed = Sync-MarketSystemClock -RecoveryStatePath {_ps_literal(state)}
$script:oldSuccess = $true
$oldSuccessResult = Sync-MarketSystemClock -RecoveryStatePath {_ps_literal(state)}
$script:oldSuccess = $false
$script:healthy = $true
$recovered = Sync-MarketSystemClock -RecoveryStatePath {_ps_literal(state)}
@{{failed=$failed; oldSuccess=$oldSuccessResult; recovered=$recovered}} | ConvertTo-Json -Depth 4
""")
    assert result.returncode == 0, result.stderr
    outcome = json.loads(result.stdout)
    assert outcome["failed"]["Status"] == "warning"
    assert "CLOCK_RECOVERY_BACKOFF" in outcome["failed"]["Error"]
    assert outcome["oldSuccess"]["Status"] == "warning"
    assert outcome["recovered"]["Status"] == "synchronized"


@pytest.mark.parametrize("corrupt", [True, False])
def test_clock_retry_invalid_state_or_backward_step_never_defers_forever(tmp_path, corrupt):
    module = PROJECT_ROOT / "tools" / "clock_recovery.psm1"
    state = tmp_path / "clock-retry.json"
    setup = (f"[IO.File]::WriteAllText({_ps_literal(state)}, '{{bad json')" if corrupt else
             f"Set-MarketClockRecoveryState -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-09T18:00:00Z')")
    result = _run_fixture(tmp_path, f"""
Import-Module {_ps_literal(module)} -Force
{setup}
Get-MarketClockRecoveryDecision -Path {_ps_literal(state)} -NowUtc ([datetime]'2026-10-08T18:00:00Z') | ConvertTo-Json
""")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["Allowed"] is True


@pytest.mark.parametrize("prepare_only,clock_ok", [(True, False), (True, True), (False, False), (False, True)])
def test_ensure_clock_exemption_is_limited_to_metadata_only(tmp_path, prepare_only, clock_ok):
    """Execute just the actual gate AST; never dot-source the live supervisor."""
    ensure = PROJECT_ROOT / "ensure_market_app.ps1"
    harness = f"""
$ErrorActionPreference = 'Stop'
$EnableRutCanary = $true
$PrepareUniverseOnly = ${str(prepare_only).lower()}
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_literal(ensure)}, [ref]$null, [ref]$null)
$gate = @($ast.FindAll({{
    param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
        $node.Clauses[0].Item1.Extent.Text -eq '$EnableRutCanary -and -not $PrepareUniverseOnly'
}}, $true))
if ($gate.Count -ne 1) {{ throw 'Expected one scoped live clock gate' }}
function Get-Service {{
    if ($PrepareUniverseOnly) {{ throw 'Metadata-only path checked live clock' }}
    [pscustomobject]@{{Status='Running'}}
}}
function w32tm {{
    $global:LASTEXITCODE = 0
    'Leap Indicator: {0 if clock_ok else 3}'
}}
Invoke-Expression $gate[0].Extent.Text
"""
    result = _run_fixture(tmp_path, harness)
    assert result.returncode == (0 if prepare_only or clock_ok else 1), result.stderr
