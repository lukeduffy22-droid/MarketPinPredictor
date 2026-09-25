[CmdletBinding()]
param(
    [string]$TradingDate = (Get-Date).ToString('yyyy-MM-dd'),
    [switch]$NoAnalysis,
    [switch]$NoParquet,
    [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$projectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$pythonPath = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "MarketPin Python environment was not found: $pythonPath"
}

$sessionDir = Join-Path $projectRoot "data\closing_tape\$TradingDate"
$logDir = Join-Path $projectRoot 'logs\runtime'
$pidPath = Join-Path $sessionDir 'recorder.pid'

if (Test-Path -LiteralPath $pidPath -PathType Leaf) {
    $recordedPid = 0
    [void][int]::TryParse((Get-Content -LiteralPath $pidPath -Raw).Trim(), [ref]$recordedPid)
    if ($recordedPid -gt 0) {
        $existing = Get-CimInstance Win32_Process -Filter "ProcessId=$recordedPid" -ErrorAction SilentlyContinue
        if ($null -ne $existing -and $existing.CommandLine -like '*backend.closing_tape.live_recorder*') {
            [pscustomobject]@{
                Status = 'already_running'
                Pid = $recordedPid
                StatusPath = (Join-Path $sessionDir 'status.json')
            }
            # This launcher is also invoked from ensure_market_app.ps1. `exit`
            # would terminate that parent PowerShell runspace before it could
            # log the result or release control normally; return only from this
            # script instead.
            return
        }
    }
}

$gateJson = & $pythonPath -m backend.closing_tape.start_gate `
    --project-root $projectRoot `
    --trading-date $TradingDate `
    --require-primary-ready `
    --backend-url 'http://127.0.0.1:8000' `
    --timeout-seconds 2
$gateExitCode = $LASTEXITCODE
if ($gateExitCode -eq 4) {
    $gate = $gateJson | ConvertFrom-Json
    $startNotBeforeUtc = ([DateTimeOffset]$gate.start_not_before_utc).ToUniversalTime().ToString('o')
    if ([string]$gate.state -eq 'primary_capture_not_ready') {
        [pscustomobject]@{
            Status = 'primary_capture_not_ready'
            Pid = $null
            Reason = $gate.reason
            PrimaryCapture = $gate.primary_capture
            StartNotBeforeUtc = $startNotBeforeUtc
        }
        return
    }
    [pscustomobject]@{
        Status = 'not_yet_due'
        Pid = $null
        Reason = $gate.reason
        StartNotBeforeUtc = $startNotBeforeUtc
    }
    return
}
if ($gateExitCode -eq 3) {
    $gate = $gateJson | ConvertFrom-Json
    if ([string]$gate.state -eq 'recovery_attempt_limit_reached') {
        [pscustomobject]@{
            Status = 'recovery_attempt_limit_reached'
            Pid = $null
            Reason = $gate.reason
            PriorNonemptyDbnCount = $gate.prior_nonempty_dbn_count
            AnalysisDueUtc = $gate.analysis_due_utc
        }
        return
    }
    [pscustomobject]@{
        Status = 'outside_start_window'
        Pid = $null
        Reason = $gate.reason
        AnalysisDueUtc = $gate.analysis_due_utc
    }
    return
}
if ($gateExitCode -ne 0) {
    throw "Closing-tape start gate failed with exit code $gateExitCode."
}
$gate = $gateJson | ConvertFrom-Json

if ($CheckOnly) {
    [pscustomobject]@{
        Status = 'ready_to_start'
        Pid = $null
        Reason = $gate.reason
        GateState = $gate.state
        CaptureMode = $gate.capture_mode
        AnalysisEligible = [bool]$gate.analysis_eligible
        StartNotBeforeUtc = ([DateTimeOffset]$gate.start_not_before_utc).ToUniversalTime().ToString('o')
        AnalysisDueUtc = ([DateTimeOffset]$gate.analysis_due_utc).ToUniversalTime().ToString('o')
    }
    return
}

# A check-only gate must remain filesystem-read-only. Create launch evidence
# directories only after the owner gate has authorized an actual start.
New-Item -ItemType Directory -Force -Path $sessionDir, $logDir | Out-Null

# Start-Process truncates redirect targets. Give every attempt its own evidence
# files so a recovery never erases the failure that made it necessary.
$launchStamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$stdoutPath = Join-Path $logDir "closing_tape_${TradingDate}_$launchStamp.stdout.log"
$stderrPath = Join-Path $logDir "closing_tape_${TradingDate}_$launchStamp.stderr.log"
$arguments = @(
    '-m', 'backend.closing_tape.live_recorder',
    '--project-root', $projectRoot,
    '--trading-date', $TradingDate
)
if ($NoAnalysis -or [string]$gate.capture_mode -eq 'capture_only') { $arguments += '--no-analysis' }
if ($NoParquet) { $arguments += '--no-parquet' }

$process = Start-Process `
    -FilePath $pythonPath `
    -ArgumentList $arguments `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -PassThru

Set-Content -LiteralPath $pidPath -Value $process.Id -Encoding ascii
Start-Sleep -Seconds 3
$process.Refresh()
if ($process.HasExited) {
    $errorTail = if (Test-Path -LiteralPath $stderrPath) {
        (Get-Content -LiteralPath $stderrPath -Tail 30) -join [Environment]::NewLine
    } else {
        'No error log was written.'
    }
    throw "Closing-tape recorder exited during startup (code $($process.ExitCode)).`n$errorTail"
}

$catalogPath = Join-Path $sessionDir 'closing_tape.sqlite'
$statusPath = Join-Path $sessionDir 'status.json'
$dbnPaths = @(Get-ChildItem -LiteralPath $sessionDir -Filter '*.dbn' -File -ErrorAction SilentlyContinue | ForEach-Object FullName)
$receiptPath = Join-Path $sessionDir "launch-receipt.$launchStamp.json"
$receipt = [ordered]@{
    schema_version = 'marketpin.closing-tape-launch-receipt.v1'
    trading_date = $TradingDate
    observed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
    process_id = $process.Id
    process_running = $true
    gate_state = [string]$gate.state
    capture_mode = [string]$gate.capture_mode
    analysis_eligible = [bool]$gate.analysis_eligible
    catalog_path = $catalogPath
    catalog_observed = [bool](Test-Path -LiteralPath $catalogPath -PathType Leaf)
    status_path = $statusPath
    status_observed = [bool](Test-Path -LiteralPath $statusPath -PathType Leaf)
    dbn_paths = $dbnPaths
    dbn_observed = [bool]($dbnPaths.Count -gt 0)
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
}
$receipt | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $receiptPath -Encoding utf8

[pscustomobject]@{
    Status = 'started'
    Pid = $process.Id
    GateState = $gate.state
    CaptureMode = $gate.capture_mode
    AnalysisEligible = [bool]$gate.analysis_eligible
    CatalogPath = $catalogPath
    StatusPath = $statusPath
    DbnPaths = $dbnPaths
    LaunchReceiptPath = $receiptPath
    Stdout = $stdoutPath
    Stderr = $stderrPath
}
