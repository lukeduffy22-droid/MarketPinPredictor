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
    if ([string]$gate.state -eq 'recovery_replay_blocked') {
        [pscustomobject]@{
            Status = 'recovery_replay_blocked'
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

if ($CheckOnly) {
    $gate = $gateJson | ConvertFrom-Json
    [pscustomobject]@{
        Status = 'ready_to_start'
        Pid = $null
        Reason = $gate.reason
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
if ($NoAnalysis) { $arguments += '--no-analysis' }
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

[pscustomobject]@{
    Status = 'started'
    Pid = $process.Id
    StatusPath = (Join-Path $sessionDir 'status.json')
    Stdout = $stdoutPath
    Stderr = $stderrPath
}
