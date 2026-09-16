[CmdletBinding()]
param(
    [string]$OutputRoot = 'C:\Users\LukeD\Documents\Codex\marketpin-live-progress-20260916',
    [string]$ProfilerRunner = 'C:\Users\LukeD\AppData\Local\hermes\bin\uvx.exe'
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $projectRoot 'market_app_supervisor.psm1') -Force
$owners = @(Get-NetTCPConnection -LocalPort 8000 -State Listen |
    Select-Object -ExpandProperty OwningProcess -Unique)
if ($owners.Count -ne 1) { throw 'Expected exactly one backend listener on port 8000.' }
$backendPid = [int]$owners[0]
if (-not (Test-MarketAppVerifiedProcess -ProcessId $backendPid -ProjectRoot $projectRoot -RequiredCommandMarkers @('server.py', 'uvicorn'))) {
    throw 'Cannot verify canonical backend ownership. Run this script from Administrator PowerShell.'
}
if (-not (Test-Path -LiteralPath $ProfilerRunner -PathType Leaf)) { throw 'uvx profiler runner is missing.' }
$captureDir = Join-Path $OutputRoot ('live-stack-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $captureDir -ErrorAction Stop | Out-Null
$profilePath = Join-Path $captureDir 'orb-speedscope.json'
$profiler = Start-Process -FilePath $ProfilerRunner -ArgumentList @(
    '--from', 'py-spy', 'py-spy', 'record', '--pid', "$backendPid",
    '--duration', '20', '--rate', '25', '--threads', '--idle', '--nonblocking',
    '--format', 'speedscope', '--output', ('"' + $profilePath + '"')
) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $captureDir 'profiler.stdout.txt') -RedirectStandardError (Join-Path $captureDir 'profiler.stderr.txt')
$checks = @()
foreach ($route in @('/health', '/v1/orb/SPX', '/v1/orb/NDX', '/v1/orb?symbols=SPX,NDX', '/v1/orb/SPX', '/v1/orb/NDX', '/health')) {
    $started = Get-Date
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $response = Invoke-WebRequest -Uri ('http://127.0.0.1:8000' + $route) -UseBasicParsing -TimeoutSec 4
        $status = [int]$response.StatusCode
        $body = $response.Content
    } catch {
        $status = 0
        if ($_.Exception.Response) { $status = [int]$_.Exception.Response.StatusCode }
        $body = [string]$_.ErrorDetails.Message
        if (-not $body) { $body = $_.Exception.Message }
    }
    $watch.Stop()
    $checks += [pscustomobject]@{ at = $started.ToString('o'); route = $route; status = $status; seconds = $watch.Elapsed.TotalSeconds; body = $body }
    $checks | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $captureDir 'http-checks.json') -Encoding UTF8
    Write-Host "$route : HTTP $status in $([math]::Round($watch.Elapsed.TotalSeconds, 3)) s"
    Start-Sleep -Seconds 2
}
if (-not $profiler.WaitForExit(10000)) { throw "Profiler did not finish on schedule; inspect $captureDir" }
if ($profiler.ExitCode -ne 0 -or -not (Test-Path -LiteralPath $profilePath)) {
    throw "Profile capture failed. Inspect $captureDir\profiler.stderr.txt"
}
Write-Host "Capture complete: $captureDir"
