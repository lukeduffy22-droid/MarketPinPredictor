param(
    [switch]$KeepRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Stop-Listeners {
    param([int[]]$Ports)

    foreach ($port in $Ports) {
        $listeners = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        if ($listeners) {
            $pids = $listeners | Select-Object -ExpandProperty OwningProcess -Unique
            foreach ($listenerPid in $pids) {
                Stop-Process -Id $listenerPid -Force -ErrorAction SilentlyContinue
            }
            Write-Host "Stopped listeners on port ${port}: $($pids -join ', ')" -ForegroundColor Yellow
        }
    }
}

$projectRoot = $PSScriptRoot
Set-Location -LiteralPath $projectRoot

Write-Host "=== Databento Preflight ===" -ForegroundColor Cyan
Write-Host "Project: $projectRoot"
Write-Host ""

Write-Host "[1/3] Running Databento-only regression tests..." -ForegroundColor Cyan
python -m pytest tests\test_databento_only_mode.py -q
if ($LASTEXITCODE -ne 0) {
    Write-Host "Preflight failed: regression tests did not pass." -ForegroundColor Red
    exit 1
}

Write-Host "[2/3] Launching stack via start_databento_app.ps1..." -ForegroundColor Cyan
& .\start_databento_app.ps1 -NoBrowser
if (-not $?) {
    Write-Host "Preflight failed: startup script returned an error." -ForegroundColor Red
    exit 1
}

Write-Host "[3/3] Validating /health provider contract..." -ForegroundColor Cyan
$health = Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -Method Get -TimeoutSec 10
$provider = [string]$health.market_data_provider

if ($provider.ToLowerInvariant() -ne "databento") {
    Write-Host "Preflight failed: expected market_data_provider=databento, got '$provider'." -ForegroundColor Red
    if (-not $KeepRunning) {
        Stop-Listeners -Ports @(8000, 8501)
    }
    exit 1
}

Write-Host "Preflight passed." -ForegroundColor Green
Write-Host "Provider: $provider" -ForegroundColor Green

if ($KeepRunning) {
    Write-Host "Services left running on ports 8000 and 8501." -ForegroundColor Green
}
else {
    Stop-Listeners -Ports @(8000, 8501)
    Write-Host "Services stopped after validation." -ForegroundColor Green
}
