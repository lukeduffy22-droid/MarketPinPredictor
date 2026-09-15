param([switch]$CompanionDashboard)

# This starts only independent research services. Existing listeners are never
# stopped or replaced, and no production stream or subscription is touched.
$ErrorActionPreference = 'Stop'
$researchRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$researchPython = Join-Path $researchRoot '.venv\Scripts\python.exe'
$researchLogs = Join-Path $researchRoot 'output\forecast_research'
New-Item -ItemType Directory -Path $researchLogs -Force | Out-Null

function Start-ResearchComponent {
    param([int]$Port, [string]$Name, [string[]]$Arguments, [string]$HealthPath)
    $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($listener) {
        if ($Name -eq 'service') {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port$HealthPath" -TimeoutSec 3
            if ($health.schema_version -ne 'marketpin-market-research.v1') {
                throw "Port $Port is occupied by a different service."
            }
        }
        Write-Output "Research $Name already has a listener on port $Port. No process changed."
        return
    }
    $process = Start-Process -FilePath $researchPython -ArgumentList $Arguments `
        -WorkingDirectory $researchRoot -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $researchLogs "$Name.stdout.log") `
        -RedirectStandardError (Join-Path $researchLogs "$Name.stderr.log") -PassThru
    Write-Output "Research $Name started with PID $($process.Id) on port $Port; verify $HealthPath."
}

Start-ResearchComponent -Port 8001 -Name 'service' -HealthPath '/health' -Arguments @(
    'tools/run_forecast_research.py', '--port', '8001', '--source-database',
    ('"' + (Join-Path $researchRoot 'data\market_data.db') + '"')
)
if ($CompanionDashboard) {
    Start-ResearchComponent -Port 8502 -Name 'dashboard' -HealthPath '/_stcore/health' -Arguments @(
        '-m', 'streamlit', 'run', 'forecast_research_app.py', '--server.port', '8502',
        '--server.address', '127.0.0.1', '--server.headless', 'true', '--browser.gatherUsageStats', 'false'
    )
}
