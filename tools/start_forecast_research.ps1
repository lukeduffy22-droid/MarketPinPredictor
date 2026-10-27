param([switch]$CompanionDashboard, [switch]$Supervised)

$ErrorActionPreference = 'Stop'
$researchRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
Import-Module (Join-Path $PSScriptRoot 'forecast_research_supervisor.psm1') -Force
Invoke-ForecastResearchEnsure -Root $researchRoot -Supervised:$Supervised
if ($CompanionDashboard -and $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART -ne '0' -and -not $Supervised) {
    if (Get-NetTCPConnection -LocalPort 8502 -State Listen -ErrorAction SilentlyContinue) {
        Write-Warning 'Companion port 8502 is occupied; no companion launch attempted.'
    }
    else {
        $id = [guid]::NewGuid().ToString('N')
        $logs = Join-Path $researchRoot 'output\forecast_research'
        Start-Process -FilePath (Join-Path $researchRoot '.venv\Scripts\python.exe') `
            -ArgumentList @('-m', 'streamlit', 'run', ('"' + (Join-Path $researchRoot 'forecast_research_app.py') + '"'),
                '--server.port', '8502', '--server.address', '127.0.0.1', '--server.headless', 'true', '--server.fileWatcherType', 'none') `
            -WorkingDirectory $researchRoot -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $logs "$id.dashboard.stdout.log") `
            -RedirectStandardError (Join-Path $logs "$id.dashboard.stderr.log") | Out-Null
    }
}
