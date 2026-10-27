# Default to the supervised research service and journal. Isolated comparisons
# require an explicit switch and a separate research port.
param([ValidateRange(1024,65535)][int]$ResearchPort=8001,
      [ValidateRange(1024,65535)][int]$DashboardPort=8502,
      [switch]$IsolatedComparison)
$ErrorActionPreference = 'Stop'
$previewRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ($ResearchPort -eq $DashboardPort -or $ResearchPort -in @(8000,8501) -or $DashboardPort -in @(8000,8001,8501) -or
    ($IsolatedComparison -and $ResearchPort -eq 8001) -or (-not $IsolatedComparison -and $ResearchPort -ne 8001)) {
    throw 'Preview ports must be separate from the running application.'
}
$portsToBind = if ($IsolatedComparison) { @($ResearchPort,$DashboardPort) } else { @($DashboardPort) }
foreach ($previewPort in $portsToBind) {
    $probe = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,$previewPort)
    try { $probe.Start() } finally { $probe.Stop() }
}
$previewOutput = Join-Path $previewRoot 'output\forecast_dashboard'
New-Item -ItemType Directory -Path $previewOutput -Force | Out-Null
$previewId = [datetime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ') + '-' + [guid]::NewGuid().ToString('N')
$previewPython = Join-Path $previewRoot '.venv\Scripts\python.exe'
$previewReceipt = @{ started_at_utc=[datetime]::UtcNow.ToString('o');
    research_port=$ResearchPort; dashboard_port=$DashboardPort;
    isolated_comparison=[bool]$IsolatedComparison;
    provider_collection_enabled=$false; production_processes_changed=$false }
if ($IsolatedComparison) {
$previewConfig = Join-Path $previewOutput "$previewId.config.json"
'{"indicator_capture":{"enabled":false}}' | Set-Content -LiteralPath $previewConfig -Encoding UTF8
$previewArgs = @(('"' + (Join-Path $previewRoot 'tools\run_forecast_research.py') + '"'),
    '--port', "$ResearchPort", '--journal-database', ('"' + (Join-Path $previewOutput 'research.db') + '"'),
    '--comparison-journal', ('"' + (Join-Path $previewRoot 'data\forecast_research.db') + '"'),
    '--runtime-config', ('"' + $previewConfig + '"'))
$previewBackend = Start-Process -FilePath $previewPython -ArgumentList $previewArgs -WorkingDirectory $previewRoot -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $previewOutput "$previewId.backend.stdout.log") `
    -RedirectStandardError (Join-Path $previewOutput "$previewId.backend.stderr.log") -PassThru
$previewReceipt.backend_launcher_pid = $previewBackend.Id
$previewReceipt.journal = Join-Path $previewOutput 'research.db'
} else {
    $managedHealth = Invoke-RestMethod -Uri 'http://127.0.0.1:8001/health' -TimeoutSec 3 -ErrorAction Stop
    if ($managedHealth.component -ne 'forecast_research' -or
        $managedHealth.health_schema_version -ne 'marketpin-market-research-health.v1' -or
        $managedHealth.source_database_access -ne 'query_only') {
        throw 'Managed research endpoint identity is unavailable.'
    }
    # Routing is read-only; the dashboard requires a fresh publication gate.
    $previewReceipt.managed_process_id = $managedHealth.process_id
    $previewReceipt.managed_instance_id = $managedHealth.instance_id
    $previewReceipt.managed_loaded_source_sha256 = $managedHealth.loaded_source_sha256
}
$oldPreviewPort = $env:MARKETPIN_FORECAST_RESEARCH_PORT
try {
    $env:MARKETPIN_FORECAST_RESEARCH_PORT = "$ResearchPort"
    $previewDashboard = Start-Process -FilePath $previewPython -WorkingDirectory $previewRoot -WindowStyle Hidden `
        -ArgumentList @('-m','streamlit','run',('"' + (Join-Path $previewRoot 'forecast_research_app.py') + '"'),
            '--server.port',"$DashboardPort",'--server.address','127.0.0.1','--server.headless','true','--server.fileWatcherType','none') `
        -RedirectStandardOutput (Join-Path $previewOutput "$previewId.dashboard.stdout.log") `
        -RedirectStandardError (Join-Path $previewOutput "$previewId.dashboard.stderr.log") -PassThru
    $previewReceipt.dashboard_launcher_pid = $previewDashboard.Id
} finally {
    $env:MARKETPIN_FORECAST_RESEARCH_PORT = $oldPreviewPort
}
$previewReceipt | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $previewOutput "$previewId.launch.json") -Encoding UTF8
$previewReceipt | ConvertTo-Json -Compress
