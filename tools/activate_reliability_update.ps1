[CmdletBinding()]
param([switch]$ReloadMainApplication)

$ErrorActionPreference = 'Stop'
$principal = [Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Open PowerShell as Administrator and rerun this script. Existing MarketPin services are elevated; ownership must be verified before reload.'
}
$activationRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$activationId = [datetime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ') + '-' + [guid]::NewGuid().ToString('N')
$activationOutput = Join-Path $activationRoot ('output\reliability_activation_' + $activationId)
New-Item -ItemType Directory -Path $activationOutput | Out-Null
trap {
    if ($activationOutput -and (Test-Path -LiteralPath $activationOutput -PathType Container)) {
    @{status='ACTIVATION_FAILED'; observed_at_utc=[datetime]::UtcNow.ToString('o');
      error=$_.Exception.Message; receipt_directory=$activationOutput} |
      ConvertTo-Json | Set-Content -LiteralPath (Join-Path $activationOutput 'failure.json') -Encoding UTF8
    }
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}

Import-Module (Join-Path $activationRoot 'tools\forecast_research_supervisor.psm1') -Force
$research = Invoke-ForecastResearchEnsure -Root $activationRoot -ReloadOwned
$research | ConvertTo-Json -Depth 14 | Set-Content -LiteralPath (Join-Path $activationOutput 'research.json') -Encoding UTF8
if ($research.Status -ne 'READY') { throw "Research reload not verified: $($research.Code). Receipt: $activationOutput" }

if ($ReloadMainApplication) {
    $centralNow = [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([datetime]::UtcNow, 'Central Standard Time')
    if ($centralNow.DayOfWeek -notin @('Saturday','Sunday') -and
        $centralNow.TimeOfDay -ge [timespan]'08:25:00' -and $centralNow.TimeOfDay -lt [timespan]'15:25:00') {
        throw 'Main-application deployment is deferred during the opening/capture window. Research reload completed separately.'
    }
    # Preserve the existing all-index startup choice rather than inventing a
    # subscription policy for this deployment. The existing launcher rechecks it.
    $startup = Get-ScheduledTask -TaskName 'MarketPinPredictor_AutoStart' -ErrorAction Stop
    $rutConfigured = [bool](@($startup.Actions | Where-Object { $_.Arguments -match '(?i)(?:^|\s)-EnableRutCanary(?:\s|$)' }).Count)
    & (Join-Path $activationRoot 'ensure_market_app.ps1') -RestartBackend -RestartDashboard `
        -EnableRutCanary:$rutConfigured -Caller 'ReliabilityDeployment'
    $backend = Invoke-RestMethod -Uri 'http://127.0.0.1:8000/health' -TimeoutSec 5 -ErrorAction Stop
    $backend | ConvertTo-Json -Depth 35 | Set-Content -LiteralPath (Join-Path $activationOutput 'backend.json') -Encoding UTF8
    $loadedFiles = @($backend.loaded_code_fingerprint.files.PSObject.Properties)
    if ($loadedFiles.Count -ne 74 -or $backend.loaded_code_fingerprint.current_on_disk_recomputed -ne $false) {
        throw "Backend loaded-source evidence is incomplete. Receipt: $activationOutput"
    }
    foreach ($entry in $loadedFiles) {
        $sourcePath = [IO.Path]::GetFullPath((Join-Path $activationRoot $entry.Name))
        if (-not $sourcePath.StartsWith($activationRoot.TrimEnd('\') + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw 'Backend source identity escaped the project root.'
        }
        $digest = (Get-FileHash -LiteralPath $sourcePath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($digest -ne $entry.Value.loaded_at_startup_sha256) { throw "Backend source mismatch: $($entry.Name)" }
    }
}
$completion = @{status='COMPONENT_RELOAD_COMPLETED'; observed_at_utc=[datetime]::UtcNow.ToString('o');
  research_process_id=$research.Health.process_id; research_loaded_source_sha256=$research.Health.loaded_source_sha256;
  main_application_reload_requested=[bool]$ReloadMainApplication; receipt_directory=$activationOutput;
  live_capture_proof='NOT_YET_VERIFIED'}
$completion | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $activationOutput 'completion.json') -Encoding UTF8
$completion | ConvertTo-Json -Depth 4
