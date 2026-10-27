# Importing this module does not inspect or change running services.
function Get-ResearchListeners {
    param([Parameter(Mandatory=$true)][string]$Root)
    # Reuse the core supervisor's netstat lookup. CIM-backed TCP enumeration
    # can stall for minutes on this host, defeating bounded research upkeep.
    Import-Module (Join-Path $Root 'market_app_supervisor.psm1') -ErrorAction Stop
    @(Get-MarketAppListenerProcessIds -Port 8001)
}
function Get-ResearchProcess {
    param([int]$ProcessId)
    Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop
}
function Test-ResearchProcess {
    param($Record, [string]$Root)
    if (-not $Record -or [IO.Path]::GetFileName([string]$Record.ExecutablePath) -ine 'python.exe') { return $false }
    $entry = [regex]::Escape((Join-Path $Root 'tools\run_forecast_research.py'))
    $python = Join-Path $Root '.venv\Scripts\python.exe'
    $canonical = [string]$Record.ExecutablePath -ieq $python
    if (-not $canonical) {
        $parent = Get-ResearchProcess -ProcessId $Record.ParentProcessId
        $canonical = $parent -and [string]$parent.ExecutablePath -ieq $python
    }
    # The script must be Python's first argument, not a path mentioned by
    # another Python program or a shell that happens to be an ancestor.
    $commandPattern = '(?i)^\s*(?:"[^"\r\n]*python\.exe"|[^\s"]*python\.exe)\s+(?:"' + $entry + '"|' + $entry + ')(?=\s|$)'
    return $canonical -and [regex]::IsMatch([string]$Record.CommandLine, $commandPattern)
}
function Get-ResearchExpectedIdentity {
    param([string]$Root)
    Import-Module (Join-Path $Root 'market_app_supervisor.psm1') -ErrorAction Stop
    $check = Invoke-MarketAppBoundedChildProcess -ProjectRoot $Root `
        -FilePath (Join-Path $Root '.venv\Scripts\python.exe') `
        -ArgumentList @(('"' + (Join-Path $Root 'tools\run_forecast_research.py') + '"'), '--check') `
        -InvocationId ([guid]::NewGuid().ToString('N')) -LogComponent 'research-preflight' -TimeoutSeconds 15
    if (-not $check.Completed -or $null -eq $check.ExitCode -or $check.ExitCode -ne 0) { throw 'RESEARCH_PREFLIGHT_FAILED' }
    $identity = ($check.Output -join "`n") | ConvertFrom-Json
    if ($identity.loaded_source_sha256 -notmatch '^[a-f0-9]{64}$' -or
        $identity.source_identity_sha256 -notmatch '^[a-f0-9]{64}$') { throw 'RESEARCH_PREFLIGHT_SCHEMA_INVALID' }
    return $identity
}
function Get-ResearchHealth {
    Invoke-RestMethod -Uri 'http://127.0.0.1:8001/health' -TimeoutSec 2 -ErrorAction Stop
}
function Test-ResearchHealth {
    param($Health, $Identity, [int]$OwnerId)
    return ($Health.schema_version -eq 'marketpin-market-research.v1' -and
        $Health.health_schema_version -eq 'marketpin-market-research-health.v1' -and
        $Health.component -eq 'forecast_research' -and $Health.process_alive -eq $true -and
        $Health.process_id -eq $OwnerId -and $Health.instance_id -match '^[a-f0-9]{32}$' -and
        $Health.source_database_access -eq 'query_only' -and
        $Health.loaded_source_sha256 -eq $Identity.loaded_source_sha256 -and
        $Health.source_identity_sha256 -eq $Identity.source_identity_sha256 -and
        $null -ne $Health.worker -and $null -ne $Health.worker.completed_capture_count)
}
function Enter-ResearchLock {
    param([string]$Root)
    $hash = [Security.Cryptography.SHA256]::Create()
    try { $key = [BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($Root.ToLowerInvariant()))).Replace('-', '') }
    finally { $hash.Dispose() }
    $mutex = [Threading.Mutex]::new($false, "Global\MarketPinForecastResearch_$key")
    try { $acquired = $mutex.WaitOne(0) }
    catch [Threading.AbandonedMutexException] { $acquired = $true }
    return [pscustomobject]@{ Mutex=$mutex; Acquired=$acquired }
}
function Exit-ResearchLock {
    param($Lock)
    try { if ($Lock.Acquired) { $Lock.Mutex.ReleaseMutex() } }
    finally { $Lock.Mutex.Dispose() }
}
function Stop-ResearchOwnedProcess {
    param([Parameter(Mandatory=$true)]$Process)
    Stop-Process -InputObject $Process -ErrorAction Stop
    if (-not $Process.WaitForExit(5000)) { throw 'RESEARCH_RELOAD_STOP_TIMEOUT' }
}
function Write-ResearchRetryState {
    param([string]$Path, $State)
    # Serialized mutable supervision state; never stored in the source/journal DB.
    $State | ConvertTo-Json | Set-Content -LiteralPath $Path -Encoding UTF8
}
function Test-ResearchSupervisionEnabled {
    param([string]$Root)
    # Explicit environment disable always wins. The local deployment receipt
    # lets the existing SYSTEM watchdog see approval without a second scheduler
    # or changes to machine-wide environment/security settings.
    if ($env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART -eq '0') { return $false }
    if ($env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART -eq '1') { return $true }
    $path = Join-Path $Root 'config\forecast_research_runtime.json'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $false }
    $configuration = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
    return ($configuration.schema_version -eq 'marketpin-research-runtime.v1' -and
            $configuration.supervision_enabled -is [bool] -and $configuration.supervision_enabled -eq $true)
}
function Invoke-ForecastResearchEnsure {
    param([Parameter(Mandatory=$true)][string]$Root, [switch]$Supervised,
          [switch]$ReloadOwned,
          [ValidateRange(1,60)][int]$ReadinessSeconds=20)
    $Root = [IO.Path]::GetFullPath($Root)
    if ($env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART -eq '0') {
        return [pscustomobject]@{ Status='DISABLED'; Code='RESEARCH_EXPLICITLY_DISABLED' }
    }
    # An existing scheduled watchdog cannot activate these offline edits.
    if ($Supervised -and -not (Test-ResearchSupervisionEnabled -Root $Root)) {
        return [pscustomobject]@{ Status='DISABLED'; Code='RESEARCH_SUPERVISION_NOT_ENABLED' }
    }
    $lock = Enter-ResearchLock -Root $Root
    try {
        if (-not $lock.Acquired) { return [pscustomobject]@{ Status='BUSY'; Code='RESEARCH_LAUNCH_SERIALIZED' } }
        $logs = Join-Path $Root 'output\forecast_research'
        New-Item -ItemType Directory -Path $logs -Force | Out-Null
        $statePath = Join-Path $logs 'retry-state.json'
        $retry = @{ Failures=0; NextAttemptUtc=$null; LaunchPid=0; LaunchCreated=$null }
        if (Test-Path -LiteralPath $statePath) {
            $saved = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
            if ($null -eq $saved.Failures -or [int]$saved.Failures -lt 0) { throw 'RESEARCH_RETRY_STATE_INVALID' }
            foreach ($key in @('Failures','NextAttemptUtc','LaunchPid','LaunchCreated')) { $retry[$key] = $saved.$key }
        }
        $owners = @(Get-ResearchListeners -Root $Root)
        if ($owners.Count) {
            if ($owners.Count -ne 1 -or -not (Test-ResearchProcess -Record (Get-ResearchProcess $owners[0]) -Root $Root)) {
                throw 'RESEARCH_FOREIGN_PORT_OWNER'
            }
            $identity = Get-ResearchExpectedIdentity -Root $Root
            $health = Get-ResearchHealth
            if ($ReloadOwned) {
                # Explicit deployment only. The unattended watchdog never
                # passes this switch and cannot evict an existing service.
                $oldIdentity = [pscustomobject]@{
                    loaded_source_sha256=$health.loaded_source_sha256
                    source_identity_sha256=$identity.source_identity_sha256
                }
                if ($health.loaded_source_sha256 -notmatch '^[a-f0-9]{64}$' -or
                    -not (Test-ResearchHealth $health $oldIdentity $owners[0])) {
                    throw 'RESEARCH_RELOAD_IDENTITY_UNPROVEN'
                }
                $ownerRecord = Get-ResearchProcess $owners[0]
                if (-not (Test-ResearchProcess $ownerRecord $Root) -or -not $ownerRecord.CreationDate) {
                    throw 'RESEARCH_RELOAD_PROCESS_UNPROVEN'
                }
                $targetProcess = Get-Process -Id $owners[0] -ErrorAction Stop
                try {
                # Keep the verified kernel process object alive across receipt
                # persistence; a recycled numeric PID must never be targeted.
                $null = $targetProcess.Handle
                if ([Math]::Abs(($targetProcess.StartTime.ToUniversalTime() -
                        ([datetime]$ownerRecord.CreationDate).ToUniversalTime()).TotalMilliseconds) -ge 1) {
                    throw 'RESEARCH_RELOAD_PROCESS_CHANGED'
                }
                $reloadReceipt = Join-Path $logs (([datetime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')) + '-' +
                    [guid]::NewGuid().ToString('N') + '.reload-before.json')
                @{observed_at_utc=[datetime]::UtcNow.ToString('o'); health=$health;
                    intended_source_sha256=$identity.loaded_source_sha256;
                    process_created_utc=$targetProcess.StartTime.ToUniversalTime().ToString('o')} |
                    ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $reloadReceipt -Encoding UTF8
                Stop-ResearchOwnedProcess -Process $targetProcess
                } finally { $targetProcess.Dispose() }
                if (@(Get-ResearchListeners -Root $Root).Count) { throw 'RESEARCH_RELOAD_PORT_STILL_OWNED' }
                $retry.Failures = 0
                $retry.NextAttemptUtc = $null
                $retry.LaunchPid = 0
                $retry.LaunchCreated = $null
                Write-ResearchRetryState $statePath $retry
            } else {
            if (-not (Test-ResearchHealth $health $identity $owners[0])) { throw 'RESEARCH_ENDPOINT_IDENTITY_MISMATCH' }
            if ($health.worker.status -in @('STALLED','ERROR','STOPPED') -or -not $health.worker.thread_alive) {
                return [pscustomobject]@{ Status='DEGRADED'; Code='RESEARCH_WORKER_REQUIRES_REVIEW'; Health=$health }
            }
            if ($health.worker.completed_capture_count -ge 2) {
                $retry.Failures = 0
                $retry.NextAttemptUtc = $null
                Write-ResearchRetryState $statePath $retry
            }
            return [pscustomobject]@{ Status='READY'; Code='RESEARCH_OWNED_ENDPOINT'; Health=$health }
            }
        }
        if ($retry.LaunchPid) {
            $pending = Get-ResearchProcess -ProcessId $retry.LaunchPid
            if ($pending -and [Math]::Abs((([datetime]$pending.CreationDate).ToUniversalTime() -
                        ([datetime]$retry.LaunchCreated).ToUniversalTime()).TotalMilliseconds) -lt 1) {
                return [pscustomobject]@{ Status='DEGRADED'; Code='RESEARCH_PENDING_PROCESS'; LaunchPid=$retry.LaunchPid }
            }
        }
        if ($retry.NextAttemptUtc -and [datetime]::UtcNow -lt ([datetime]$retry.NextAttemptUtc).ToUniversalTime()) {
            return [pscustomobject]@{ Status='BACKOFF'; Code='RESEARCH_RETRY_BACKOFF'; NextAttemptUtc=$retry.NextAttemptUtc }
        }
        $retry.Failures = [Math]::Min(16, [int]$retry.Failures + 1)
        $delay = [Math]::Min(1800, 60 * [Math]::Pow(2, $retry.Failures - 1))
        $retry.NextAttemptUtc = [datetime]::UtcNow.AddSeconds($delay).ToString('o')
        $retry.LaunchPid = 0
        $retry.LaunchCreated = $null
        Write-ResearchRetryState $statePath $retry
        $identity = Get-ResearchExpectedIdentity -Root $Root
        $launchId = [datetime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ') + '-' + [guid]::NewGuid().ToString('N')
        $stdout = Join-Path $logs "$launchId.stdout.log"
        $stderr = Join-Path $logs "$launchId.stderr.log"
        $process = Start-Process -FilePath (Join-Path $Root '.venv\Scripts\python.exe') `
            -ArgumentList @(('"' + (Join-Path $Root 'tools\run_forecast_research.py') + '"'), '--port', '8001') `
            -WorkingDirectory $Root -WindowStyle Hidden -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr -PassThru
        $null = $process.Handle
        $retry.LaunchPid = $process.Id
        $retry.LaunchCreated = $process.StartTime.ToUniversalTime().ToString('o')
        Write-ResearchRetryState $statePath $retry
        $deadline = [datetime]::UtcNow.AddSeconds($ReadinessSeconds)
        do {
            if ($process.HasExited) { throw "RESEARCH_EARLY_EXIT: exit=$($process.ExitCode) stderr=$stderr" }
            $owners = @(Get-ResearchListeners -Root $Root)
            if ($owners.Count) {
                $owner = Get-ResearchProcess $owners[0]
                if ($owners.Count -ne 1 -or -not (Test-ResearchProcess $owner $Root) -or
                    ($owner.ProcessId -ne $process.Id -and $owner.ParentProcessId -ne $process.Id)) {
                    throw 'RESEARCH_FOREIGN_PORT_OWNER'
                }
                $health = $null
                try { $health = Get-ResearchHealth } catch { }
                if ($health) {
                    if (-not (Test-ResearchHealth $health $identity $owners[0])) { throw 'RESEARCH_ENDPOINT_IDENTITY_MISMATCH' }
                    if ($health.worker.thread_alive -and $health.worker.status -in @('COLLECTING','WAITING')) {
                        Write-ResearchRetryState $statePath $retry
                        return [pscustomobject]@{ Status='READY'; Code='RESEARCH_OWNED_ENDPOINT'; Health=$health; Stdout=$stdout; Stderr=$stderr }
                    }
                }
            }
            Start-Sleep -Milliseconds 250
        } while ([datetime]::UtcNow -lt $deadline)
        throw "RESEARCH_READINESS_TIMEOUT: stderr=$stderr"
    }
    finally { Exit-ResearchLock -Lock $lock }
}
Export-ModuleMember -Function Invoke-ForecastResearchEnsure
