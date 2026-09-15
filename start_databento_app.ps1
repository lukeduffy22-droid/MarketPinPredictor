param(
    [switch]$NoBrowser,
    [switch]$UseRemoteDatabase,
    [switch]$EnableRutCanary,
    [ValidatePattern('^[A-Za-z0-9_.:@-]+$')][string]$Caller = 'ManualStart'
)

$ErrorActionPreference = 'Stop'
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force

$AppDir = $PSScriptRoot
$SupervisorModule = Join-Path $AppDir 'market_app_supervisor.psm1'
$PythonExe = Join-Path $AppDir '.venv\Scripts\python.exe'
$StreamlitExe = Join-Path $AppDir '.venv\Scripts\streamlit.exe'
$BackendEntrypoint = [System.IO.Path]::GetFullPath((Join-Path $AppDir 'server.py'))
$DashboardEntrypoint = [System.IO.Path]::GetFullPath((Join-Path $AppDir 'app.py'))
$OpeningAcceptancePreflightScript = Join-Path $AppDir 'tools\preflight_opening_acceptance.py'
$OpeningAcceptancePreflightSchemaVersion = 'marketpin-opening-acceptance-preflight.v2'
$OpeningAcceptancePreflightRequiredFileCount = 67
$OpeningAcceptancePreflightRequiredModuleCount = 50
# Keep the manual full-stack launcher aligned with the scheduled recovery's
# bounded live-I/O headroom.
$OpeningAcceptancePreflightTimeoutSeconds = 150
$InvocationId = [guid]::NewGuid().ToString('N')
$script:OpeningAcceptancePreflightResult = $null
Import-Module -Name $SupervisorModule -Force -ErrorAction Stop

function Write-StartupSupervisorLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Event = 'status'
    )

    Write-MarketAppSupervisorLog `
        -ProjectRoot $AppDir `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -Event $Event `
        -Message $Message
}

function Invoke-OpeningAcceptancePreflight {
    param(
        [Parameter(Mandatory = $true)][string]$PythonPath,
        [Parameter(Mandatory = $true)][string]$PreflightScript,
        [Parameter(Mandatory = $true)][string]$DatabasePath,
        [ValidateSet('strict', 'bootstrap-eligibility', 'bootstrap', 'fingerprint')]
        [string]$Phase = 'strict',
        [ValidatePattern('^[0-9a-f]{64}$')]
        [string]$ExpectedSourceFingerprint,
        [ValidateSet('canonical_init_db', 'orb_reference_decision_sidecar')]
        [string]$BootstrapScope,
        [ValidateRange(1, 300)][int]$TimeoutSeconds = $OpeningAcceptancePreflightTimeoutSeconds
    )

    if (-not (Test-Path -LiteralPath $PreflightScript -PathType Leaf)) {
        Write-StartupSupervisorLog `
            -Event 'opening_acceptance_preflight_failed' `
            -Message 'component=source reason=preflight_script_missing action=abort_before_process_change'
        throw "Opening-acceptance preflight script was not found: $PreflightScript"
    }

    $preflightArguments = @(
        '-B',
        ('"' + ([System.IO.Path]::GetFullPath($PreflightScript)) + '"'),
        '--project-root',
        ('"' + ([System.IO.Path]::GetFullPath($AppDir)) + '"'),
        '--database',
        ('"' + ([System.IO.Path]::GetFullPath($DatabasePath)) + '"'),
        '--phase',
        $Phase
    )
    if ($ExpectedSourceFingerprint) {
        $preflightArguments += @(
            '--expected-source-fingerprint',
            $ExpectedSourceFingerprint
        )
    }
    if ($BootstrapScope) {
        $preflightArguments += @('--bootstrap-scope', $BootstrapScope)
    }
    try {
        $child = Invoke-MarketAppBoundedChildProcess `
            -ProjectRoot $AppDir `
            -FilePath $PythonPath `
            -ArgumentList $preflightArguments `
            -InvocationId "$InvocationId-opening-preflight" `
            -LogComponent 'opening-acceptance-preflight' `
            -TimeoutSeconds $TimeoutSeconds
    }
    catch {
        Write-StartupSupervisorLog `
            -Event 'opening_acceptance_preflight_failed' `
            -Message "component=source reason=child_launch_failed error_type=$($_.Exception.GetType().Name) timeout_seconds=$TimeoutSeconds action=abort_before_process_change"
        throw 'Opening-acceptance source/schema preflight child could not be started.'
    }
    if ($child.TimedOut) {
        Write-StartupSupervisorLog `
            -Event 'opening_acceptance_preflight_timed_out' `
            -Message "component=source child_pid=$($child.ProcessId) timeout_seconds=$TimeoutSeconds elapsed_ms=$($child.ElapsedMilliseconds) termination_confirmed=$($child.TerminationConfirmed) action=abort_before_process_change"
        throw "Opening-acceptance source/schema preflight timed out after $TimeoutSeconds seconds."
    }

    $rawOutput = @(
        $child.Output |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { $_ }
    )
    $standardError = @(
        $child.StandardError |
            ForEach-Object { ([string]$_).Trim() } |
            Where-Object { $_ }
    )
    $hasPreflightExitCode = $null -ne $child.ExitCode
    $preflightExitCode = if ($hasPreflightExitCode) {
        [int]$child.ExitCode
    }
    else {
        $null
    }
    $payloadLine = if ($rawOutput.Count -eq 1) { $rawOutput[0] } else { $null }
    $payloadTimestampFieldMatches = if ($payloadLine) {
        @([regex]::Matches($payloadLine, '"checked_at_utc"\s*:'))
    }
    else {
        @()
    }
    $payloadTimestampMatches = if ($payloadLine) {
        @([regex]::Matches(
            $payloadLine,
            '"checked_at_utc"\s*:\s*"(?<value>[^"\\]+)"'
        ))
    }
    else {
        @()
    }
    $payloadTimestampText = if (
        $payloadTimestampFieldMatches.Count -eq 1 -and
        $payloadTimestampMatches.Count -eq 1
    ) {
        [string]$payloadTimestampMatches[0].Groups['value'].Value
    }
    else {
        $null
    }
    $payload = $null
    if ($payloadLine) {
        try {
            $payload = $payloadLine | ConvertFrom-Json -ErrorAction Stop
        }
        catch {
            $payload = $null
        }
    }

    $issueCodes = if ($payload) {
        @(
            $payload.issues |
                ForEach-Object { [string]$_.code } |
                Where-Object { $_ }
        )
    }
    else {
        @('PREFLIGHT_OUTPUT_INVALID')
    }
    if ($rawOutput.Count -ne 1) {
        $issueCodes = @($issueCodes + 'PREFLIGHT_STDOUT_CONTRACT_INVALID')
    }
    if ($standardError.Count -ne 0) {
        $issueCodes = @($issueCodes + 'PREFLIGHT_STDERR_NONEMPTY')
    }
    if (-not $hasPreflightExitCode) {
        $issueCodes = @($issueCodes + 'CHILD_EXIT_CODE_UNAVAILABLE')
    }

    $payloadContractValid = $false
    if ($payload) {
        $commonPayloadFields = @(
            'schema_version',
            'phase',
            'checked_at_utc',
            'ready',
            'project_root',
            'database_path',
            'database_contract',
            'schema_bootstrap_required',
            'schema_bootstrap_scope',
            'schema_bootstrap_performed',
            'bootstrap_attempt_count',
            'source_fingerprint_sha256',
            'required_file_count',
            'required_module_count',
            'database_issues',
            'issues'
        )
        $expectedPayloadFields = if ($Phase -ceq 'bootstrap') {
            @($commonPayloadFields + 'prebootstrap_database_issues')
        }
        else {
            $commonPayloadFields
        }
        $payloadFieldNames = @($payload.PSObject.Properties.Name)
        $payloadFieldsMatch = (
            $payloadFieldNames.Count -eq $expectedPayloadFields.Count
        )
        if ($payloadFieldsMatch) {
            foreach ($expectedField in $expectedPayloadFields) {
                if ($payloadFieldNames -cnotcontains $expectedField) {
                    $payloadFieldsMatch = $false
                    break
                }
            }
        }
        $payloadRootMatches = $false
        $payloadDatabaseMatches = $false
        try {
            $payloadRootMatches = ([System.IO.Path]::GetFullPath(
                [string]$payload.project_root
            )).Equals(
                [System.IO.Path]::GetFullPath($AppDir),
                [StringComparison]::OrdinalIgnoreCase
            )
            $payloadDatabaseMatches = ([System.IO.Path]::GetFullPath(
                [string]$payload.database_path
            )).Equals(
                [System.IO.Path]::GetFullPath($DatabasePath),
                [StringComparison]::OrdinalIgnoreCase
            )
        }
        catch {
            $payloadRootMatches = $false
            $payloadDatabaseMatches = $false
        }
        $payloadIssuesAreArray = $payload.issues -is [System.Array]
        $payloadDatabaseIssuesAreArray = $payload.database_issues -is [System.Array]
        $payloadIssuesEmpty = (
            $payloadIssuesAreArray -and @($payload.issues).Count -eq 0
        )
        $payloadDatabaseIssues = if ($payloadDatabaseIssuesAreArray) {
            @($payload.database_issues)
        }
        else {
            @('__invalid_database_issues_type__')
        }
        $payloadFingerprint = [string]$payload.source_fingerprint_sha256
        $payloadTimestampValid = $false
        try {
            $payloadTimestamp = [DateTimeOffset]::Parse(
                $payloadTimestampText,
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::RoundtripKind
            )
            $payloadTimestampValid = (
                $payloadTimestampFieldMatches.Count -eq 1 -and
                $payloadTimestampMatches.Count -eq 1 -and
                $payloadTimestamp.Offset -eq [TimeSpan]::Zero -and
                $payloadTimestampText -cmatch '(Z|\+00:00)$'
            )
        }
        catch {
            $payloadTimestampValid = $false
        }
        $bootstrapAttemptTypeValid = (
            $payload.bootstrap_attempt_count -is [int] -or
            $payload.bootstrap_attempt_count -is [long]
        )
        $requiredFileCountTypeValid = (
            $payload.required_file_count -is [int] -or
            $payload.required_file_count -is [long]
        )
        $requiredModuleCountTypeValid = (
            $payload.required_module_count -is [int] -or
            $payload.required_module_count -is [long]
        )
        $baseContractValid = (
            $payloadFieldsMatch -and
            $payload.schema_version -ceq $OpeningAcceptancePreflightSchemaVersion -and
            $payload.phase -ceq $Phase -and
            $payload.ready -is [bool] -and
            $payload.ready -eq $true -and
            $payloadTimestampValid -and
            $payloadRootMatches -and
            $payloadDatabaseMatches -and
            $payloadFingerprint -cmatch '^[0-9a-f]{64}$' -and
            $requiredFileCountTypeValid -and
            [int]$payload.required_file_count -eq $OpeningAcceptancePreflightRequiredFileCount -and
            $requiredModuleCountTypeValid -and
            [int]$payload.required_module_count -eq $OpeningAcceptancePreflightRequiredModuleCount -and
            $payloadDatabaseIssuesAreArray -and
            $bootstrapAttemptTypeValid -and
            $payloadIssuesEmpty
        )
        if ($ExpectedSourceFingerprint) {
            $baseContractValid = (
                $baseContractValid -and
                $payloadFingerprint -ceq $ExpectedSourceFingerprint
            )
        }

        $phaseContractValid = switch ($Phase) {
            'strict' {
                $payload.database_contract -ceq 'strict' -and
                $payload.schema_bootstrap_required -is [bool] -and
                $payload.schema_bootstrap_required -eq $false -and
                $payload.schema_bootstrap_performed -is [bool] -and
                $payload.schema_bootstrap_performed -eq $false -and
                [int]$payload.bootstrap_attempt_count -eq 0 -and
                $null -eq $payload.schema_bootstrap_scope -and
                $payloadDatabaseIssues.Count -eq 0
            }
            'bootstrap-eligibility' {
                $eligibilityShapeValid = (
                    $payload.schema_bootstrap_required -is [bool] -and
                    $payload.schema_bootstrap_performed -is [bool] -and
                    $payload.schema_bootstrap_performed -eq $false -and
                    [int]$payload.bootstrap_attempt_count -eq 0
                )
                $strictShape = (
                    $payload.database_contract -ceq 'strict' -and
                    $payload.schema_bootstrap_required -eq $false -and
                    $null -eq $payload.schema_bootstrap_scope -and
                    $payloadDatabaseIssues.Count -eq 0
                )
                $bootstrapShape = (
                    $payload.database_contract -ceq 'additive_bootstrap_required' -and
                    $payload.schema_bootstrap_required -eq $true -and
                    [string]$payload.schema_bootstrap_scope -in @(
                        'canonical_init_db',
                        'orb_reference_decision_sidecar'
                    ) -and
                    $payloadDatabaseIssues.Count -gt 0
                )
                $eligibilityShapeValid -and ($strictShape -or $bootstrapShape)
            }
            'bootstrap' {
                $prebootstrapIssuesAreArray = (
                    $payload.prebootstrap_database_issues -is [System.Array]
                )
                $payload.database_contract -ceq 'strict' -and
                $payload.schema_bootstrap_required -is [bool] -and
                $payload.schema_bootstrap_required -eq $false -and
                $payload.schema_bootstrap_performed -is [bool] -and
                $payload.schema_bootstrap_performed -eq $true -and
                [int]$payload.bootstrap_attempt_count -eq 1 -and
                [string]$payload.schema_bootstrap_scope -ceq $BootstrapScope -and
                $payloadDatabaseIssues.Count -eq 0 -and
                $prebootstrapIssuesAreArray -and
                @($payload.prebootstrap_database_issues).Count -gt 0
            }
            'fingerprint' {
                $payload.database_contract -ceq 'cached_full_proof_not_rechecked' -and
                $payload.schema_bootstrap_required -is [bool] -and
                $payload.schema_bootstrap_required -eq $false -and
                $payload.schema_bootstrap_performed -is [bool] -and
                $payload.schema_bootstrap_performed -eq $false -and
                [int]$payload.bootstrap_attempt_count -eq 0 -and
                $null -eq $payload.schema_bootstrap_scope -and
                $payloadDatabaseIssues.Count -eq 0
            }
        }
        $payloadContractValid = $baseContractValid -and $phaseContractValid
        if (-not $payloadContractValid) {
            $issueCodes = @($issueCodes + 'PREFLIGHT_PAYLOAD_CONTRACT_INVALID')
        }
    }
    if (-not $hasPreflightExitCode -or $preflightExitCode -ne 0 -or
        -not $payload -or -not $payloadContractValid -or
        $standardError.Count -ne 0 -or $rawOutput.Count -ne 1) {
        $issueSummary = if ($issueCodes.Count -gt 0) {
            $issueCodes -join ','
        }
        else {
            'PREFLIGHT_FAILED_WITHOUT_ISSUE_CODE'
        }
        Write-StartupSupervisorLog `
            -Event 'opening_acceptance_preflight_failed' `
            -Message "component=source phase=$Phase child_pid=$($child.ProcessId) exit_code=$(if ($hasPreflightExitCode) { $preflightExitCode } else { 'unavailable' }) elapsed_ms=$($child.ElapsedMilliseconds) issues=$issueSummary action=abort_before_process_change"
        throw "Opening-acceptance source/schema preflight failed (phase=$Phase issues=$issueSummary)."
    }

    Write-StartupSupervisorLog `
        -Event 'opening_acceptance_preflight_passed' `
        -Message "component=source phase=$Phase child_pid=$($child.ProcessId) elapsed_ms=$($child.ElapsedMilliseconds) schema_version=$($payload.schema_version) database_contract=$($payload.database_contract) required_files=$($payload.required_file_count) required_modules=$($payload.required_module_count) action=continue"
    return $payload
}

function Assert-OpeningAcceptanceMutationPreflight {
    param([switch]$AllowSchemaBootstrap)

    if ($null -ne $script:OpeningAcceptancePreflightResult) {
        $cachedFingerprint = [string]$script:OpeningAcceptancePreflightResult.source_fingerprint_sha256
        Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'fingerprint' `
            -ExpectedSourceFingerprint $cachedFingerprint `
            -TimeoutSeconds $OpeningAcceptancePreflightTimeoutSeconds | Out-Null
        return $script:OpeningAcceptancePreflightResult
    }

    # A strict-complete database needs one read-only eligibility pass. Only the
    # closed additive-migration contract may proceed to the bounded bootstrap.
    $initialPhase = if ($AllowSchemaBootstrap) {
        'bootstrap-eligibility'
    }
    else {
        'strict'
    }
    $preflight = Invoke-OpeningAcceptancePreflight `
        -PythonPath $PythonExe `
        -PreflightScript $OpeningAcceptancePreflightScript `
        -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
        -Phase $initialPhase `
        -TimeoutSeconds $OpeningAcceptancePreflightTimeoutSeconds

    if ($preflight.schema_bootstrap_required -eq $true) {
        if (-not $AllowSchemaBootstrap) {
            throw 'Opening-acceptance schema bootstrap was required but not authorized for this mutation path.'
        }
        $bootstrapScope = [string]$preflight.schema_bootstrap_scope
        $backendApplicationPids = @(Get-MarketAppListenerProcessIds -Port 8000)
        $dashboardApplicationPids = @(Get-MarketAppListenerProcessIds -Port 8501)
        $activeApplicationPids = @(
            $backendApplicationPids + $dashboardApplicationPids
        )
        $bootstrapBlockReason = $null
        if (
            $bootstrapScope -ceq 'canonical_init_db' -and
            $activeApplicationPids.Count -ne 0
        ) {
            $bootstrapBlockReason = 'canonical_init_requires_free_application_ports'
        }
        elseif (
            $bootstrapScope -ceq 'orb_reference_decision_sidecar' -and
            $activeApplicationPids.Count -ne 0
        ) {
            $bootstrapNow = Get-Date
            $onlineBootstrapWindow = (
                $bootstrapNow -lt $bootstrapNow.Date.AddHours(8).AddMinutes(25) -or
                $bootstrapNow -ge $bootstrapNow.Date.AddHours(15).AddMinutes(15)
            )
            if (-not $onlineBootstrapWindow) {
                $bootstrapBlockReason = 'decision_sidecar_online_window_closed'
            }
            elseif (
                $backendApplicationPids.Count -gt 1 -or
                $dashboardApplicationPids.Count -gt 1
            ) {
                $bootstrapBlockReason = 'application_listener_count_unverified'
            }
            elseif (
                $backendApplicationPids.Count -eq 1 -and
                -not (Test-MarketAppVerifiedProcess `
                    -ProcessId ([int]$backendApplicationPids[0]) `
                    -ProjectRoot $AppDir `
                    -RequiredCommandMarkers @('server.py', 'backend.app:app'))
            ) {
                $bootstrapBlockReason = 'backend_listener_ownership_unverified'
            }
            elseif (
                $dashboardApplicationPids.Count -eq 1 -and
                -not (Test-MarketAppVerifiedProcess `
                    -ProcessId ([int]$dashboardApplicationPids[0]) `
                    -ProjectRoot $AppDir `
                    -RequiredCommandMarkers @('streamlit', 'app.py'))
            ) {
                $bootstrapBlockReason = 'dashboard_listener_ownership_unverified'
            }
        }
        if ($bootstrapBlockReason) {
            Write-StartupSupervisorLog `
                -Event 'opening_acceptance_schema_bootstrap_blocked' `
                -Message "component=database scope=$bootstrapScope reason=$bootstrapBlockReason listener_pids=$($activeApplicationPids -join ',') action=abort_without_schema_change"
            throw "Opening-acceptance schema bootstrap was blocked (scope=$bootstrapScope reason=$bootstrapBlockReason)."
        }

        Write-StartupSupervisorLog `
            -Event 'opening_acceptance_schema_bootstrap_requested' `
            -Message "component=database scope=$bootstrapScope database_contract=$($preflight.database_contract) issue_count=$(@($preflight.database_issues).Count) action=run_bounded_additive_schema_bootstrap"
        $preflight = Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'bootstrap' `
            -ExpectedSourceFingerprint ([string]$preflight.source_fingerprint_sha256) `
            -BootstrapScope $bootstrapScope `
            -TimeoutSeconds $OpeningAcceptancePreflightTimeoutSeconds
        Write-StartupSupervisorLog `
            -Event 'opening_acceptance_schema_bootstrap_passed' `
            -Message "component=database bootstrap_attempt_count=$($preflight.bootstrap_attempt_count) source_fingerprint=$($preflight.source_fingerprint_sha256) action=cache_strict_postcondition"
    }
    $script:OpeningAcceptancePreflightResult = $preflight
    return $script:OpeningAcceptancePreflightResult
}

Set-Location $AppDir
if ($EnableRutCanary) {
    $timeService = Get-Service -Name W32Time -ErrorAction Stop
    if ($timeService.Status -ne 'Running') {
        throw '-EnableRutCanary requires the Windows Time service to be Running.'
    }
    $timeStatus = @(& w32tm /query /status 2>&1)
    if ($LASTEXITCODE -ne 0 -or (($timeStatus -join "`n") -match 'Leap Indicator:\s*3')) {
        throw '-EnableRutCanary requires a synchronized Windows Time status (Leap Indicator must not be 3).'
    }
}
Remove-Item Env:DATABENTO_REFRESH_CACHE -ErrorAction SilentlyContinue
if (-not $UseRemoteDatabase) {
    $sqlitePath = (Join-Path $AppDir "data\market_data.db").Replace("\", "/")
    $env:DATABASE_URL = "sqlite:///$sqlitePath"
}
$env:MARKET_DATA_PROVIDER = "databento"
# Protect opening SPX/NDX capacity. ETF families remain supported canaries but
# are not part of the launch-critical subscription.
$env:DATABENTO_SYMBOLS = if ($EnableRutCanary) { "SPX,NDX,VIX,RUT" } else { "SPX,NDX,VIX" }
$env:DATABENTO_RUT_CANARY_ENABLED = if ($EnableRutCanary) { "1" } else { "0" }
# Keep the guarded four-family canary within its independently measured
# subscription budget; core-only behavior keeps the configured 3,600 ceiling.
$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS = if ($EnableRutCanary) { "3200" } else { "3600" }
$env:DATABENTO_REPLAY_MINUTES = "0"
$env:DATABENTO_SNAPSHOT_INTERVAL_SECONDS = "60"
$env:PREDICTION_CAPTURE_INTERVAL_SECONDS = "60"
$env:DATABENTO_USE_MULTI_EXPIRATION = "0"
$env:DATABENTO_SUBSCRIPTION_PROFILE = "near-term-shadow"
# Leave the primary-expiration bound unchanged and preserve the required
# 50-pair next-listed reservation while trimming surplus pairs from each
# non-primary expiration that contributed to regular-session queue load.
$env:DATABENTO_SHADOW_MAX_STRIKE_PAIRS = "50"
$env:DATABENTO_ALLOW_PRIOR_UNIVERSE_FALLBACK = "1"
$env:DATABENTO_UNIVERSE_FALLBACK_MAX_AGE_DAYS = "4"
$env:DATABENTO_UNIVERSE_FALLBACK_REFRESH_SECONDS = "28800"
$env:DATABENTO_QUOTE_FRESHNESS_SECONDS = "30"
$env:DATABENTO_HANDOFF_TIMEOUT_SECONDS = "120"
$env:DATABENTO_UNIVERSE_REFRESH_SECONDS = "28800"
$env:DATABENTO_STREAM_STALL_SECONDS = "45"
$env:DATABENTO_PROGRESS_WINDOW_SECONDS = "20"
$env:DATABENTO_COMPUTE_WARMUP_GRACE_SECONDS = "30"
$env:DATABENTO_OVERLOAD_COMPUTE_BACKOFF_SECONDS = "30"

function Stop-MarketPinProcesses {
    param([string]$Root)

    # Keep this invariant local to the only broad manual-stop helper so a
    # future caller cannot bypass the source/schema proof accidentally.
    Assert-OpeningAcceptanceMutationPreflight | Out-Null

    $processes = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and
        $_.CommandLine.Contains($Root) -and (
            $_.CommandLine.Contains("backend.app:app") -or
            $_.CommandLine.Contains("server.py") -or
            $_.CommandLine.Contains("app.py") -or
            $_.CommandLine.Contains("streamlit_app.py")
        )
    }

    if ($processes) {
        $pids = @()
        foreach ($proc in $processes) {
            if (-not (Test-MarketAppVerifiedProcess `
                -ProcessId $proc.ProcessId `
                -ProjectRoot $Root `
                -RequiredCommandMarkers @('backend.app:app', 'server.py', 'streamlit', 'app.py'))) {
                throw "Refusing to stop PID $($proc.ProcessId) because it is not a verified MarketPinPredictor process."
            }
            $pids += $proc.ProcessId
            Write-StartupSupervisorLog `
                -Event 'stop_verified_startup_process' `
                -Message "process_pid=$($proc.ProcessId) action=stop"
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
            $quiescence = Wait-MarketAppProcessNetworkQuiescence `
                -ProcessId $proc.ProcessId `
                -TimeoutSeconds 30
            if (-not $quiescence.Quiescent) {
                throw "PID $($proc.ProcessId) did not reach exact-PID network quiescence after stop: process_exists=$($quiescence.ProcessExists) tcp_connection_count=$($quiescence.TcpConnectionCount)."
            }
            Write-StartupSupervisorLog `
                -Event 'stop_verified_startup_process_quiescent' `
                -Message "process_pid=$($proc.ProcessId) process_exited=true tcp_connection_count=0 quiescence_elapsed_ms=$($quiescence.ElapsedMilliseconds) action=continue"
        }
        Write-Host "Stopped stale MarketPin processes: $($pids -join ', ')" -ForegroundColor Yellow
    }

    # Never kill an arbitrary listener. A remaining listener may belong to an
    # unrelated application; fail closed and show the owning process instead.
    foreach ($port in @(8000, 8501)) {
        $listenerPids = @(Get-MarketAppListenerProcessIds -Port $port)
        if ($listenerPids) {
            throw "Port $port is still occupied after stopping verified MarketPin processes. Refusing to kill listener PID(s): $($listenerPids -join ',')."
        }
    }
}

function Wait-ForHttpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,

        [Parameter(Mandatory = $true)]
        [string]$Label,

        [Parameter(Mandatory = $true)]
        [int]$Port,

        [Parameter(Mandatory = $true)]
        [int]$LaunchProcessId,

        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract,

        [int]$TimeoutSeconds = 90
    )

    $listenerPid = Wait-MarketAppOwnedEndpoint `
        -Url $Url `
        -Port $Port `
        -LaunchProcessId $LaunchProcessId `
        -ExpectedEndpointContract $ExpectedEndpointContract `
        -TimeoutSeconds $TimeoutSeconds
    if ($listenerPid) {
        Write-Host "  $Label is responding at $Url" -ForegroundColor Green
        Write-StartupSupervisorLog `
            -Event 'launch_succeeded' `
            -Message "component=$($Label.ToLowerInvariant()) port=$Port launch_pid=$LaunchProcessId listener_pid=$listenerPid"
        return $true
    }

    Write-Host "  $Label did not become ready under launch PID $LaunchProcessId within $TimeoutSeconds seconds: $Url" -ForegroundColor Red
    Write-StartupSupervisorLog `
        -Event 'launch_ownership_failed' `
        -Message "component=$($Label.ToLowerInvariant()) port=$Port launch_pid=$LaunchProcessId"
    return $false
}

function Stop-StartupProcesses {
    param(
        [System.Diagnostics.Process]$BackendProcess,
        [System.Diagnostics.Process]$DashboardProcess
    )

    if ($BackendProcess) {
        Stop-MarketAppAttemptedLaunch -LaunchProcessId $BackendProcess.Id
    }

    if ($DashboardProcess) {
        Stop-MarketAppAttemptedLaunch -LaunchProcessId $DashboardProcess.Id
    }
}

Write-StartupSupervisorLog `
    -Event 'invocation_started' `
    -Message "no_browser=$([bool]$NoBrowser) use_remote_database=$([bool]$UseRemoteDatabase)"
$supervisorLock = Enter-MarketAppSupervisorLock -ProjectRoot $AppDir -TimeoutMilliseconds 0
if (-not $supervisorLock.Acquired) {
    Write-StartupSupervisorLog `
        -Event 'supervisor_busy' `
        -Message "mutex=$($supervisorLock.Name) action=abort"
    Exit-MarketAppSupervisorLock -LockHandle $supervisorLock
    throw 'Another MarketPinPredictor supervisor invocation is already active; full startup was not attempted.'
}

Write-StartupSupervisorLog -Event 'supervisor_lock_acquired' -Message "mutex=$($supervisorLock.Name)"
try {
Write-Host "Starting MarketPinPredictor with Databento OPRA..." -ForegroundColor Cyan
Write-Host "Startup self-check:" -ForegroundColor Cyan
Write-Host "  Active path      : $AppDir"
Write-Host "  Provider         : $env:MARKET_DATA_PROVIDER"
Write-Host "  Databento key    : $(if($env:DATABENTO_API_KEY){'present'}else{'missing'})"
Write-Host "  Massive/Polygon  : disabled"
Write-Host "  Database         : $(if($UseRemoteDatabase){'configured remote database'}else{'local SQLite'})"

if (-not (Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
    Write-StartupSupervisorLog `
        -Event 'canonical_runtime_missing' `
        -Message 'component=python expected=.venv\Scripts\python.exe action=abort_before_process_change'
    throw "Canonical MarketPin Python was not found: $PythonExe"
}
if (-not (Test-Path -LiteralPath $StreamlitExe -PathType Leaf)) {
    Write-StartupSupervisorLog `
        -Event 'canonical_runtime_missing' `
        -Message 'component=streamlit expected=.venv\Scripts\streamlit.exe action=abort_before_process_change'
    throw "Canonical MarketPin Streamlit was not found: $StreamlitExe"
}

Write-Host "  Project dir : $AppDir"
Write-Host "  Python      : $PythonExe"
Write-Host ""

$universePreparationNow = Get-Date
$universePreparationDeadline = Get-MarketAppUniversePreparationDeadline `
    -Now $universePreparationNow
$universeProviderDiscoveryAllowed = Test-MarketAppUniverseProviderDiscoveryAllowed `
    -Now $universePreparationNow
$universePreparation = Invoke-MarketAppUniverseCachePreparation `
    -ProjectRoot $AppDir `
    -PythonExe $PythonExe `
    -Symbols $env:DATABENTO_SYMBOLS `
    -Deadline $universePreparationDeadline `
    -InvocationId "$InvocationId-manual-start" `
    -SkipProviderDiscovery:(-not $universeProviderDiscoveryAllowed)
if (-not $universePreparation.StartupMayContinue) {
    Write-StartupSupervisorLog `
        -Event 'universe_cache_preparation_blocked_startup' `
        -Message "component=backend outcome=$($universePreparation.Outcome) deadline_ct=$($universePreparationDeadline.ToString('HH:mm:ss')) action=abstain"
    throw "Databento universe preparation blocked backend startup (outcome=$($universePreparation.Outcome))."
}
if ($universePreparation.UsesFallback) {
    Write-Host (
        "  Universe    : PRIOR_SESSION_FALLBACK source=$($universePreparation.Result.provenance.source_date) target=$($universePreparation.Result.provenance.trading_date)"
    ) -ForegroundColor Yellow
    Write-StartupSupervisorLog `
        -Event 'universe_cache_prior_session_fallback' `
        -Message "label=$($universePreparation.ProvenanceLabel) source_date=$($universePreparation.Result.provenance.source_date) trading_date=$($universePreparation.Result.provenance.trading_date)"
}
else {
    Write-Host "  Universe    : CURRENT_DAY_CACHE" -ForegroundColor Green
    Write-StartupSupervisorLog `
        -Event 'universe_cache_current_day_ready' `
        -Message "label=$($universePreparation.ProvenanceLabel) source_date=$($universePreparation.Result.provenance.source_date) trading_date=$($universePreparation.Result.provenance.trading_date)"
}

# Universe preparation and the opening-acceptance proof must both succeed
# before this manual full-stack fallback may stop an existing verified process.
Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
Stop-MarketPinProcesses -Root $AppDir

$backendLogPaths = New-MarketAppLaunchLogPaths `
    -ProjectRoot $AppDir `
    -Component 'backend' `
    -InvocationId $InvocationId
$dashboardLogPaths = New-MarketAppLaunchLogPaths `
    -ProjectRoot $AppDir `
    -Component 'dashboard' `
    -InvocationId $InvocationId
Write-StartupSupervisorLog `
    -Event 'launch_logs_prepared' `
    -Message "component=backend retained_stdout=$($backendLogPaths.RetainedStandardOutputPath) retained_stderr=$($backendLogPaths.RetainedStandardErrorPath)"
Write-StartupSupervisorLog `
    -Event 'launch_logs_prepared' `
    -Message "component=dashboard retained_stdout=$($dashboardLogPaths.RetainedStandardOutputPath) retained_stderr=$($dashboardLogPaths.RetainedStandardErrorPath)"

$backendProcess = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @(
        ('"' + $BackendEntrypoint + '"')
    ) `
    -WorkingDirectory $AppDir `
    -RedirectStandardOutput $backendLogPaths.StandardOutputPath `
    -RedirectStandardError $backendLogPaths.StandardErrorPath `
    -PassThru
Write-StartupSupervisorLog `
    -Event 'launch_started' `
    -Message "component=backend port=8000 launch_pid=$($backendProcess.Id)"

$dashboardProcess = Start-Process -FilePath $StreamlitExe -ArgumentList @(
    "run",
    ('"' + $DashboardEntrypoint + '"'),
    "--server.address",
    "127.0.0.1",
    "--server.port",
    "8501",
    "--server.headless",
    "true",
    # Source changes are deployed through a verified restart.  Leaving
    # Streamlit's watcher active lets it evict imported modules during a rerun,
    # which can crash Python dataclass decoration mid-import.
    "--server.fileWatcherType",
    "none"
) -WorkingDirectory $AppDir `
    -RedirectStandardOutput $dashboardLogPaths.StandardOutputPath `
    -RedirectStandardError $dashboardLogPaths.StandardErrorPath `
    -PassThru
Write-StartupSupervisorLog `
    -Event 'launch_started' `
    -Message "component=dashboard port=8501 launch_pid=$($dashboardProcess.Id)"

Write-Host "Waiting for services to come online..." -ForegroundColor Cyan

if (-not (Wait-ForHttpEndpoint `
    -Url "http://127.0.0.1:8000/health" `
    -Label "Backend" `
    -Port 8000 `
    -LaunchProcessId $backendProcess.Id `
    -ExpectedEndpointContract 'BackendHealth' `
    -TimeoutSeconds 90)) {
    Write-Host "Backend failed to start. Cleaning up processes." -ForegroundColor Red
    Stop-StartupProcesses -BackendProcess $backendProcess -DashboardProcess $dashboardProcess
    throw 'Backend launch ownership/readiness verification failed.'
}

if (-not (Wait-ForHttpEndpoint `
    -Url "http://127.0.0.1:8501/_stcore/health" `
    -Label "Dashboard" `
    -Port 8501 `
    -LaunchProcessId $dashboardProcess.Id `
    -ExpectedEndpointContract 'StreamlitHealth' `
    -TimeoutSeconds 90)) {
    Write-Host "Dashboard failed to start. Cleaning up processes." -ForegroundColor Red
    Stop-StartupProcesses -BackendProcess $backendProcess -DashboardProcess $dashboardProcess
    throw 'Dashboard launch ownership/readiness verification failed.'
}

if ($env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART -ne '0') {
    try {
        & (Join-Path $AppDir 'tools\start_forecast_research.ps1')
    }
    catch {
        Write-Warning "Independent forecast research did not start: $($_.Exception.GetType().Name). Core capture remains running."
    }
}

if (-not $NoBrowser) {
    Start-Process "http://127.0.0.1:8501"
}

Write-Host "Backend:   http://127.0.0.1:8000" -ForegroundColor Green
Write-Host "Dashboard: http://127.0.0.1:8501" -ForegroundColor Green
Write-Host "Backend launcher PID:   $($backendProcess.Id)" -ForegroundColor DarkGray
Write-Host "Dashboard launcher PID: $($dashboardProcess.Id)" -ForegroundColor DarkGray
}
catch {
    Write-StartupSupervisorLog -Event 'invocation_failed' -Message "error_type=$($_.Exception.GetType().Name)"
    throw
}
finally {
    Write-StartupSupervisorLog -Event 'supervisor_lock_released' -Message "mutex=$($supervisorLock.Name)"
    Exit-MarketAppSupervisorLock -LockHandle $supervisorLock
}
