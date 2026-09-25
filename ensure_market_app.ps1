[CmdletBinding()]
param(
    [switch]$RestartBackend,
    [switch]$RestartDashboard,
    [switch]$StartDashboardIfMissing,
    [switch]$PrepareUniverseOnly,
    [switch]$EnableRutCanary,
    [int]$ExpectedBackendPid = 0,
    [int]$ExpectedDashboardPid = 0,
    [ValidatePattern('^[A-Za-z0-9_.:@-]+$')][string]$Caller = 'DirectEnsure'
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$RuntimeLogDir = Join-Path $ProjectRoot 'logs\runtime'
$PythonExe = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$StreamlitExe = Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe'
$BackendEntrypoint = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot 'server.py'))
$DashboardEntrypoint = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot 'app.py'))
$SupervisorModule = Join-Path $ProjectRoot 'market_app_supervisor.psm1'
$ClosingTapeScript = Join-Path $ProjectRoot 'start_closing_tape.ps1'
$ClosingTapeFinalizeScript = Join-Path $ProjectRoot 'tools\finalize_closing_tape_if_due.py'
$OpeningAcceptancePreflightScript = Join-Path $ProjectRoot 'tools\preflight_opening_acceptance.py'
$OpeningAcceptancePreflightSchemaVersion = 'marketpin-opening-acceptance-preflight.v2'
$OpeningAcceptancePreflightRequiredFileCount = 67
$OpeningAcceptancePreflightRequiredModuleCount = 50
# September 9 retained live full phases needed 111.716 and 113.151 seconds near
# the open; later peers needed 101.906 and 107.919 seconds, while thirteen
# same-day attempts hit the former 120-second cutoff under session I/O pressure.
# A 150-second per-child bound adds bounded headroom while remaining below the
# five-minute watchdog cadence and the scheduled task's 15-minute limit.
$OpeningAcceptancePreflightTimeoutSeconds = 150
$InvocationId = [guid]::NewGuid().ToString('N')
# This proof is intentionally scoped to one ensure_market_app.ps1 invocation.
# Cache only a successful result; a failed or timed-out preflight must be
# attempted again by a later watchdog invocation rather than being remembered.
$script:OpeningAcceptancePreflightResult = $null

Import-Module -Name $SupervisorModule -Force -ErrorAction Stop

function Write-WatchdogLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Event = 'status'
    )

    Write-MarketAppSupervisorLog `
        -ProjectRoot $ProjectRoot `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -Event $Event `
        -Message $Message
}

function Write-WatchdogLogBestEffort {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Event = 'status'
    )

    # Terminal observability must never replace the invocation result. In
    # particular, a log-path failure while handling another exception must not
    # turn the logging error into the process's reported failure.
    try {
        Write-WatchdogLog -Event $Event -Message $Message
    }
    catch {
    }
}

function Test-PortListener {
    param([Parameter(Mandatory = $true)][int]$Port)

    return Test-MarketAppPortListener -Port $Port
}

function Get-BackendReadinessState {
    param(
        [string]$HealthUrl = 'http://127.0.0.1:8000/health',
        [string]$LiveUrl = 'http://127.0.0.1:8000/health/live'
    )

    $health = $null
    $live = $null
    $healthReachable = $false
    $liveReachable = $false
    try {
        $health = Invoke-RestMethod -Uri $HealthUrl -TimeoutSec 5 -ErrorAction Stop
        $healthReachable = $true
    }
    catch {
        # The pure recovery decision below classifies this without exposing an
        # endpoint exception (which may contain environment-specific details).
    }
    try {
        $live = Invoke-RestMethod -Uri $LiveUrl -TimeoutSec 5 -ErrorAction Stop
        $liveReachable = $true
    }
    catch {
        # Keep /health and /health/live evidence independent.
    }

    $sampler = if ($health) { $health.orb_reference_sampler } else { $null }
    return [pscustomobject]@{
        HealthReachable = $healthReachable
        LiveHealthReachable = $liveReachable
        HealthProvider = if ($health.market_data_provider) { [string]$health.market_data_provider } else { [string]$health.provider }
        LiveProvider = [string]$live.provider
        StreamingActive = [bool]$health.streaming_active
        SubscriptionAllowed = [bool]$health.subscription_allowed
        SubscriptionSessionState = [string]$health.subscription_session_state
        OrbSamplerAlive = [bool]$sampler.thread_alive
        OrbSamplerIntervalSeconds = if ($null -ne $sampler.interval_seconds) { [int]$sampler.interval_seconds } else { 0 }
        TradingDate = [string]$health.universe_provenance.trading_date
        HealthSubscriptionEpochId = [string]$health.subscription_epoch_id
        LiveSubscriptionEpochId = [string]$live.subscription_epoch_id
        HealthActiveGeneration = if ($null -ne $health.active_generation) { $health.active_generation } else { $health.subscription_generation }
        LiveActiveGeneration = if ($null -ne $live.active_generation) { $live.active_generation } else { $live.subscription_generation }
        HealthHandoffStatus = [string]$health.handoff_status
        LiveHandoffStatus = [string]$live.handoff_status
        IsFallback = [bool]$health.universe_provenance.is_fallback
        ProvenanceMode = [string]$health.universe_provenance.mode
        SourceDate = [string]$health.universe_provenance.source_date
        ConfiguredSymbols = @(
            $health.symbols_requested |
                ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
                Where-Object { $_ }
        )
        StreamConnected = [bool]$live.stream_connected
        StreamProgressing = [bool]$live.stream_progressing
        CollectionReady = [bool]$live.collection_ready
        CalculationReady = [bool]$live.calculation_ready
        PredictionPipelineOk = [bool]$live.prediction_pipeline_ok
        RequiredMissingSymbols = @($live.required_missing_symbols)
        RequiredInvalidSymbols = @($live.required_invalid_symbols)
        RequiredEpochMismatchSymbols = @($live.required_epoch_mismatch_symbols)
        RequiredGenerationMismatchSymbols = @($live.required_generation_mismatch_symbols)
        RequiredZeroFreshQuoteSymbols = @($live.required_zero_fresh_quote_symbols)
        RequiredStaleSymbols = @($live.required_stale_symbols)
        # Preserve exact JSON types for the narrow dead-handoff recovery gate.
        HealthEvidence = $health
        LiveEvidence = $live
    }
}

function Confirm-DeadHandoffRecoveryPreflight {
    param(
        [int]$ExpectedPid,
        [datetime]$ExpectedStartTime,
        [psobject]$BeforeState,
        [bool]$RecoveryAlreadyAttempted
    )
    try {
        Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
        # Read-only cache proof runs while the current stream still exists.
        # Never use provider discovery to authorize an intraday restart.
        $preparation = Invoke-MarketAppUniverseCachePreparation `
            -ProjectRoot $ProjectRoot -PythonExe $PythonExe -Symbols $env:DATABENTO_SYMBOLS `
            -Deadline ((Get-Date).AddSeconds(45)) `
            -InvocationId "$InvocationId-dead-handoff-prestop" -SkipProviderDiscovery
        $afterState = Get-BackendReadinessState
        $listenerPid = Assert-MarketAppExpectedListenerPid -Port 8000 -ExpectedPid $ExpectedPid
        $owned = Test-MarketAppVerifiedProcess -ProcessId $listenerPid -ProjectRoot $ProjectRoot `
            -RequiredCommandMarkers @('server.py', 'backend.app:app')
        $startTime = (Get-Process -Id $listenerPid -ErrorAction Stop).StartTime
        $decisionTime = Get-Date
        $approved = [bool](
            $startTime -eq $ExpectedStartTime -and
            (Test-MarketAppDeadHandoffRecoveryEligible -Now $decisionTime -ListenerCount 1 `
                -OwnershipVerified $owned -ListenerStartTime $startTime -RuntimeState $afterState `
                -RecoveryAlreadyAttempted $RecoveryAlreadyAttempted) -and
            (Test-MarketAppDeadHandoffCacheProof -BeforeState $BeforeState -AfterState $afterState `
                -Preparation $preparation -ExpectedSymbols @($env:DATABENTO_SYMBOLS -split ',') `
                -ExpectedContractCap ([int]$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS))
        )
        Write-WatchdogLog -Event 'dead_handoff_recovery_prevalidated' `
            -Message "component=backend listener_pid=$listenerPid approved=$approved selected_hash=$($preparation.Result.selected_universe_sha256) action=$(if ($approved) { 'allow_exact_owner_stop' } else { 'preserve_listener' })"
        return [pscustomobject]@{Approved=$approved;DecisionTime=$decisionTime}
    }
    catch {
        Write-WatchdogLog -Event 'dead_handoff_recovery_prevalidation_failed' `
            -Message "component=backend listener_pid=$ExpectedPid error_type=$($_.Exception.GetType().Name) action=preserve_listener"
        return [pscustomobject]@{Approved=$false;DecisionTime=(Get-Date)}
    }
}

function Get-BackendUniverseState {
    param([psobject]$RuntimeState = $null)

    $state = if ($RuntimeState) { $RuntimeState } else { Get-BackendReadinessState }
    $tradingDate = [string]$state.TradingDate
    if (-not $state.HealthReachable -or $tradingDate -notmatch '^\d{4}-\d{2}-\d{2}$') {
        return $null
    }
    return [pscustomobject]@{
        TradingDate = $tradingDate
        IsFallback = [bool]$state.IsFallback
        ProvenanceMode = [string]$state.ProvenanceMode
        SourceDate = [string]$state.SourceDate
        ConfiguredSymbols = @($state.ConfiguredSymbols)
    }
}

function Test-MarketAppCurrentDayUniversePreparationProof {
    [CmdletBinding()]
    param(
        [psobject]$Preparation,
        [Parameter(Mandatory = $true)]
        [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
        [string]$ExpectedTradingDate,
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$ExpectedContractCap,
        [Parameter(Mandatory = $true)]
        [string[]]$ExpectedSymbols
    )

    try {
        if ($null -eq $Preparation -or $null -eq $Preparation.Result) {
            return $false
        }
        $result = $Preparation.Result
        $provenance = $result.provenance
        if ($null -eq $provenance) {
            return $false
        }
        $selectedCountValue = $result.selected_contract_count
        $selectedCountIsInteger = [bool](
            $selectedCountValue -is [byte] -or
            $selectedCountValue -is [sbyte] -or
            $selectedCountValue -is [int16] -or
            $selectedCountValue -is [uint16] -or
            $selectedCountValue -is [int32] -or
            $selectedCountValue -is [uint32] -or
            $selectedCountValue -is [int64] -or
            $selectedCountValue -is [uint64]
        )
        if (-not $selectedCountIsInteger) {
            return $false
        }
        $selectedContractCount = [int64]$selectedCountValue
        $baseProofValid = [bool](
            $Preparation.StartupMayContinue -is [bool] -and
            $Preparation.StartupMayContinue -eq $true -and
            $Preparation.UsesFallback -is [bool] -and
            $Preparation.UsesFallback -eq $false -and
            [string]$Preparation.ProvenanceLabel -ceq 'CURRENT_DAY_CACHE' -and
            $result.current_day_cache_ready -is [bool] -and
            $result.current_day_cache_ready -eq $true -and
            $provenance.is_fallback -is [bool] -and
            $provenance.is_fallback -eq $false -and
            [string]$provenance.mode -ceq 'current_day_cache' -and
            [string]$provenance.trading_date -ceq $ExpectedTradingDate -and
            [string]$provenance.source_date -ceq $ExpectedTradingDate -and
            [string]$result.selected_universe_sha256 -cmatch '^[0-9a-f]{64}$' -and
            $selectedContractCount -ge 1 -and
            $selectedContractCount -le $ExpectedContractCap
        )
        if (-not $baseProofValid) {
            return $false
        }

        $normalizedExpectedSymbols = @(
            $ExpectedSymbols |
                ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
                Where-Object { $_ } |
                Select-Object -Unique
        )
        $reportedSymbols = @(
            $result.requested_symbols |
                ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
                Where-Object { $_ }
        )
        if (
            $normalizedExpectedSymbols.Count -eq 0 -or
            $reportedSymbols.Count -ne $normalizedExpectedSymbols.Count -or
            ($reportedSymbols -join ',') -cne ($normalizedExpectedSymbols -join ',')
        ) {
            return $false
        }

        $marketReadiness = $result.market_primary_readiness
        if ($null -eq $marketReadiness) {
            return $false
        }
        $readinessProperties = @($marketReadiness.PSObject.Properties)
        if ($readinessProperties.Count -ne $normalizedExpectedSymbols.Count) {
            return $false
        }
        $isIntegerValue = {
            param([object]$Value)
            return [bool](
                $Value -is [byte] -or
                $Value -is [sbyte] -or
                $Value -is [int16] -or
                $Value -is [uint16] -or
                $Value -is [int32] -or
                $Value -is [uint32] -or
                $Value -is [int64] -or
                $Value -is [uint64]
            )
        }
        $expectedDate = [datetime]::MinValue
        if (-not [datetime]::TryParseExact(
            $ExpectedTradingDate,
            'yyyy-MM-dd',
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::None,
            [ref]$expectedDate
        )) {
            return $false
        }

        foreach ($symbol in $normalizedExpectedSymbols) {
            $property = $marketReadiness.PSObject.Properties[$symbol]
            if ($null -eq $property -or $null -eq $property.Value) {
                return $false
            }
            $market = $property.Value
            if (
                $market.subscription_available -isnot [bool] -or
                $market.subscription_available -ne $true -or
                $market.admission_passes -isnot [bool] -or
                $market.admission_passes -ne $true -or
                -not (& $isIntegerValue $market.primary_plan_count) -or
                [int64]$market.primary_plan_count -ne 1 -or
                -not (& $isIntegerValue $market.primary_contract_count) -or
                -not (& $isIntegerValue $market.selected_strike_pairs) -or
                -not (& $isIntegerValue $market.minimum_pair_count) -or
                -not (& $isIntegerValue $market.complete_pair_count) -or
                -not (& $isIntegerValue $market.orb_reference_minimum_pair_count)
            ) {
                return $false
            }
            $primaryContractCount = [int64]$market.primary_contract_count
            $selectedStrikePairs = [int64]$market.selected_strike_pairs
            $minimumPairCount = [int64]$market.minimum_pair_count
            $completePairCount = [int64]$market.complete_pair_count
            $orbReferenceMinimumPairCount = [int64]$market.orb_reference_minimum_pair_count
            if (
                $minimumPairCount -lt 1 -or
                $selectedStrikePairs -lt $minimumPairCount -or
                $primaryContractCount -ne (2 * $selectedStrikePairs) -or
                $orbReferenceMinimumPairCount -lt 5 -or
                $completePairCount -lt $orbReferenceMinimumPairCount
            ) {
                return $false
            }

            $primaryExpirationText = [string]$market.primary_expiration
            $primaryExpiration = [datetime]::MinValue
            if (-not [datetime]::TryParseExact(
                $primaryExpirationText,
                'yyyy-MM-dd',
                [System.Globalization.CultureInfo]::InvariantCulture,
                [System.Globalization.DateTimeStyles]::None,
                [ref]$primaryExpiration
            )) {
                return $false
            }
            if (
                $market.primary_expiration_context_only -isnot [bool] -or
                $market.primary_expiration_same_day_authority -isnot [bool]
            ) {
                return $false
            }

            if ($symbol -ceq 'VIX') {
                if (
                    $primaryExpiration -le $expectedDate -or
                    [string]$market.primary_expiration_authority -cne 'vix_forward_expiration_context_only' -or
                    $market.primary_expiration_context_only -ne $true -or
                    $market.primary_expiration_same_day_authority -ne $false -or
                    [string]$market.primary_expiration_selection_basis -cne 'vix_last_trading_day_precedes_settlement_date'
                ) {
                    return $false
                }
            }
            elseif ($symbol -in @('SPX', 'NDX', 'RUT')) {
                if (
                    $primaryExpiration -lt $expectedDate -or
                    [string]$market.primary_expiration_authority -cne 'primary_expiration' -or
                    $market.primary_expiration_context_only -ne $false -or
                    [string]$market.primary_expiration_selection_basis -cne 'earliest_live_eligible_expiration'
                ) {
                    return $false
                }
                if (
                    $symbol -in @('SPX', 'NDX') -and (
                        $primaryExpiration -ne $expectedDate -or
                        $market.primary_expiration_same_day_authority -ne $true
                    )
                ) {
                    return $false
                }
            }
            else {
                return $false
            }
        }
        return $true
    }
    catch {
        return $false
    }
}

function Invoke-BackendPreparedAutomaticListenerStop {
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$ExpectedPid,
        [Parameter(Mandatory = $true)][datetime]$DecisionTime,
        [Parameter(Mandatory = $true)][datetime]$PreparationDeadline,
        [Parameter(Mandatory = $true)]
        [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
        [string]$ExpectedTradingDate,
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$ExpectedContractCap,
        [Parameter(Mandatory = $true)]
        [ValidateSet(
            'stale_backend_trading_date',
            'rut_canary_preopen_upgrade',
            'verified_prior_day_backend_preopen_refresh'
        )]
        [string]$RecoveryReason,
        [switch]$RequireCurrentDay,
        [switch]$AllowLateSessionSalvage
    )

    $preparation = $null
    try {
        Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
        $preparationNow = Get-Date
        $providerDiscoveryAllowed = Test-MarketAppUniverseProviderDiscoveryAllowed `
            -Now $preparationNow
        $preparation = Invoke-MarketAppUniverseCachePreparation `
            -ProjectRoot $ProjectRoot `
            -PythonExe $PythonExe `
            -Symbols $env:DATABENTO_SYMBOLS `
            -TradingDate $ExpectedTradingDate `
            -Deadline $PreparationDeadline `
            -InvocationId "$InvocationId-$RecoveryReason-prestop" `
            -SkipProviderDiscovery:(-not $providerDiscoveryAllowed)
    }
    catch {
        Write-WatchdogLog `
            -Event 'backend_replacement_universe_preparation_failed' `
            -Message "component=backend recovery_reason=$RecoveryReason error_type=$($_.Exception.GetType().Name) action=preserve_verified_listener"
        return [pscustomobject]@{
            Stopped = $false
            Reason = 'universe_preparation_exception'
            Preparation = $null
        }
    }

    $startupMayContinue = (
        $preparation.StartupMayContinue -is [bool] -and
        $preparation.StartupMayContinue -eq $true
    )
    if (-not $startupMayContinue) {
        Write-WatchdogLog `
            -Event 'backend_replacement_universe_preparation_blocked' `
            -Message "component=backend recovery_reason=$RecoveryReason outcome=$($preparation.Outcome) timed_out=$([bool]$preparation.TimedOut) deadline_ct=$($PreparationDeadline.ToString('HH:mm:ss')) action=preserve_verified_listener"
        return [pscustomobject]@{
            Stopped = $false
            Reason = 'universe_preparation_blocked'
            Preparation = $null
        }
    }

    $preparedTradingDate = [string]$preparation.Result.provenance.trading_date
    if ($preparedTradingDate -cne $ExpectedTradingDate) {
        Write-WatchdogLog `
            -Event 'backend_replacement_universe_preparation_blocked' `
            -Message "component=backend recovery_reason=$RecoveryReason outcome=trading_date_mismatch expected_trading_date=$ExpectedTradingDate observed_trading_date=$preparedTradingDate action=preserve_verified_listener"
        return [pscustomobject]@{
            Stopped = $false
            Reason = 'universe_preparation_trading_date_mismatch'
            Preparation = $null
        }
    }

    $currentDayReady = Test-MarketAppCurrentDayUniversePreparationProof `
        -Preparation $preparation `
        -ExpectedTradingDate $ExpectedTradingDate `
        -ExpectedContractCap $ExpectedContractCap `
        -ExpectedSymbols @($env:DATABENTO_SYMBOLS -split ',')
    if ($RequireCurrentDay -and -not $currentDayReady) {
        $deferredEvent = if ($RecoveryReason -ceq 'rut_canary_preopen_upgrade') {
            'rut_canary_preopen_upgrade_deferred'
        }
        else {
            'backend_replacement_current_day_universe_required'
        }
        Write-WatchdogLog `
            -Event $deferredEvent `
            -Message "component=backend recovery_reason=$RecoveryReason outcome=$($preparation.Outcome) label=$($preparation.ProvenanceLabel) action=preserve_verified_listener"
        return [pscustomobject]@{
            Stopped = $false
            Reason = if ($RecoveryReason -ceq 'rut_canary_preopen_upgrade') {
                'rut_upgrade_requires_current_day_universe'
            }
            else {
                'prior_day_refresh_requires_current_day_universe'
            }
            Preparation = $null
        }
    }

    Write-WatchdogLog `
        -Event 'backend_replacement_universe_prepared' `
        -Message "component=backend recovery_reason=$RecoveryReason outcome=$($preparation.Outcome) label=$($preparation.ProvenanceLabel) trading_date=$($preparation.Result.provenance.trading_date) action=request_exact_owner_stop"
    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
    $automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop `
        -Port 8000 `
        -ExpectedPid $ExpectedPid `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers @('server.py', 'backend.app:app') `
        -Component 'backend' `
        -DecisionTime $DecisionTime `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -RecoveryReason $RecoveryReason `
        -AllowLateSessionSalvage:$AllowLateSessionSalvage
    return [pscustomobject]@{
        Stopped = [bool]$automaticStop.Stopped
        Reason = [string]$automaticStop.Reason
        Preparation = $preparation
    }
}

function Test-ReadinessRecoveryAlreadyAttempted {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('backend', 'dashboard')]
        [string]$Component,
        [Parameter(Mandatory = $true)][string]$TradingDate
    )

    $state = Get-MarketAppSessionRecoveryJournalState `
        -ProjectRoot $ProjectRoot `
        -Component $Component `
        -TradingDate $TradingDate
    return [bool]$state.Succeeded
}

function Test-ReadinessRecoveryPending {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('backend', 'dashboard')]
        [string]$Component,
        [Parameter(Mandatory = $true)][string]$TradingDate
    )

    $state = Get-MarketAppSessionRecoveryJournalState `
        -ProjectRoot $ProjectRoot `
        -Component $Component `
        -TradingDate $TradingDate
    return [bool]($state.Requested -and -not $state.Succeeded)
}

function Test-BackendReadinessRecoveryAlreadyAttempted {
    param([Parameter(Mandatory = $true)][string]$TradingDate)

    return Test-ReadinessRecoveryAlreadyAttempted `
        -Component 'backend' `
        -TradingDate $TradingDate
}

function Test-DashboardReadinessRecoveryAlreadyAttempted {
    param([Parameter(Mandatory = $true)][string]$TradingDate)

    return Test-ReadinessRecoveryAlreadyAttempted `
        -Component 'dashboard' `
        -TradingDate $TradingDate
}

function Stop-VerifiedComponentListener {
    param(
        [Parameter(Mandatory = $true)][int]$Port,
        [Parameter(Mandatory = $true)][int]$ExpectedPid,
        [Parameter(Mandatory = $true)][string[]]$RequiredCommandMarkers,
        [Parameter(Mandatory = $true)][string]$Component
    )

    $listenerPid = Assert-MarketAppExpectedListenerPid -Port $Port -ExpectedPid $ExpectedPid
    if (-not (Test-MarketAppVerifiedProcess `
        -ProcessId $listenerPid `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers $RequiredCommandMarkers)) {
        throw "Refusing to stop $Component listener PID $listenerPid on port $Port because it is not a verified MarketPinPredictor process."
    }

    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null

    # The full source preflight is deliberately bounded but can still take
    # long enough for listener ownership to change. Re-resolve the exact PID
    # and ownership immediately before the stop rather than trusting the
    # evidence collected before the preflight.
    $listenerPid = Assert-MarketAppExpectedListenerPid -Port $Port -ExpectedPid $ExpectedPid
    if (-not (Test-MarketAppVerifiedProcess `
        -ProcessId $listenerPid `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers $RequiredCommandMarkers)) {
        throw "Refusing to stop $Component listener PID $listenerPid on port $Port because it is not a verified MarketPinPredictor process."
    }

    Write-WatchdogLog `
        -Event 'stop_verified_listener' `
        -Message "component=$Component port=$Port listener_pid=$listenerPid action=stop"
    Stop-Process -Id $listenerPid -Force -ErrorAction Stop

    $quiescence = Wait-MarketAppProcessNetworkQuiescence `
        -ProcessId $listenerPid `
        -TimeoutSeconds 30
    if (-not $quiescence.Quiescent) {
        throw "$Component PID $listenerPid did not reach exact-PID network quiescence after stop: process_exists=$($quiescence.ProcessExists) tcp_connection_count=$($quiescence.TcpConnectionCount)."
    }
    Write-WatchdogLog `
        -Event 'stop_verified_quiescent' `
        -Message "component=$Component port=$Port listener_pid=$listenerPid process_exited=true tcp_connection_count=0 quiescence_elapsed_ms=$($quiescence.ElapsedMilliseconds) action=continue"
}

function Start-VerifiedComponent {
    param(
        [Parameter(Mandatory = $true)][string]$Component,
        [Parameter(Mandatory = $true)][int]$Port,
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract,
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$StandardOutputPath,
        [Parameter(Mandatory = $true)][string]$StandardErrorPath
    )

    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
    Write-WatchdogLog -Event 'launch_requested' -Message "component=$Component port=$Port action=start"
    $launch = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentList `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $StandardOutputPath `
        -RedirectStandardError $StandardErrorPath `
        -WindowStyle Hidden `
        -PassThru
    Write-WatchdogLog `
        -Event 'launch_started' `
        -Message "component=$Component port=$Port launch_pid=$($launch.Id)"

    $listenerPid = Wait-MarketAppOwnedEndpoint `
        -Url $Url `
        -Port $Port `
        -LaunchProcessId $launch.Id `
        -ExpectedEndpointContract $ExpectedEndpointContract `
        -TimeoutSeconds 90
    if (-not $listenerPid) {
        Write-WatchdogLog `
            -Event 'launch_ownership_failed' `
            -Message "component=$Component port=$Port launch_pid=$($launch.Id) action=cleanup_attempted_launch"
        Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
        Stop-MarketAppAttemptedLaunch -LaunchProcessId $launch.Id
        throw "$Component did not acquire port $Port and become HTTP-ready as a descendant of launch PID $($launch.Id)."
    }

    Write-WatchdogLog `
        -Event 'launch_succeeded' `
        -Message "component=$Component port=$Port launch_pid=$($launch.Id) listener_pid=$listenerPid"
    return [pscustomobject]@{
        Component = $Component
        LaunchProcessId = [int]$launch.Id
        ListenerProcessId = [int]$listenerPid
    }
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
        Write-WatchdogLog `
            -Event 'opening_acceptance_preflight_failed' `
            -Message 'component=source reason=preflight_script_missing action=abort_before_process_change'
        throw "Opening-acceptance preflight script was not found: $PreflightScript"
    }

    $preflightArguments = @(
        '-B',
        ('"' + ([System.IO.Path]::GetFullPath($PreflightScript)) + '"'),
        '--project-root',
        ('"' + ([System.IO.Path]::GetFullPath($ProjectRoot)) + '"'),
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
            -ProjectRoot $ProjectRoot `
            -FilePath $PythonPath `
            -ArgumentList $preflightArguments `
            -InvocationId "$InvocationId-opening-preflight" `
            -LogComponent 'opening-acceptance-preflight' `
            -TimeoutSeconds $TimeoutSeconds
    }
    catch {
        Write-WatchdogLog `
            -Event 'opening_acceptance_preflight_failed' `
            -Message "component=source reason=child_launch_failed error_type=$($_.Exception.GetType().Name) timeout_seconds=$TimeoutSeconds action=abort_before_process_change"
        throw 'Opening-acceptance source/schema preflight child could not be started.'
    }
    if ($child.TimedOut) {
        Write-WatchdogLog `
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
                [System.IO.Path]::GetFullPath($ProjectRoot),
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
        Write-WatchdogLog `
            -Event 'opening_acceptance_preflight_failed' `
            -Message "component=source phase=$Phase child_pid=$($child.ProcessId) exit_code=$(if ($hasPreflightExitCode) { $preflightExitCode } else { 'unavailable' }) elapsed_ms=$($child.ElapsedMilliseconds) issues=$issueSummary action=abort_before_process_change"
        throw "Opening-acceptance source/schema preflight failed (phase=$Phase issues=$issueSummary)."
    }

    Write-WatchdogLog `
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
            -DatabasePath (Join-Path $ProjectRoot 'data\market_data.db') `
            -Phase 'fingerprint' `
            -ExpectedSourceFingerprint $cachedFingerprint `
            -TimeoutSeconds $OpeningAcceptancePreflightTimeoutSeconds | Out-Null
        return $script:OpeningAcceptancePreflightResult
    }

    # Assign only after Invoke-OpeningAcceptancePreflight returns a successful
    # proof. There is no timestamp, TTL, file, or cross-invocation cache.
    $initialPhase = if ($AllowSchemaBootstrap) {
        'bootstrap-eligibility'
    }
    else {
        'strict'
    }
    $preflight = Invoke-OpeningAcceptancePreflight `
        -PythonPath $PythonExe `
        -PreflightScript $OpeningAcceptancePreflightScript `
        -DatabasePath (Join-Path $ProjectRoot 'data\market_data.db') `
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
                    -ProjectRoot $ProjectRoot `
                    -RequiredCommandMarkers @('server.py', 'backend.app:app'))
            ) {
                $bootstrapBlockReason = 'backend_listener_ownership_unverified'
            }
            elseif (
                $dashboardApplicationPids.Count -eq 1 -and
                -not (Test-MarketAppVerifiedProcess `
                    -ProcessId ([int]$dashboardApplicationPids[0]) `
                    -ProjectRoot $ProjectRoot `
                    -RequiredCommandMarkers @('streamlit', 'app.py'))
            ) {
                $bootstrapBlockReason = 'dashboard_listener_ownership_unverified'
            }
        }
        if ($bootstrapBlockReason) {
            Write-WatchdogLog `
                -Event 'opening_acceptance_schema_bootstrap_blocked' `
                -Message "component=database scope=$bootstrapScope reason=$bootstrapBlockReason listener_pids=$($activeApplicationPids -join ',') action=abort_without_schema_change"
            throw "Opening-acceptance schema bootstrap was blocked (scope=$bootstrapScope reason=$bootstrapBlockReason)."
        }

        Write-WatchdogLog `
            -Event 'opening_acceptance_schema_bootstrap_requested' `
            -Message "component=database scope=$bootstrapScope database_contract=$($preflight.database_contract) issue_count=$(@($preflight.database_issues).Count) action=run_bounded_additive_schema_bootstrap"
        $preflight = Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $ProjectRoot 'data\market_data.db') `
            -Phase 'bootstrap' `
            -ExpectedSourceFingerprint ([string]$preflight.source_fingerprint_sha256) `
            -BootstrapScope $bootstrapScope `
            -TimeoutSeconds $OpeningAcceptancePreflightTimeoutSeconds
        Write-WatchdogLog `
            -Event 'opening_acceptance_schema_bootstrap_passed' `
            -Message "component=database bootstrap_attempt_count=$($preflight.bootstrap_attempt_count) source_fingerprint=$($preflight.source_fingerprint_sha256) action=cache_strict_postcondition"
    }
    $script:OpeningAcceptancePreflightResult = $preflight
    return $script:OpeningAcceptancePreflightResult
}

function Invoke-MarketAppCurrentDayUniversePrestage {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRootPath,
        [Parameter(Mandatory = $true)][string]$PythonPath,
        [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z0-9_,]+$')][string]$Symbols,
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$ExpectedContractCap,
        [Parameter(Mandatory = $true)][datetime]$Now,
        [Parameter(Mandatory = $true)][datetime]$NotAfter,
        [Parameter(Mandatory = $true)][string]$PrestageInvocationId
    )

    if ($Now -ge $NotAfter) {
        throw 'Current-day Databento universe pre-stage reached its protected 07:40 CT boundary.'
    }

    $expectedTradingDate = $Now.ToString('yyyy-MM-dd')
    $deadline = Get-MarketAppUniversePreparationDeadline `
        -Now $Now `
        -NotAfter $NotAfter
    $providerDiscoveryAllowed = Test-MarketAppUniverseProviderDiscoveryAllowed `
        -Now $Now `
        -OpeningProtectionBoundary $NotAfter
    $preparation = Invoke-MarketAppUniverseCachePreparation `
        -ProjectRoot $ProjectRootPath `
        -PythonExe $PythonPath `
        -Symbols $Symbols `
        -TradingDate $expectedTradingDate `
        -Deadline $deadline `
        -InvocationId $PrestageInvocationId `
        -SkipProviderDiscovery:(-not $providerDiscoveryAllowed)

    $currentDayProofValid = Test-MarketAppCurrentDayUniversePreparationProof `
        -Preparation $preparation `
        -ExpectedTradingDate $expectedTradingDate `
        -ExpectedContractCap $ExpectedContractCap `
        -ExpectedSymbols @($Symbols -split ',')
    if (-not $currentDayProofValid) {
        $outcome = if ($preparation) { [string]$preparation.Outcome } else { 'unavailable' }
        throw "Current-day Databento universe pre-stage failed closed (outcome=$outcome)."
    }

    return $preparation
}

$invocationOutcome = 'failed'
$invocationTerminalPath = 'unhandled_error'
$invocationErrorType = 'none'
$normalizedExitCode = 1

try {
    Write-WatchdogLog `
        -Event 'invocation_started' `
        -Message "restart_backend=$([bool]$RestartBackend) expected_backend_pid=$ExpectedBackendPid restart_dashboard=$([bool]$RestartDashboard) expected_dashboard_pid=$ExpectedDashboardPid start_dashboard_if_missing=$([bool]$StartDashboardIfMissing) prepare_universe_only=$([bool]$PrepareUniverseOnly)"

if ($PrepareUniverseOnly -and (
    $RestartBackend -or $RestartDashboard -or $StartDashboardIfMissing
)) {
    Write-WatchdogLog -Event 'invalid_request' -Message 'component=universe reason=prestage_component_action_conflict'
    throw '-PrepareUniverseOnly cannot be combined with a component start or restart action.'
}

if ($RestartDashboard -and $StartDashboardIfMissing) {
    Write-WatchdogLog -Event 'invalid_request' -Message 'component=dashboard reason=conflicting_restart_and_start_if_missing'
    throw '-RestartDashboard and -StartDashboardIfMissing are mutually exclusive.'
}

if ($RestartBackend -and (
    -not $PSBoundParameters.ContainsKey('ExpectedBackendPid') -or $ExpectedBackendPid -le 0
)) {
    Write-WatchdogLog -Event 'invalid_request' -Message 'component=backend reason=missing_expected_listener_pid'
    throw '-RestartBackend requires -ExpectedBackendPid from the immediately preceding verified listener check.'
}
if ($RestartDashboard -and (
    -not $PSBoundParameters.ContainsKey('ExpectedDashboardPid') -or $ExpectedDashboardPid -le 0
)) {
    Write-WatchdogLog -Event 'invalid_request' -Message 'component=dashboard reason=missing_expected_listener_pid'
    throw '-RestartDashboard requires -ExpectedDashboardPid from the immediately preceding verified listener check.'
}

$now = Get-Date
$watchStart = $now.Date.AddHours(7).AddMinutes(45)
$watchEnd = $now.Date.AddHours(18)
if (
    $now.DayOfWeek -in @([DayOfWeek]::Saturday, [DayOfWeek]::Sunday) -or
    $now -lt $watchStart -or
    $now -gt $watchEnd
) {
    if (-not ($RestartBackend -or $RestartDashboard -or $StartDashboardIfMissing -or $PrepareUniverseOnly)) {
        Write-WatchdogLog -Event 'outside_watch_window' -Message 'action=noop'
        $invocationOutcome = 'noop'
        $invocationTerminalPath = 'outside_watch_window'
        $normalizedExitCode = 0
        exit 0
    }
}

if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Project Python was not found: $PythonExe"
}
if (-not $PrepareUniverseOnly) {
    if (-not (Test-Path -LiteralPath $StreamlitExe)) {
        throw "Project Streamlit was not found: $StreamlitExe"
    }
    if (-not (Test-Path -LiteralPath $ClosingTapeScript -PathType Leaf)) {
        throw "Closing-tape launcher was not found: $ClosingTapeScript"
    }
    if (-not (Test-Path -LiteralPath $ClosingTapeFinalizeScript -PathType Leaf)) {
        throw "Closing-tape post-close finalizer was not found: $ClosingTapeFinalizeScript"
    }
}

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

$sqlitePath = (Join-Path $ProjectRoot 'data\market_data.db').Replace('\', '/')
$env:DATABASE_URL = "sqlite:///$sqlitePath"
$env:MARKET_DATA_PROVIDER = 'databento'
# Protect opening SPX/NDX capacity. ETF families remain supported canaries but
# are not part of the launch-critical subscription.
$env:DATABENTO_SYMBOLS = if ($EnableRutCanary) { 'SPX,NDX,VIX,RUT' } else { 'SPX,NDX,VIX' }
$env:DATABENTO_RUT_CANARY_ENABLED = if ($EnableRutCanary) { '1' } else { '0' }
# Keep the guarded RUT path within its independently measured subscription
# budget. The per-expiration shadow bound below controls non-primary breadth.
$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS = if ($EnableRutCanary) { '3200' } else { '3600' }
$env:DATABENTO_REPLAY_MINUTES = '0'
$env:DATABENTO_SNAPSHOT_INTERVAL_SECONDS = '60'
$env:PREDICTION_CAPTURE_INTERVAL_SECONDS = '60'
$env:DATABENTO_USE_MULTI_EXPIRATION = '0'
$env:DATABENTO_SUBSCRIPTION_PROFILE = 'near-term-shadow'
# Leave the primary-expiration bound unchanged and preserve the required
# 50-pair next-listed reservation while trimming surplus pairs from each
# non-primary expiration that contributed to regular-session queue load.
$env:DATABENTO_SHADOW_MAX_STRIKE_PAIRS = '50'
$env:DATABENTO_ALLOW_PRIOR_UNIVERSE_FALLBACK = '1'
$env:DATABENTO_UNIVERSE_FALLBACK_MAX_AGE_DAYS = '4'
$env:DATABENTO_UNIVERSE_FALLBACK_REFRESH_SECONDS = '28800'
$env:DATABENTO_QUOTE_FRESHNESS_SECONDS = '30'
$env:DATABENTO_HANDOFF_TIMEOUT_SECONDS = '120'
$env:DATABENTO_UNIVERSE_REFRESH_SECONDS = '28800'
$env:DATABENTO_STREAM_STALL_SECONDS = '45'
$env:DATABENTO_PROGRESS_WINDOW_SECONDS = '20'
$env:DATABENTO_COMPUTE_WARMUP_GRACE_SECONDS = '30'
$env:DATABENTO_OVERLOAD_COMPUTE_BACKOFF_SECONDS = '30'
Remove-Item Env:DATABENTO_REFRESH_CACHE -ErrorAction SilentlyContinue

$supervisorLock = Enter-MarketAppSupervisorLock -ProjectRoot $ProjectRoot -TimeoutMilliseconds 0
if (-not $supervisorLock.Acquired) {
    Write-WatchdogLog `
        -Event 'supervisor_busy' `
        -Message "mutex=$($supervisorLock.Name) action=noop"
    Exit-MarketAppSupervisorLock -LockHandle $supervisorLock
    if ($RestartBackend -or $RestartDashboard -or $PrepareUniverseOnly) {
        if ($PrepareUniverseOnly) {
            throw 'Another MarketPinPredictor supervisor invocation is active; current-day universe pre-stage was not attempted.'
        }
        throw 'Another MarketPinPredictor supervisor invocation is already active; explicit restart was not attempted.'
    }
    $invocationOutcome = 'noop'
    $invocationTerminalPath = 'supervisor_busy'
    $normalizedExitCode = 0
    exit 0
}

Write-WatchdogLog -Event 'supervisor_lock_acquired' -Message "mutex=$($supervisorLock.Name)"
try {
    if ($PrepareUniverseOnly) {
        $prestageNow = Get-Date
        $prestageNotAfter = $prestageNow.Date.AddHours(7).AddMinutes(40)
        try {
            Assert-OpeningAcceptanceMutationPreflight `
                -AllowSchemaBootstrap | Out-Null
            # Start the preparation budget after preflight, which can consume
            # up to 150 seconds itself. Keep the absolute 07:40 cutoff intact.
            $prestageNow = Get-Date
            $prestage = Invoke-MarketAppCurrentDayUniversePrestage `
                -ProjectRootPath $ProjectRoot `
                -PythonPath $PythonExe `
                -Symbols $env:DATABENTO_SYMBOLS `
                -ExpectedContractCap ([int]$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS) `
                -Now $prestageNow `
                -NotAfter $prestageNotAfter `
                -PrestageInvocationId "$InvocationId-0700-prestage"
        }
        catch {
            Write-WatchdogLog `
                -Event 'universe_current_day_prestage_failed' `
                -Message "component=universe trading_date=$($prestageNow.ToString('yyyy-MM-dd')) error_type=$($_.Exception.GetType().Name) action=abstain_without_component_change"
            throw
        }
        Write-WatchdogLog `
            -Event 'universe_current_day_prestage_ready' `
            -Message "component=universe trading_date=$($prestage.Result.provenance.trading_date) symbols=$($env:DATABENTO_SYMBOLS) selected_contract_count=$($prestage.Result.selected_contract_count) selected_hash=$($prestage.Result.selected_universe_sha256) action=return_without_component_change"
    }
    else {
    $backendWasStopped = $false
    $preparedBackendUniverse = $null
    $dashboardWasStopped = $false
    $explicitComponentAction = [bool](
        $RestartBackend -or $RestartDashboard -or $StartDashboardIfMissing
    )
    $manageBackend = -not $explicitComponentAction -or [bool]$RestartBackend
    $manageDashboard = (
        -not $explicitComponentAction -or
        [bool]$RestartDashboard -or
        [bool]$StartDashboardIfMissing
    )
    $manageRecorder = -not $explicitComponentAction
    $currentTradingDate = $now.ToString('yyyy-MM-dd')
    $backendRecoveryAlreadySucceeded = if ($manageBackend) {
        Test-BackendReadinessRecoveryAlreadyAttempted -TradingDate $currentTradingDate
    }
    else { $false }
    $dashboardRecoveryAlreadySucceeded = if ($manageDashboard) {
        Test-DashboardReadinessRecoveryAlreadyAttempted -TradingDate $currentTradingDate
    }
    else { $false }
    $backendRecoveryCompletionPending = if ($manageBackend) {
        Test-ReadinessRecoveryPending -Component 'backend' -TradingDate $currentTradingDate
    }
    else { $false }
    $dashboardRecoveryCompletionPending = if ($manageDashboard) {
        Test-ReadinessRecoveryPending -Component 'dashboard' -TradingDate $currentTradingDate
    }
    else { $false }
    $backendSessionRecoveryReason = if ($backendRecoveryCompletionPending) {
        'pending_prior_invocation'
    }
    else { $null }
    $dashboardSessionRecoveryReason = if ($dashboardRecoveryCompletionPending) {
        'pending_prior_invocation'
    }
    else { $null }

    if ($RestartBackend) {
        Stop-VerifiedComponentListener `
            -Port 8000 `
            -ExpectedPid $ExpectedBackendPid `
            -RequiredCommandMarkers @('server.py', 'backend.app:app') `
            -Component 'backend'
        $backendWasStopped = $true
    }
    if ($RestartDashboard) {
        Stop-VerifiedComponentListener `
            -Port 8501 `
            -ExpectedPid $ExpectedDashboardPid `
            -RequiredCommandMarkers @('streamlit', 'app.py') `
            -Component 'dashboard'
        $dashboardWasStopped = $true
    }

    if ($manageBackend) {
        $backendListenerPresent = Test-PortListener -Port 8000
        if ($backendListenerPresent -and -not $backendWasStopped) {
            $expectedTradingDate = $now.ToString('yyyy-MM-dd')
            $backendReadinessState = Get-BackendReadinessState
            $backendUniverseState = Get-BackendUniverseState -RuntimeState $backendReadinessState
            $evaluateBackendReadiness = $false
            $observedTradingDate = if ($backendUniverseState) {
                [string]$backendUniverseState.TradingDate
            }
            else {
                $null
            }
            if ($observedTradingDate -and $observedTradingDate -ne $expectedTradingDate) {
                if ($backendRecoveryAlreadySucceeded) {
                    Write-WatchdogLog `
                        -Event 'backend_session_recovery_deferred' `
                        -Message "component=backend trading_date=$expectedTradingDate observed_trading_date=$observedTradingDate reason=session_recovery_already_succeeded action=preserve_listener"
                }
                else {
                    $backendPids = @(Get-MarketAppListenerProcessIds -Port 8000)
                    if ($backendPids.Count -ne 1) {
                        throw "Stale backend trading date was observed, but port 8000 did not resolve to exactly one listener PID."
                    }
                    # The invocation caches this proof; the stop helper's
                    # recheck must not consume the provider preparation budget.
                    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                    $stalePreparationNow = Get-Date
                    $stalePreparationDeadline = Get-MarketAppUniversePreparationDeadline `
                        -Now $stalePreparationNow `
                        -NotAfter $stalePreparationNow.Date.AddHours(8).AddMinutes(25)
                    Write-WatchdogLog `
                        -Event 'stale_backend_trading_date' `
                        -Message "component=backend observed_trading_date=$observedTradingDate expected_trading_date=$expectedTradingDate action=stage_replacement_universe"
                    $preparedStop = Invoke-BackendPreparedAutomaticListenerStop `
                        -ExpectedPid ([int]$backendPids[0]) `
                        -DecisionTime $now `
                        -PreparationDeadline $stalePreparationDeadline `
                        -ExpectedTradingDate $expectedTradingDate `
                        -ExpectedContractCap ([int]$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS) `
                        -RecoveryReason 'stale_backend_trading_date' `
                        -AllowLateSessionSalvage:($now -ge $now.Date.AddHours(8).AddMinutes(25))
                    if ($preparedStop.Stopped) {
                        $preparedBackendUniverse = $preparedStop.Preparation
                        Write-WatchdogLog `
                            -Event 'backend_session_recovery_requested' `
                            -Message "component=backend trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) reason=stale_backend_trading_date action=launch_from_prevalidated_universe"
                        $backendWasStopped = $true
                        $backendListenerPresent = $false
                        $backendRecoveryCompletionPending = $true
                        $backendSessionRecoveryReason = 'stale_backend_trading_date'
                    }
                }
            }
            elseif (
                $backendUniverseState -and
                [bool]$backendUniverseState.IsFallback -and
                $now -lt $now.Date.AddHours(8).AddMinutes(25)
            ) {
                Write-WatchdogLog `
                    -Event 'universe_fallback_recovery_requested' `
                    -Message "component=backend trading_date=$observedTradingDate source_date=$($backendUniverseState.SourceDate) mode=$($backendUniverseState.ProvenanceMode) action=stage_current_day_cache"
                $recoveryPreparation = $null
                try {
                    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                    $recoveryPreparationNow = Get-Date
                    $recoveryPreparationDeadline = Get-MarketAppUniversePreparationDeadline `
                        -Now $recoveryPreparationNow `
                        -NotAfter $recoveryPreparationNow.Date.AddHours(8).AddMinutes(25)
                    $recoveryProviderDiscoveryAllowed = Test-MarketAppUniverseProviderDiscoveryAllowed `
                        -Now $recoveryPreparationNow
                    $recoveryPreparation = Invoke-MarketAppUniverseCachePreparation `
                        -ProjectRoot $ProjectRoot `
                        -PythonExe $PythonExe `
                        -Symbols $env:DATABENTO_SYMBOLS `
                        -Deadline $recoveryPreparationDeadline `
                        -InvocationId "$InvocationId-fallback-recovery" `
                        -SkipProviderDiscovery:(-not $recoveryProviderDiscoveryAllowed)
                }
                catch {
                    $safeRecoveryError = ([string]$_.Exception.Message -replace '[\r\n]+', ' ' -replace '\s+', '_')
                    Write-WatchdogLog `
                        -Event 'universe_fallback_recovery_failed' `
                        -Message "component=backend error=$safeRecoveryError action=preserve_verified_listener"
                }
                if ($recoveryPreparation -and -not $recoveryPreparation.StartupMayContinue) {
                    Write-WatchdogLog `
                        -Event 'universe_fallback_recovery_failed_closed' `
                        -Message "component=backend outcome=$($recoveryPreparation.Outcome) deadline_ct=$($recoveryPreparationDeadline.ToString('HH:mm:ss')) action=preserve_verified_listener"
                    $recoveryPreparation = $null
                }
                if ($recoveryPreparation) {
                    if ($recoveryPreparation.UsesFallback) {
                        Write-WatchdogLog `
                            -Event 'universe_fallback_recovery_pending' `
                            -Message "component=backend source_date=$($recoveryPreparation.Result.provenance.source_date) trading_date=$($recoveryPreparation.Result.provenance.trading_date) action=preserve_verified_listener"
                    }
                    elseif (-not (Test-MarketAppCurrentDayUniversePreparationProof `
                        -Preparation $recoveryPreparation `
                        -ExpectedTradingDate $expectedTradingDate `
                        -ExpectedContractCap ([int]$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS) `
                        -ExpectedSymbols @($env:DATABENTO_SYMBOLS -split ','))) {
                        Write-WatchdogLog `
                            -Event 'universe_fallback_recovery_proof_failed' `
                            -Message "component=backend expected_trading_date=$expectedTradingDate action=preserve_verified_listener"
                    }
                    else {
                        $backendPids = @(Get-MarketAppListenerProcessIds -Port 8000)
                        if ($backendPids.Count -ne 1) {
                            throw 'Current-day universe cache was prepared, but port 8000 did not resolve to exactly one listener PID.'
                        }
                        Write-WatchdogLog `
                            -Event 'universe_fallback_recovered' `
                            -Message "component=backend trading_date=$($recoveryPreparation.Result.provenance.trading_date) action=request_bounded_stop"
                        Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                        $automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop `
                            -Port 8000 `
                            -ExpectedPid ([int]$backendPids[0]) `
                            -ProjectRoot $ProjectRoot `
                            -RequiredCommandMarkers @('server.py', 'backend.app:app') `
                            -Component 'backend' `
                            -DecisionTime $now `
                            -InvocationId $InvocationId `
                            -Caller $Caller `
                            -RecoveryReason 'universe_fallback_recovered'
                        if ($automaticStop.Stopped) {
                            # Reuse the cache proof that authorized this exact
                            # stop; a second preparation can time out or regress
                            # to fallback after the healthy owner is gone.
                            $preparedBackendUniverse = $recoveryPreparation
                            $backendWasStopped = $true
                            $backendListenerPresent = $false
                        }
                    }
                }
            }
            elseif ($backendUniverseState -and [bool]$backendUniverseState.IsFallback) {
                Write-WatchdogLog `
                    -Event 'universe_fallback_recovery_missed_opening_boundary' `
                    -Message "component=backend trading_date=$observedTradingDate source_date=$($backendUniverseState.SourceDate) action=assess_current_session_contract"
                $evaluateBackendReadiness = $true
            }
            elseif (
                $EnableRutCanary -and
                $backendUniverseState -and
                'RUT' -notin @($backendUniverseState.ConfiguredSymbols)
            ) {
                # A clock repair after the 07:45 attempt can make the guarded
                # RUT canary eligible. Upgrade only while enough pre-open time
                # remains to stage the current universe and regain a healthy
                # listener; never punch a gap into an in-progress opening range.
                $rutUpgradeDeadline = $now.Date.AddHours(8).AddMinutes(25)
                if ($now -lt $rutUpgradeDeadline) {
                    $backendPids = @(Get-MarketAppListenerProcessIds -Port 8000)
                    if ($backendPids.Count -ne 1) {
                        throw 'RUT canary upgrade was requested, but port 8000 did not resolve to exactly one listener PID.'
                    }
                    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                    $rutPreparationNow = Get-Date
                    $rutPreparationDeadline = Get-MarketAppUniversePreparationDeadline `
                        -Now $rutPreparationNow `
                        -NotAfter $rutUpgradeDeadline
                    Write-WatchdogLog `
                        -Event 'rut_canary_preopen_upgrade' `
                        -Message "component=backend configured_symbols=$(@($backendUniverseState.ConfiguredSymbols) -join ',') deadline_ct=$($rutUpgradeDeadline.ToString('HH:mm:ss')) action=stage_current_day_universe"
                    $preparedStop = Invoke-BackendPreparedAutomaticListenerStop `
                        -ExpectedPid ([int]$backendPids[0]) `
                        -DecisionTime $now `
                        -PreparationDeadline $rutPreparationDeadline `
                        -ExpectedTradingDate $expectedTradingDate `
                        -ExpectedContractCap ([int]$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS) `
                        -RecoveryReason 'rut_canary_preopen_upgrade' `
                        -RequireCurrentDay
                    if ($preparedStop.Stopped) {
                        $preparedBackendUniverse = $preparedStop.Preparation
                        $backendWasStopped = $true
                        $backendListenerPresent = $false
                    }
                }
                else {
                    Write-WatchdogLog `
                        -Event 'rut_canary_upgrade_missed_opening_boundary' `
                        -Message "component=backend configured_symbols=$(@($backendUniverseState.ConfiguredSymbols) -join ',') deadline_ct=$($rutUpgradeDeadline.ToString('HH:mm:ss')) action=preserve_verified_listener"
                    # RUT stays absent, but the protected live core still gets a
                    # read-only readiness assessment (never a late restart).
                    $evaluateBackendReadiness = $true
                }
            }
            elseif (-not $observedTradingDate) {
                $backendPids = @(Get-MarketAppListenerProcessIds -Port 8000)
                $listenerOwnershipVerified = $false
                $listenerStartTime = [datetime]::MinValue
                if ($backendPids.Count -eq 1) {
                    $listenerOwnershipVerified = Test-MarketAppVerifiedProcess `
                        -ProcessId ([int]$backendPids[0]) `
                        -ProjectRoot $ProjectRoot `
                        -RequiredCommandMarkers @('server.py', 'backend.app:app')
                    if ($listenerOwnershipVerified) {
                        try {
                            $listenerStartTime = (Get-Process -Id ([int]$backendPids[0]) -ErrorAction Stop).StartTime
                        }
                        catch {
                            $listenerStartTime = [datetime]::MinValue
                        }
                    }
                }
                $missingDateDecision = Resolve-MarketAppMissingTradingDateAction `
                    -Now $now `
                    -ListenerCount $backendPids.Count `
                    -OwnershipVerified $listenerOwnershipVerified `
                    -ListenerStartTime $listenerStartTime `
                    -RecoveryAlreadySucceeded $backendRecoveryAlreadySucceeded
                if ($missingDateDecision.Action -eq 'restart') {
                    Write-WatchdogLog `
                        -Event 'prior_day_backend_health_unverified' `
                        -Message "component=backend expected_trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) listener_start_date=$($listenerStartTime.ToString('yyyy-MM-dd')) reason=$($missingDateDecision.Reason) action=request_bounded_stop"
                    Write-WatchdogLog `
                        -Event 'backend_session_recovery_requested' `
                        -Message "component=backend trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) reason=$($missingDateDecision.Reason) action=request_exact_owner_stop"
                    Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                    $automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop `
                        -Port 8000 `
                        -ExpectedPid ([int]$backendPids[0]) `
                        -ProjectRoot $ProjectRoot `
                        -RequiredCommandMarkers @('server.py', 'backend.app:app') `
                        -Component 'backend' `
                        -DecisionTime $now `
                        -InvocationId $InvocationId `
                        -Caller $Caller `
                        -RecoveryReason 'prior_day_backend_health_unverified' `
                        -AllowLateSessionSalvage:($missingDateDecision.Reason -eq 'verified_prior_day_listener_late_salvage')
                    if ($automaticStop.Stopped) {
                        $backendWasStopped = $true
                        $backendListenerPresent = $false
                        $backendRecoveryCompletionPending = $true
                        $backendSessionRecoveryReason = [string]$missingDateDecision.Reason
                    }
                }
                else {
                    Write-WatchdogLog `
                        -Event 'backend_trading_date_unverified' `
                        -Message "component=backend expected_trading_date=$expectedTradingDate listener_count=$($backendPids.Count) reason=$($missingDateDecision.Reason) action=preserve_listener"
                    if ($missingDateDecision.Reason -eq 'listener_started_current_day') {
                        $evaluateBackendReadiness = $true
                    }
                }
            }
            else {
                $evaluateBackendReadiness = $true
            }

            if ($backendListenerPresent -and $evaluateBackendReadiness) {
                $backendPids = @(Get-MarketAppListenerProcessIds -Port 8000)
                $listenerOwnershipVerified = $false
                $listenerStartTime = [datetime]::MinValue
                if ($backendPids.Count -eq 1) {
                    $listenerOwnershipVerified = Test-MarketAppVerifiedProcess `
                        -ProcessId ([int]$backendPids[0]) `
                        -ProjectRoot $ProjectRoot `
                        -RequiredCommandMarkers @('server.py', 'backend.app:app')
                    if ($listenerOwnershipVerified) {
                        try {
                            $listenerStartTime = (Get-Process -Id ([int]$backendPids[0]) -ErrorAction Stop).StartTime
                        }
                        catch {
                            $listenerStartTime = [datetime]::MinValue
                        }
                    }
                }
                $readinessDecision = Resolve-MarketAppBackendReadinessAction `
                    -Now $now `
                    -ListenerCount $backendPids.Count `
                    -OwnershipVerified $listenerOwnershipVerified `
                    -ListenerStartTime $listenerStartTime `
                    -RuntimeState $backendReadinessState `
                    -RecoveryAlreadyAttempted $backendRecoveryAlreadySucceeded `
                    -DeadHandoffRecoveryAlreadyAttempted:($backendRecoveryAlreadySucceeded -or $backendRecoveryCompletionPending)
                $readinessStopTime = $now
                if ($readinessDecision.Reason -eq 'verified_current_day_dead_handoff_late_salvage') {
                    $deadHandoffProof = Confirm-DeadHandoffRecoveryPreflight `
                        -ExpectedPid ([int]$backendPids[0]) -ExpectedStartTime $listenerStartTime `
                        -BeforeState $backendReadinessState `
                        -RecoveryAlreadyAttempted:($backendRecoveryAlreadySucceeded -or $backendRecoveryCompletionPending)
                    if ($deadHandoffProof.Approved) {
                        $readinessStopTime = $deadHandoffProof.DecisionTime
                    }
                    else {
                        $readinessDecision = [pscustomobject]@{
                            Action='preserve';Reason='dead_handoff_prevalidation_unverified'
                            FailureReasons=@('dead_handoff_prevalidation_unverified')
                            StartAgeSeconds=$readinessDecision.StartAgeSeconds
                        }
                    }
                }
                $readinessFailures = @($readinessDecision.FailureReasons) -join ','
                if ($readinessDecision.Action -eq 'restart') {
                    Write-WatchdogLog `
                        -Event 'backend_readiness_recovery_requested' `
                        -Message "component=backend trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) start_age_seconds=$([math]::Round([double]$readinessDecision.StartAgeSeconds, 1)) failures=$readinessFailures action=request_bounded_stop"
                    Write-WatchdogLog `
                        -Event 'backend_session_recovery_requested' `
                        -Message "component=backend trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) reason=$($readinessDecision.Reason) action=request_exact_owner_stop"
                    if ($readinessDecision.Reason -ceq 'verified_prior_day_backend_preopen_refresh') {
                        Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                        $refreshPreparationNow = Get-Date
                        $refreshPreparationDeadline = Get-MarketAppUniversePreparationDeadline `
                            -Now $refreshPreparationNow `
                            -NotAfter $refreshPreparationNow.Date.AddHours(8).AddMinutes(25)
                        $automaticStop = Invoke-BackendPreparedAutomaticListenerStop `
                            -ExpectedPid ([int]$backendPids[0]) `
                            -DecisionTime $readinessStopTime `
                            -PreparationDeadline $refreshPreparationDeadline `
                            -ExpectedTradingDate $expectedTradingDate `
                            -ExpectedContractCap ([int]$env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS) `
                            -RecoveryReason 'verified_prior_day_backend_preopen_refresh' `
                            -RequireCurrentDay
                        if ($automaticStop.Stopped) {
                            $preparedBackendUniverse = $automaticStop.Preparation
                        }
                    }
                    else {
                        Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                        $automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop `
                            -Port 8000 `
                            -ExpectedPid ([int]$backendPids[0]) `
                            -ProjectRoot $ProjectRoot `
                            -RequiredCommandMarkers @('server.py', 'backend.app:app') `
                            -Component 'backend' `
                            -DecisionTime $readinessStopTime `
                            -InvocationId $InvocationId `
                            -Caller $Caller `
                            -RecoveryReason $readinessDecision.Reason `
                            -RuntimeState $backendReadinessState `
                            -AllowLateSessionSalvage:($readinessDecision.Reason -in @('verified_prior_day_backend_late_salvage', 'verified_current_day_dead_handoff_late_salvage'))
                    }
                    if ($automaticStop.Stopped) {
                        $backendWasStopped = $true
                        $backendListenerPresent = $false
                        $backendRecoveryCompletionPending = $true
                        $backendSessionRecoveryReason = [string]$readinessDecision.Reason
                    }
                }
                elseif ($readinessDecision.Reason -eq 'current_session_contract_progressing') {
                    Write-WatchdogLog `
                        -Event 'backend_prior_day_process_current_session_verified' `
                        -Message "component=backend trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) reason=current_session_contract_progressing action=preserve_listener"
                }
                elseif ($readinessDecision.Reason -eq 'current_session_post_close_preserved') {
                    Write-WatchdogLog `
                        -Event 'backend_prior_day_process_post_close_preserved' `
                        -Message "component=backend trading_date=$expectedTradingDate listener_pid=$($backendPids[0]) reason=current_session_post_close_preserved action=preserve_retained_final_state"
                }
                elseif (@($readinessDecision.FailureReasons).Count -gt 0) {
                    Write-WatchdogLog `
                        -Event 'backend_readiness_recovery_deferred' `
                        -Message "component=backend trading_date=$expectedTradingDate listener_count=$($backendPids.Count) reason=$($readinessDecision.Reason) failures=$readinessFailures action=preserve_listener"
                }
            }
        }

        if ($backendListenerPresent) {
            if ($backendWasStopped) {
                throw 'A replacement listener appeared on port 8000 after the verified backend was stopped; refusing a concurrent launch.'
            }
            $backendPids = @(Get-MarketAppListenerProcessIds -Port 8000)
            Write-WatchdogLog `
                -Event 'component_already_listening' `
                -Message "component=backend port=8000 listener_pids=$($backendPids -join ',') action=noop"
        }
        else {
            Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
            Invoke-MarketAppVerifiedOrphanBackendCleanup `
                -ProjectRoot $ProjectRoot `
                -Port 8000 `
                -ProviderRemotePort 13000 `
                -RequiredCommandMarkers @('server.py', 'backend.app:app') `
                -InvocationId $InvocationId `
                -Caller $Caller | Out-Null
            $universePreparation = $preparedBackendUniverse
            if ($null -eq $universePreparation) {
                $universePreparationNow = Get-Date
                $universePreparationDeadline = Get-MarketAppUniversePreparationDeadline `
                    -Now $universePreparationNow
                $universeProviderDiscoveryAllowed = Test-MarketAppUniverseProviderDiscoveryAllowed `
                    -Now $universePreparationNow
                $universePreparation = Invoke-MarketAppUniverseCachePreparation `
                    -ProjectRoot $ProjectRoot `
                    -PythonExe $PythonExe `
                    -Symbols $env:DATABENTO_SYMBOLS `
                    -Deadline $universePreparationDeadline `
                    -InvocationId "$InvocationId-backend-start" `
                    -SkipProviderDiscovery:(-not $universeProviderDiscoveryAllowed)
            }
            else {
                $universePreparationDeadline = $null
                Write-WatchdogLog `
                    -Event 'backend_replacement_universe_preparation_reused' `
                    -Message "component=backend outcome=$($universePreparation.Outcome) label=$($universePreparation.ProvenanceLabel) action=launch_without_second_provider_call"
            }
            if (-not $universePreparation.StartupMayContinue) {
                Write-WatchdogLog `
                    -Event 'universe_cache_preparation_blocked_startup' `
                    -Message "component=backend outcome=$($universePreparation.Outcome) deadline_ct=$(if ($universePreparationDeadline) { $universePreparationDeadline.ToString('HH:mm:ss') } else { 'prevalidated' }) action=abstain"
                throw "Databento universe preparation blocked backend startup (outcome=$($universePreparation.Outcome))."
            }
            if ($universePreparation.UsesFallback) {
                Write-WatchdogLog `
                    -Event 'universe_cache_prior_session_fallback' `
                    -Message "label=$($universePreparation.ProvenanceLabel) source_date=$($universePreparation.Result.provenance.source_date) trading_date=$($universePreparation.Result.provenance.trading_date)"
            }
            else {
                Write-WatchdogLog `
                    -Event 'universe_cache_current_day_ready' `
                    -Message "label=$($universePreparation.ProvenanceLabel) source_date=$($universePreparation.Result.provenance.source_date) trading_date=$($universePreparation.Result.provenance.trading_date)"
            }
            $backendLogPaths = New-MarketAppLaunchLogPaths `
                -ProjectRoot $ProjectRoot `
                -Component 'backend' `
                -InvocationId $InvocationId
            Write-WatchdogLog `
                -Event 'launch_logs_prepared' `
                -Message "component=backend retained_stdout=$($backendLogPaths.RetainedStandardOutputPath) retained_stderr=$($backendLogPaths.RetainedStandardErrorPath)"
            Invoke-MarketAppVerifiedOrphanBackendCleanup `
                -ProjectRoot $ProjectRoot `
                -Port 8000 `
                -ProviderRemotePort 13000 `
                -RequiredCommandMarkers @('server.py', 'backend.app:app') `
                -InvocationId $InvocationId `
                -Caller $Caller | Out-Null
            $backendLaunchResult = Start-VerifiedComponent `
                -Component 'backend' `
                -Port 8000 `
                -Url 'http://127.0.0.1:8000/health' `
                -ExpectedEndpointContract 'BackendHealth' `
                -FilePath $PythonExe `
                -ArgumentList @(
                    ('"' + $BackendEntrypoint + '"')
                ) `
                -StandardOutputPath $backendLogPaths.StandardOutputPath `
                -StandardErrorPath $backendLogPaths.StandardErrorPath
            if ($backendRecoveryCompletionPending) {
                Write-WatchdogLog `
                    -Event 'backend_session_recovery_succeeded' `
                    -Message "component=backend trading_date=$currentTradingDate reason=$backendSessionRecoveryReason replacement_listener_pid=$($backendLaunchResult.ListenerProcessId) action=latched"
                $backendRecoveryAlreadySucceeded = $true
                $backendRecoveryCompletionPending = $false
            }
        }
    }

    if ($manageDashboard) {
        $dashboardListenerPresent = Test-PortListener -Port 8501
        if ($dashboardListenerPresent -and $dashboardWasStopped) {
            throw 'A replacement listener appeared on port 8501 after the verified dashboard was stopped; refusing a concurrent launch.'
        }
        if ($dashboardListenerPresent -and -not $dashboardWasStopped) {
            $dashboardPids = @(Get-MarketAppListenerProcessIds -Port 8501)
            $dashboardOwnershipVerified = $false
            $dashboardListenerStartTime = [datetime]::MinValue
            if ($dashboardPids.Count -eq 1) {
                $dashboardOwnershipVerified = Test-MarketAppVerifiedProcess `
                    -ProcessId ([int]$dashboardPids[0]) `
                    -ProjectRoot $ProjectRoot `
                    -RequiredCommandMarkers @('streamlit', 'app.py')
                if ($dashboardOwnershipVerified) {
                    try {
                        $dashboardListenerStartTime = (Get-Process -Id ([int]$dashboardPids[0]) -ErrorAction Stop).StartTime
                    }
                    catch {
                        $dashboardListenerStartTime = [datetime]::MinValue
                    }
                }
            }
            $dashboardEndpointReady = Test-MarketAppHttpEndpointContract `
                -Url 'http://127.0.0.1:8501/_stcore/health' `
                -Port 8501 `
                -ExpectedEndpointContract 'StreamlitHealth' `
                -TimeoutSeconds 3
            $dashboardReadinessDecision = Resolve-MarketAppDashboardReadinessAction `
                -Now $now `
                -ListenerCount $dashboardPids.Count `
                -OwnershipVerified $dashboardOwnershipVerified `
                -ListenerStartTime $dashboardListenerStartTime `
                -EndpointReady $dashboardEndpointReady `
                -RecoveryAlreadyAttempted $dashboardRecoveryAlreadySucceeded
            if ($dashboardReadinessDecision.Action -eq 'restart') {
                Write-WatchdogLog `
                    -Event 'dashboard_readiness_recovery_requested' `
                    -Message "component=dashboard trading_date=$($now.ToString('yyyy-MM-dd')) listener_pid=$($dashboardPids[0]) start_age_seconds=$([math]::Round([double]$dashboardReadinessDecision.StartAgeSeconds, 1)) reason=$($dashboardReadinessDecision.Reason) failures=$(@($dashboardReadinessDecision.FailureReasons) -join ',') action=request_bounded_stop"
                Write-WatchdogLog `
                    -Event 'dashboard_session_recovery_requested' `
                    -Message "component=dashboard trading_date=$currentTradingDate listener_pid=$($dashboardPids[0]) reason=$($dashboardReadinessDecision.Reason) action=request_exact_owner_stop"
                Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                $automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop `
                    -Port 8501 `
                    -ExpectedPid ([int]$dashboardPids[0]) `
                    -ProjectRoot $ProjectRoot `
                    -RequiredCommandMarkers @('streamlit', 'app.py') `
                    -Component 'dashboard' `
                    -DecisionTime $now `
                    -InvocationId $InvocationId `
                    -Caller $Caller `
                    -RecoveryReason 'dashboard_readiness_recovery_requested' `
                    -AllowLateSessionSalvage:($dashboardReadinessDecision.Reason -eq 'verified_prior_day_dashboard_late_salvage')
                if ($automaticStop.Stopped) {
                    $dashboardWasStopped = $true
                    $dashboardListenerPresent = $false
                    $dashboardRecoveryCompletionPending = $true
                    $dashboardSessionRecoveryReason = [string]$dashboardReadinessDecision.Reason
                }
            }
            elseif ($dashboardReadinessDecision.Reason -ne 'healthy') {
                Write-WatchdogLog `
                    -Event 'dashboard_readiness_recovery_deferred' `
                    -Message "component=dashboard trading_date=$($now.ToString('yyyy-MM-dd')) listener_count=$($dashboardPids.Count) reason=$($dashboardReadinessDecision.Reason) failures=$(@($dashboardReadinessDecision.FailureReasons) -join ',') action=preserve_listener"
            }
        }

        if ($dashboardListenerPresent) {
            $dashboardPids = @(Get-MarketAppListenerProcessIds -Port 8501)
            $dashboardContractVerified = [bool](
                $dashboardReadinessDecision -and
                $dashboardReadinessDecision.Reason -eq 'healthy'
            )
            Write-WatchdogLog `
                -Event 'component_already_listening' `
                -Message "component=dashboard port=8501 listener_pids=$($dashboardPids -join ',') ownership_verified=$dashboardOwnershipVerified endpoint_contract=StreamlitHealth endpoint_contract_verified=$dashboardContractVerified action=noop"
        }
        else {
            $dashboardLogPaths = New-MarketAppLaunchLogPaths `
                -ProjectRoot $ProjectRoot `
                -Component 'dashboard' `
                -InvocationId $InvocationId
            Write-WatchdogLog `
                -Event 'launch_logs_prepared' `
                -Message "component=dashboard retained_stdout=$($dashboardLogPaths.RetainedStandardOutputPath) retained_stderr=$($dashboardLogPaths.RetainedStandardErrorPath)"
            $dashboardLaunchResult = Start-VerifiedComponent `
                -Component 'dashboard' `
                -Port 8501 `
                -Url 'http://127.0.0.1:8501/_stcore/health' `
                -ExpectedEndpointContract 'StreamlitHealth' `
                -FilePath $StreamlitExe `
                -ArgumentList @(
                    'run',
                    ('"' + $DashboardEntrypoint + '"'),
                    '--server.address',
                    '127.0.0.1',
                    '--server.port',
                    '8501',
                    '--server.headless',
                    'true',
                    # Production source changes become active only through a
                    # verified restart; do not let the watcher evict modules
                    # while a dashboard rerun is importing dataclasses.
                    '--server.fileWatcherType',
                    'none'
                ) `
                -StandardOutputPath $dashboardLogPaths.StandardOutputPath `
                -StandardErrorPath $dashboardLogPaths.StandardErrorPath
            if ($dashboardRecoveryCompletionPending) {
                Write-WatchdogLog `
                    -Event 'dashboard_session_recovery_succeeded' `
                    -Message "component=dashboard trading_date=$currentTradingDate reason=$dashboardSessionRecoveryReason replacement_listener_pid=$($dashboardLaunchResult.ListenerProcessId) action=latched"
                $dashboardRecoveryAlreadySucceeded = $true
                $dashboardRecoveryCompletionPending = $false
            }
        }
    }

    if ($manageRecorder) {
        $tradingDate = $now.ToString('yyyy-MM-dd')
        $recorderPid = Get-MarketAppVerifiedRecorderProcessId `
            -ProjectRoot $ProjectRoot `
            -TradingDate $tradingDate
        if ($recorderPid) {
            $recorderStatus = Get-MarketAppRecorderStatus `
                -ProjectRoot $ProjectRoot `
                -TradingDate $tradingDate
            if ($recorderStatus.Healthy) {
                Write-WatchdogLog `
                    -Event 'component_already_running' `
                    -Message "component=closing_tape recorder_pid=$recorderPid state=$($recorderStatus.State) status_age_seconds=$($recorderStatus.AgeSeconds) action=noop"
            }
            else {
                # A verified writer may be hashing, blocked in the provider, or
                # preserving a failure. Never auto-kill it and risk truncating
                # the canonical tape; surface the condition for the operator.
                Write-WatchdogLog `
                    -Event 'component_degraded' `
                    -Message "component=closing_tape recorder_pid=$recorderPid state=$($recorderStatus.State) reason=$($recorderStatus.Reason -replace ' ', '_') action=preserve_process"
            }
        }
        else {
            $gateResult = & $ClosingTapeScript -TradingDate $tradingDate -NoParquet -CheckOnly
            $gateStatus = [string]$gateResult.Status
            if ($gateStatus -notin @('ready_to_start', 'already_running', 'not_yet_due', 'primary_capture_not_ready', 'outside_start_window', 'recovery_attempt_limit_reached')) {
                throw "Closing-tape check-only gate returned unexpected status: $gateStatus"
            }
            Write-WatchdogLog `
                -Event 'closing_tape_gate_result' `
                -Message "component=closing_tape status=$gateStatus recorder_pid=$($gateResult.Pid) action=$(if ($gateStatus -eq 'ready_to_start') { 'request_preflight' } else { 'preserve_without_launch' })"

            $result = $gateResult
            if ($gateStatus -eq 'ready_to_start') {
                Write-WatchdogLog `
                    -Event 'launch_requested' `
                    -Message "component=closing_tape trading_date=$tradingDate action=start"
                Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                $result = & $ClosingTapeScript -TradingDate $tradingDate -NoParquet
            }
            $resultStatus = [string]$result.Status
            if ($resultStatus -notin @('started', 'already_running', 'not_yet_due', 'primary_capture_not_ready', 'outside_start_window', 'recovery_attempt_limit_reached')) {
                throw "Closing-tape launcher returned unexpected status: $resultStatus"
            }
            Write-WatchdogLog `
                -Event 'closing_tape_launch_result' `
                -Message "component=closing_tape status=$resultStatus recorder_pid=$($result.Pid)"
            if ($resultStatus -eq 'not_yet_due') {
                Write-WatchdogLog `
                    -Event 'closing_tape_deferred_for_opening_capture' `
                    -Message "component=closing_tape start_not_before_utc=$($result.StartNotBeforeUtc) action=defer reason=protect_opening_gamma_and_orb"
            }
            elseif ($resultStatus -eq 'primary_capture_not_ready') {
                Write-WatchdogLog `
                    -Event 'closing_tape_deferred_for_primary_capture' `
                    -Message "component=closing_tape action=defer reason=primary_transport_or_subscription_unsafe"
            }
            elseif ($resultStatus -eq 'recovery_attempt_limit_reached') {
                # Preserve both bounded attempts and require operator review.
                Write-WatchdogLog `
                    -Event 'closing_tape_recovery_attempt_limit_reached' `
                    -Message "component=closing_tape prior_nonempty_dbn_count=$($result.PriorNonemptyDbnCount) action=preserve_primary_backend"
            }
            elseif ($resultStatus -eq 'outside_start_window') {
                Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null
                $finalizeOutput = @(& $PythonExe $ClosingTapeFinalizeScript `
                    --project-root $ProjectRoot `
                    --trading-date $tradingDate)
                $finalizeExitCode = $LASTEXITCODE
                $finalizeResolution = Resolve-MarketAppPostCloseFinalizeResult `
                    -Output $finalizeOutput `
                    -ExitCode $finalizeExitCode
                $finalizeResult = $finalizeResolution.Result
                if ($finalizeResolution.ExpectedIncomplete) {
                    $issueCount = @($finalizeResult.issues).Count
                    Write-WatchdogLog `
                        -Event 'closing_tape_finalize_degraded' `
                        -Message "component=closing_tape action=incomplete session_id=$($finalizeResult.session_id) issue_count=$issueCount"
                }
                else {
                    Write-WatchdogLog `
                        -Event 'closing_tape_finalize_result' `
                        -Message "component=closing_tape action=$($finalizeResult.action) session_id=$($finalizeResult.session_id)"
                }
            }
        }
    }
    }
}
finally {
    Exit-MarketAppSupervisorLock -LockHandle $supervisorLock
    Write-WatchdogLogBestEffort -Event 'supervisor_lock_released' -Message "mutex=$($supervisorLock.Name)"
}

    $invocationOutcome = 'success'
    $invocationTerminalPath = if ($PrepareUniverseOnly) { 'prepare_universe_only' } else { 'supervisor_run' }
    $normalizedExitCode = 0
}
catch {
    $invocationErrorType = $_.Exception.GetType().Name
    Write-WatchdogLogBestEffort `
        -Event 'invocation_failed' `
        -Message "error_type=$invocationErrorType normalized_exit_code=1"
    throw
}
finally {
    Write-WatchdogLogBestEffort `
        -Event 'invocation_completed' `
        -Message "outcome=$invocationOutcome terminal_path=$invocationTerminalPath error_type=$invocationErrorType normalized_exit_code=$normalizedExitCode"
}
