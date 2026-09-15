$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnsurePath = Join-Path $ProjectRoot 'ensure_market_app.ps1'
$StartMarketDayPath = Join-Path $ProjectRoot 'start_market_day.ps1'
$StartClosingTapePath = Join-Path $ProjectRoot 'start_closing_tape.ps1'
$ClosingTapeGatePath = Join-Path $ProjectRoot 'backend\closing_tape\start_gate.py'
$ClosingTapeConfigPath = Join-Path $ProjectRoot 'backend\closing_tape\config.py'
$SupervisorPath = Join-Path $ProjectRoot 'market_app_supervisor.psm1'
$EnsureSource = Get-Content -LiteralPath $EnsurePath -Raw
$StartMarketDaySource = Get-Content -LiteralPath $StartMarketDayPath -Raw
$StartClosingTapeSource = Get-Content -LiteralPath $StartClosingTapePath -Raw
$ClosingTapeGateSource = Get-Content -LiteralPath $ClosingTapeGatePath -Raw
$ClosingTapeConfigSource = Get-Content -LiteralPath $ClosingTapeConfigPath -Raw
$SupervisorSource = Get-Content -LiteralPath $SupervisorPath -Raw

Describe 'Opening acceptance launch preflight' {
    It 'keeps all launch scripts syntactically valid' {
        foreach ($path in @($EnsurePath, $StartMarketDayPath, $StartClosingTapePath)) {
            $tokens = $null
            $errors = $null
            [void][System.Management.Automation.Language.Parser]::ParseFile(
                $path,
                [ref]$tokens,
                [ref]$errors
            )
            if (@($errors).Count -ne 0) {
                throw "PowerShell parse failed for $path`: $($errors -join '; ')"
            }
        }
    }

    It 'keeps the full preflight lazy on a healthy no-op watchdog invocation' {
        foreach ($pattern in @(
            'tools\\preflight_opening_acceptance\.py',
            'opening_acceptance_preflight_failed',
            'action=abort_before_process_change',
            'opening_acceptance_preflight_passed',
            'function Assert-OpeningAcceptanceMutationPreflight'
        )) {
            if ($EnsureSource -notmatch $pattern) {
                throw "ensure_market_app.ps1 is missing launch guard marker: $pattern"
            }
        }

        $mainStart = $EnsureSource.IndexOf(
            '$backendWasStopped = $false',
            $EnsureSource.IndexOf('$supervisorLock = Enter-MarketAppSupervisorLock')
        )
        $firstMutationDecision = $EnsureSource.IndexOf(
            'if ($RestartBackend) {',
            $mainStart
        )
        if ($mainStart -lt 0 -or $firstMutationDecision -le $mainStart) {
            throw 'Could not isolate the normal watchdog decision prelude.'
        }
        $normalDecisionPrelude = $EnsureSource.Substring(
            $mainStart,
            $firstMutationDecision - $mainStart
        )
        $normalDecisionPrelude | Should Not Match 'Assert-OpeningAcceptanceMutationPreflight'
        $EnsureSource | Should Not Match '\$openingAcceptancePreflight\s*=\s*Invoke-OpeningAcceptancePreflight'
        $EnsureSource | Should Match '\$script:OpeningAcceptancePreflightResult = \$null'
        $EnsureSource | Should Not Match 'OpeningAcceptancePreflight(Expires|Ttl|Timestamp|CachePath)'
    }

    It 'guards every explicit stop, automatic stop, component start, and failed-launch cleanup' {
        $stopBody = [regex]::Match(
            $EnsureSource,
            '(?s)function Stop-VerifiedComponentListener \{.*?(?=\r?\nfunction Start-VerifiedComponent)'
        ).Value
        $startBody = [regex]::Match(
            $EnsureSource,
            '(?s)function Start-VerifiedComponent \{.*?(?=\r?\nfunction Invoke-OpeningAcceptancePreflight)'
        ).Value
        $stopBody | Should Match 'Assert-OpeningAcceptanceMutationPreflight(?: -AllowSchemaBootstrap)? \| Out-Null[\s\S]+Stop-Process -Id \$listenerPid'
        $stopBody | Should Match 'Stop-Process -Id \$listenerPid[\s\S]+Wait-MarketAppProcessNetworkQuiescence[\s\S]+process_exited=true tcp_connection_count=0'
        ([regex]::Matches($stopBody, 'Assert-MarketAppExpectedListenerPid')).Count | Should Be 2
        ([regex]::Matches($stopBody, 'Test-MarketAppVerifiedProcess')).Count | Should Be 2
        $startBody | Should Match 'Assert-OpeningAcceptanceMutationPreflight(?: -AllowSchemaBootstrap)? \| Out-Null\s+Write-WatchdogLog[\s\S]+\$launch = Start-Process'
        $startBody | Should Match 'launch_ownership_failed[\s\S]+Assert-OpeningAcceptanceMutationPreflight(?: -AllowSchemaBootstrap)? \| Out-Null\s+Stop-MarketAppAttemptedLaunch'

        $automaticStops = [regex]::Matches(
            $EnsureSource,
            '(?m)^\s*\$automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop\s+`'
        )
        $guardedAutomaticStops = [regex]::Matches(
            $EnsureSource,
            '(?m)^\s*Assert-OpeningAcceptanceMutationPreflight(?: -AllowSchemaBootstrap)? \| Out-Null\r?\n\s*\$automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop\s+`'
        )
        $automaticStops.Count | Should Be 5
        $guardedAutomaticStops.Count | Should Be $automaticStops.Count
    }

    It 'clears verified orphan backends before provider preparation and rechecks before launch' {
        $preparationAssignment = $EnsureSource.IndexOf(
            '$universePreparation = $preparedBackendUniverse'
        )
        $firstOrphanGuard = $EnsureSource.LastIndexOf(
            'Invoke-MarketAppVerifiedOrphanBackendCleanup',
            $preparationAssignment
        )
        $mutationPreflight = $EnsureSource.LastIndexOf(
            'Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null',
            $firstOrphanGuard
        )
        $providerPreparation = $EnsureSource.IndexOf(
            '$universePreparation = Invoke-MarketAppUniverseCachePreparation',
            $preparationAssignment
        )
        $secondOrphanGuard = $EnsureSource.IndexOf(
            'Invoke-MarketAppVerifiedOrphanBackendCleanup',
            $providerPreparation
        )
        $backendLaunch = $EnsureSource.IndexOf(
            '$backendLaunchResult = Start-VerifiedComponent',
            $secondOrphanGuard
        )

        ($mutationPreflight -ge 0) | Should Be $true
        ($firstOrphanGuard -gt $mutationPreflight) | Should Be $true
        ($preparationAssignment -gt $firstOrphanGuard) | Should Be $true
        ($providerPreparation -gt $preparationAssignment) | Should Be $true
        ($secondOrphanGuard -gt $providerPreparation) | Should Be $true
        ($backendLaunch -gt $secondOrphanGuard) | Should Be $true
        ([regex]::Matches(
            $EnsureSource,
            'Invoke-MarketAppVerifiedOrphanBackendCleanup'
        )).Count | Should Be 2

        $cleanupBody = [regex]::Match(
            $SupervisorSource,
            '(?s)function Invoke-MarketAppVerifiedOrphanBackendCleanup \{.*?(?=\r?\nfunction Get-MarketAppProcessTcpConnectionCountFromNetstatLines)'
        ).Value
        $cleanupBody | Should Match 'Test-MarketAppVerifiedProcess'
        $cleanupBody | Should Match 'Wait-MarketAppProcessNetworkQuiescence'
        $cleanupBody | Should Match 'Get-MarketAppRemoteTcpOwnerProcessIds'
        $cleanupBody | Should Not Match 'Invoke-MarketAppUniverseCachePreparation|prepare_databento_universe_cache|Invoke-WebRequest|Invoke-RestMethod'
    }

    It 'uses a read-only closing-tape gate and preflights only the ready launch path' {
        $gateCall = $EnsureSource.IndexOf(
            '$gateResult = & $ClosingTapeScript -TradingDate $tradingDate -NoParquet -CheckOnly'
        )
        $readyBranch = $EnsureSource.IndexOf(
            "if (`$gateStatus -eq 'ready_to_start')",
            $gateCall
        )
        $preflight = $EnsureSource.IndexOf(
            'Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null',
            $readyBranch
        )
        $launchCall = $EnsureSource.IndexOf(
            '$result = & $ClosingTapeScript -TradingDate $tradingDate -NoParquet',
            $preflight
        )
        ($gateCall -ge 0) | Should Be $true
        ($readyBranch -gt $gateCall) | Should Be $true
        ($preflight -gt $readyBranch) | Should Be $true
        ($launchCall -gt $preflight) | Should Be $true

        $checkOnlyBranch = $StartClosingTapeSource.IndexOf('if ($CheckOnly) {')
        $checkOnlyReady = $StartClosingTapeSource.IndexOf("Status = 'ready_to_start'", $checkOnlyBranch)
        $checkOnlyReturn = $StartClosingTapeSource.IndexOf('return', $checkOnlyReady)
        $directoryCreate = $StartClosingTapeSource.IndexOf('New-Item -ItemType Directory')
        $recorderStart = $StartClosingTapeSource.IndexOf('$process = Start-Process')
        ($checkOnlyBranch -ge 0) | Should Be $true
        ($checkOnlyReady -gt $checkOnlyBranch) | Should Be $true
        ($checkOnlyReturn -gt $checkOnlyReady) | Should Be $true
        ($directoryCreate -gt $checkOnlyReturn) | Should Be $true
        ($recorderStart -gt $directoryCreate) | Should Be $true
        $ClosingTapeGateSource | Should Not Match '\.mkdir\(|os\.makedirs'
        $ClosingTapeConfigSource | Should Not Match '\.mkdir\(|os\.makedirs'
        $StartClosingTapeSource | Should Match '--require-primary-ready'
        $ClosingTapeGateSource | Should Match 'PRIMARY_CAPTURE_FAMILIES = \("SPX", "NDX", "VIX", "RUT"\)'
        $ClosingTapeGateSource | Should Match 'PRIMARY_ORB_\{window_name\.upper\(\)\}_INCOMPLETE:\{symbol\}'
    }

    It 'propagates the guarded child launcher exit code to Task Scheduler' {
        if ($StartMarketDaySource -notmatch 'exit \$LASTEXITCODE\s*$') {
            throw 'start_market_day.ps1 does not propagate the guarded launcher exit code.'
        }
    }
}
