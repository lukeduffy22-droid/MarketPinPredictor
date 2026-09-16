$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ModulePath = Join-Path $ProjectRoot 'market_app_supervisor.psm1'
Import-Module -Name $ModulePath -Force

# Load only the pure task-contract and repair functions from the launcher. Dot-
# sourcing start_market_day.ps1 would execute its launch path, which these
# source tests must never do.
$StartMarketDayPath = Join-Path $ProjectRoot 'start_market_day.ps1'
$startMarketDayTokens = $null
$startMarketDayParseErrors = $null
$startMarketDayAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $StartMarketDayPath,
    [ref]$startMarketDayTokens,
    [ref]$startMarketDayParseErrors
)
if (@($startMarketDayParseErrors).Count -ne 0) {
    throw "start_market_day.ps1 did not parse cleanly: $($startMarketDayParseErrors -join '; ')"
}
foreach ($functionName in @(
    'Get-MarketDayBootstrapMutexName',
    'Enter-MarketDayBootstrapLock',
    'Exit-MarketDayBootstrapLock',
    'Write-MarketClockLog',
    'Test-MarketLauncherIsElevated',
    'ConvertTo-MarketTaskArgumentTokens',
    'Resolve-MarketWindowsIdentitySid',
    'Test-MarketStartupTaskContract',
    'Repair-MarketStartupBootRecovery',
    'Test-MarketWatchdogTaskContract',
    'Repair-MarketWatchdogRecoveryAuthority',
    'Get-MarketDayInvocationAction'
)) {
    $definition = @($startMarketDayAst.FindAll(
        {
            param($candidate)
            $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $candidate.Name -eq $functionName
        },
        $true
    ))
    if ($definition.Count -ne 1) {
        throw "Expected exactly one $functionName definition in start_market_day.ps1."
    }
    Invoke-Expression $definition[0].Extent.Text
}

# Load only the tested orchestration helpers from ensure_market_app.ps1. Dot-
# sourcing the launcher would execute its live supervision path.
$EnsureMarketAppPath = Join-Path $ProjectRoot 'ensure_market_app.ps1'
$ensureMarketAppTokens = $null
$ensureMarketAppParseErrors = $null
$ensureMarketAppAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $EnsureMarketAppPath,
    [ref]$ensureMarketAppTokens,
    [ref]$ensureMarketAppParseErrors
)
if (@($ensureMarketAppParseErrors).Count -ne 0) {
    throw "ensure_market_app.ps1 did not parse cleanly: $($ensureMarketAppParseErrors -join '; ')"
}
foreach ($functionName in @(
    'Test-MarketAppCurrentDayUniversePreparationProof',
    'Invoke-BackendPreparedAutomaticListenerStop',
    'Assert-OpeningAcceptanceMutationPreflight',
    'Invoke-MarketAppCurrentDayUniversePrestage'
)) {
    $definition = @($ensureMarketAppAst.FindAll(
        {
            param($candidate)
            $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $candidate.Name -eq $functionName
        },
        $true
    ))
    if ($definition.Count -ne 1) {
        throw "Expected exactly one $functionName definition in ensure_market_app.ps1."
    }
    Invoke-Expression $definition[0].Extent.Text
}

function Write-WatchdogLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Event = 'status'
    )
}

function New-TestBackendUniversePreparation {
    param(
        [bool]$StartupMayContinue = $true,
        [bool]$UsesFallback = $false,
        [bool]$TimedOut = $false,
        [string]$TradingDate = '2026-09-09',
        [string[]]$Symbols = @('SPX', 'NDX', 'VIX', 'RUT')
    )

    $marketPrimaryReadiness = [ordered]@{}
    foreach ($symbol in $Symbols) {
        $isVix = $symbol -ceq 'VIX'
        $pairCount = if ($symbol -in @('SPX', 'NDX')) { 100 } else { 10 }
        $primaryExpiration = if ($isVix) {
            ([datetime]::ParseExact(
                $TradingDate,
                'yyyy-MM-dd',
                [System.Globalization.CultureInfo]::InvariantCulture
            )).AddDays(7).ToString('yyyy-MM-dd')
        }
        else {
            $TradingDate
        }
        $marketPrimaryReadiness[$symbol] = [pscustomobject]@{
            subscription_available = $true
            admission_passes = $true
            primary_plan_count = 1
            primary_expiration = $primaryExpiration
            primary_contract_count = 2 * $pairCount
            selected_strike_pairs = $pairCount
            minimum_pair_count = if ($symbol -in @('SPX', 'NDX')) { 100 } else { 1 }
            complete_pair_count = $pairCount
            orb_reference_minimum_pair_count = 5
            primary_expiration_authority = if ($isVix) {
                'vix_forward_expiration_context_only'
            }
            else {
                'primary_expiration'
            }
            primary_expiration_context_only = [bool]$isVix
            primary_expiration_same_day_authority = [bool](-not $isVix)
            primary_expiration_selection_basis = if ($isVix) {
                'vix_last_trading_day_precedes_settlement_date'
            }
            else {
                'earliest_live_eligible_expiration'
            }
        }
    }

    return [pscustomobject]@{
        Outcome = if ($StartupMayContinue) {
            'provider_completed_cache_only_validated'
        }
        else {
            'provider_child_exit_or_quiescence_unverified'
        }
        TimedOut = $TimedOut
        StartupMayContinue = $StartupMayContinue
        UsesFallback = $UsesFallback
        ProvenanceLabel = if ($UsesFallback) {
            'PRIOR_SESSION_FALLBACK'
        }
        else {
            'CURRENT_DAY_CACHE'
        }
        Result = [pscustomobject]@{
            current_day_cache_ready = -not $UsesFallback
            selected_contract_count = 1800
            selected_universe_sha256 = ('a' * 64)
            requested_symbols = @($Symbols)
            market_primary_readiness = [pscustomobject]$marketPrimaryReadiness
            provenance = [pscustomobject]@{
                trading_date = $TradingDate
                source_date = if ($UsesFallback) { '2026-09-08' } else { $TradingDate }
                mode = if ($UsesFallback) { 'prior_cache_filtered' } else { 'current_day_cache' }
                is_fallback = $UsesFallback
            }
        }
    }
}

function New-TestSubscriptionBoundaryState {
    param(
        [string]$TradingDate = '2026-09-08',
        [string]$HealthCashCloseUtc = '2026-09-08T20:00:00+00:00',
        [string]$LiveCashCloseUtc = ''
    )

    if ([string]::IsNullOrWhiteSpace($LiveCashCloseUtc)) {
        $LiveCashCloseUtc = $HealthCashCloseUtc
    }
    return [pscustomobject]@{
        HealthReachable = $true
        LiveHealthReachable = $true
        HealthEvidence = [pscustomobject]@{
            subscription_window = [pscustomobject]@{
                trading_date = $TradingDate
                cash_close_utc = $HealthCashCloseUtc
            }
        }
        LiveEvidence = [pscustomobject]@{
            subscription_window = [pscustomobject]@{
                trading_date = $TradingDate
                cash_close_utc = $LiveCashCloseUtc
            }
        }
    }
}

$WatchdogTaskName = 'MarketPinPredictor_Watchdog'
$WatchdogTaskPath = '\'
$StartupTaskName = 'MarketPinPredictor_AutoStart'
$StartupTaskPath = '\'

function New-TestMarketWatchdogTask {
    param(
        [string]$TaskName = 'MarketPinPredictor_Watchdog',
        [string]$TaskPath = '\',
        [string]$UserId = 'S-1-5-18',
        [string]$LogonType = 'ServiceAccount',
        [string]$RunLevel = 'Highest',
        [string]$Execute = (Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'),
        [string]$WorkingDirectory = $ProjectRoot,
        [string]$Arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$(Join-Path $ProjectRoot 'start_market_day.ps1')`" -EnableRutCanary -SkipClockSync",
        [int]$ActionCount = 1
    )

    $actions = @()
    for ($index = 0; $index -lt $ActionCount; $index++) {
        $actions += [pscustomobject]@{
            Execute = $Execute
            WorkingDirectory = $WorkingDirectory
            Arguments = $Arguments
        }
    }
    $trigger = New-TestMarketTrigger `
        -ClassName 'MSFT_TaskWeeklyTrigger' `
        -StartBoundary '2026-09-05T07:50:00-05:00'
    $trigger.Repetition.Interval = 'PT5M'
    $trigger.Repetition.Duration = 'PT10H10M'
    $trigger.Repetition.StopAtDurationEnd = $true
    return [pscustomobject]@{
        TaskName = $TaskName
        TaskPath = $TaskPath
        Principal = [pscustomobject]@{
            UserId = $UserId
            LogonType = $LogonType
            RunLevel = $RunLevel
            ProcessTokenSidType = 'Default'
            Id = 'Author'
        }
        Actions = $actions
        Settings = [pscustomobject]@{
            Enabled = $true
            MultipleInstances = 'IgnoreNew'
            ExecutionTimeLimit = 'PT15M'
            DisallowStartIfOnBatteries = $false
            StopIfGoingOnBatteries = $false
            WakeToRun = $true
            StartWhenAvailable = $false
            RestartCount = 0
            RestartInterval = $null
        }
        Triggers = @($trigger)
    }
}

function New-TestMarketTrigger {
    param(
        [Parameter(Mandatory = $true)][string]$ClassName,
        [string]$StartBoundary,
        [string]$UserId,
        [bool]$Enabled = $true,
        [string]$Delay
    )
    return [pscustomobject]@{
        CimClass = [pscustomobject]@{CimClassName=$ClassName}
        Enabled = $Enabled
        StartBoundary = $StartBoundary
        EndBoundary = $null
        Delay = $Delay
        RandomDelay = $null
        UserId = $UserId
        DaysOfWeek = if ($ClassName -eq 'MSFT_TaskWeeklyTrigger') { 62 } else { 0 }
        WeeksInterval = if ($ClassName -eq 'MSFT_TaskWeeklyTrigger') { 1 } else { 0 }
        Repetition = [pscustomobject]@{Interval=$null;Duration=$null;StopAtDurationEnd=$false}
    }
}

function New-TestMarketStartupTask {
    param(
        [int]$BootCount = 1,
        [bool]$BootEnabled = $true,
        [string]$BootDelay,
        [string[]]$WeeklyTimes = @('07:00','07:15','07:45','08:15'),
        [string]$LogonUserId = 'S-1-5-21-1000-1000-1000-1001',
        [string]$Arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$(Join-Path $ProjectRoot 'start_market_day.ps1')`" -EnableRutCanary",
        [bool]$WakeToRun = $true
    )
    $triggers = @()
    for ($index = 0; $index -lt $BootCount; $index++) {
        $triggers += New-TestMarketTrigger -ClassName 'MSFT_TaskBootTrigger' -Enabled $BootEnabled -Delay $BootDelay
    }
    foreach ($time in $WeeklyTimes) {
        $triggers += New-TestMarketTrigger -ClassName 'MSFT_TaskWeeklyTrigger' -StartBoundary "2026-09-05T$($time):00-05:00"
    }
    $triggers += New-TestMarketTrigger -ClassName 'MSFT_TaskLogonTrigger' -UserId $LogonUserId
    return [pscustomobject]@{
        TaskName = 'MarketPinPredictor_AutoStart'
        TaskPath = '\'
        Principal = [pscustomobject]@{UserId='S-1-5-18';LogonType='ServiceAccount';RunLevel='Highest'}
        Actions = @([pscustomobject]@{
            Execute=(Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe')
            WorkingDirectory=$ProjectRoot
            Arguments=$Arguments
        })
        Settings = [pscustomobject]@{
            Enabled=$true
            MultipleInstances='IgnoreNew'
            ExecutionTimeLimit='PT15M'
            DisallowStartIfOnBatteries=$false
            StopIfGoingOnBatteries=$false
            WakeToRun=$WakeToRun
            StartWhenAvailable=$true
            RestartCount=3
            RestartInterval='PT1M'
        }
        Triggers = $triggers
    }
}

Describe 'Market app supervisor mutex' {
    It 'uses one deterministic mutex for equivalent project paths' {
        $root = Join-Path $TestDrive 'same-root'
        New-Item -ItemType Directory -Path $root | Out-Null

        $first = Get-MarketAppSupervisorMutexName -ProjectRoot $root
        $second = Get-MarketAppSupervisorMutexName -ProjectRoot ($root + [IO.Path]::DirectorySeparatorChar)

        $first | Should Be $second
        $first | Should Match '^Global\\MarketPinPredictor\.Supervisor\.[0-9a-f]{24}$'
    }

    It 'allows only one of two PowerShell processes to hold the project mutex' {
        $root = Join-Path $TestDrive 'contended-root'
        $signal = Join-Path $TestDrive 'holder-acquired.signal'
        New-Item -ItemType Directory -Path $root | Out-Null
        $escapedModule = $ModulePath.Replace("'", "''")
        $escapedRoot = $root.Replace("'", "''")
        $escapedSignal = $signal.Replace("'", "''")
        $holderScript = @"
Import-Module -Name '$escapedModule' -Force
`$lockHandle = Enter-MarketAppSupervisorLock -ProjectRoot '$escapedRoot' -TimeoutMilliseconds 0
if (-not `$lockHandle.Acquired) { exit 2 }
try {
    [IO.File]::WriteAllText('$escapedSignal', 'acquired')
    Start-Sleep -Milliseconds 1500
}
finally {
    Exit-MarketAppSupervisorLock -LockHandle `$lockHandle
}
"@
        $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($holderScript))
        $pwsh = (Get-Command pwsh.exe -ErrorAction Stop).Source
        $holder = Start-Process `
            -FilePath $pwsh `
            -ArgumentList @('-NoProfile', '-NonInteractive', '-EncodedCommand', $encoded) `
            -WindowStyle Hidden `
            -PassThru

        try {
            $deadline = [DateTime]::UtcNow.AddSeconds(5)
            while (-not (Test-Path -LiteralPath $signal) -and [DateTime]::UtcNow -lt $deadline) {
                Start-Sleep -Milliseconds 50
            }
            Test-Path -LiteralPath $signal | Should Be $true

            $contender = Enter-MarketAppSupervisorLock -ProjectRoot $root -TimeoutMilliseconds 0
            try {
                $contender.Acquired | Should Be $false
            }
            finally {
                Exit-MarketAppSupervisorLock -LockHandle $contender
            }

            $holder | Wait-Process -Timeout 5
            $holder.Refresh()
            $holder.ExitCode | Should Be 0

            $afterRelease = Enter-MarketAppSupervisorLock -ProjectRoot $root -TimeoutMilliseconds 250
            try {
                $afterRelease.Acquired | Should Be $true
            }
            finally {
                Exit-MarketAppSupervisorLock -LockHandle $afterRelease
            }
        }
        finally {
            if (-not $holder.HasExited) {
                Stop-Process -Id $holder.Id -Force -ErrorAction SilentlyContinue
            }
        }
    }
}

Describe 'Market-day wrapper bootstrap coordination' {
    It 'releases the separate wrapper mutex before either guarded launcher invocation' {
        $source = Get-Content -LiteralPath $StartMarketDayPath -Raw
        $acquireIndex = $source.IndexOf('$bootstrapLock = Enter-MarketDayBootstrapLock')
        $releaseIndex = $source.IndexOf('Exit-MarketDayBootstrapLock -LockHandle $bootstrapLock', $acquireIndex)
        $prestageInvokeIndex = $source.IndexOf('& powershell.exe @prestageArguments', $releaseIndex)
        $normalInvokeIndex = $source.IndexOf('& powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $EnsureScript', $releaseIndex)

        ($acquireIndex -ge 0) | Should Be $true
        ($releaseIndex -gt $acquireIndex) | Should Be $true
        ($prestageInvokeIndex -gt $releaseIndex) | Should Be $true
        ($normalInvokeIndex -gt $releaseIndex) | Should Be $true
        $source | Should Match '\$BootstrapMutexTimeoutMilliseconds = 45000'
        $source | Should Match 'finally \{[\s\S]+Exit-MarketDayBootstrapLock -LockHandle \$bootstrapLock[\s\S]+\}[\s\S]+if \(\$null -ne \$prestageArguments\)'
    }

    It 'keeps a locked clock log nonfatal and labels the next successful append with the writer PID' {
        $oldRuntimeLogDir = Get-Variable -Name RuntimeLogDir -Scope Script -ErrorAction SilentlyContinue
        $oldClockLogPath = Get-Variable -Name ClockLogPath -Scope Script -ErrorAction SilentlyContinue
        $script:RuntimeLogDir = Join-Path $TestDrive 'clock-log-retry'
        $script:ClockLogPath = Join-Path $script:RuntimeLogDir 'clock_sync.log'
        New-Item -ItemType Directory -Path $script:RuntimeLogDir -Force | Out-Null
        $exclusive = [IO.File]::Open(
            $script:ClockLogPath,
            [IO.FileMode]::OpenOrCreate,
            [IO.FileAccess]::ReadWrite,
            [IO.FileShare]::None
        )
        try {
            {
                Write-MarketClockLog `
                    -Result ([pscustomobject]@{Status='contended';Service='test';Error=$null}) `
                    -MaxAttempts 2 `
                    -RetryDelayMilliseconds 10
            } | Should Not Throw
        }
        finally {
            $exclusive.Dispose()
        }

        Write-MarketClockLog -Result ([pscustomobject]@{Status='written';Service='test';Error=$null})
        $line = Get-Content -LiteralPath $script:ClockLogPath -Raw
        $line | Should Match "powershell_pid=$PID status=written service=test error=none"

        if ($oldRuntimeLogDir) { $script:RuntimeLogDir = $oldRuntimeLogDir.Value }
        else { Remove-Variable -Name RuntimeLogDir -Scope Script -ErrorAction SilentlyContinue }
        if ($oldClockLogPath) { $script:ClockLogPath = $oldClockLogPath.Value }
        else { Remove-Variable -Name ClockLogPath -Scope Script -ErrorAction SilentlyContinue }
    }

    It 'serializes two real wrapper processes without corrupting PID-labeled evidence' {
        $root = Join-Path $TestDrive 'wrapper-contention'
        $runtimeDir = Join-Path $root 'logs\runtime'
        $clockLog = Join-Path $runtimeDir 'clock_sync.log'
        $holderSignal = Join-Path $root 'holder.signal'
        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null

        $functionSource = @(
            'Get-MarketDayBootstrapMutexName',
            'Enter-MarketDayBootstrapLock',
            'Exit-MarketDayBootstrapLock',
            'Write-MarketClockLog'
        ) | ForEach-Object {
            $name = $_
            @($startMarketDayAst.FindAll(
                {
                    param($candidate)
                    $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                        $candidate.Name -eq $name
                },
                $true
            ))[0].Extent.Text
        }
        $escapedRoot = $root.Replace("'", "''")
        $escapedRuntimeDir = $runtimeDir.Replace("'", "''")
        $escapedClockLog = $clockLog.Replace("'", "''")
        $escapedHolderSignal = $holderSignal.Replace("'", "''")
        $common = @"
`$ErrorActionPreference = 'Stop'
`$ProjectRoot = '$escapedRoot'
`$RuntimeLogDir = '$escapedRuntimeDir'
`$ClockLogPath = '$escapedClockLog'
$($functionSource -join "`r`n")
"@
        $holderCommand = $common + @"
`$lockHandle = Enter-MarketDayBootstrapLock -ProjectRoot `$ProjectRoot -TimeoutMilliseconds 5000
if (-not `$lockHandle.Acquired) { exit 2 }
try {
    [IO.File]::WriteAllText('$escapedHolderSignal', 'ready')
    Start-Sleep -Milliseconds 750
    Write-MarketClockLog -Result ([pscustomobject]@{Status='holder';Service='test';Error=`$null})
}
finally {
    Exit-MarketDayBootstrapLock -LockHandle `$lockHandle
}
"@
        $contenderCommand = $common + @"
`$lockHandle = Enter-MarketDayBootstrapLock -ProjectRoot `$ProjectRoot -TimeoutMilliseconds 5000
if (-not `$lockHandle.Acquired) { exit 2 }
try {
    Write-MarketClockLog -Result ([pscustomobject]@{Status='contender';Service='test';Error=`$null})
}
finally {
    Exit-MarketDayBootstrapLock -LockHandle `$lockHandle
}
"@
        $powerShellExe = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
        $holderEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($holderCommand))
        $contenderEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($contenderCommand))
        $holder = $null
        $contender = $null
        try {
            $holder = Start-Process -FilePath $powerShellExe `
                -ArgumentList @('-NoProfile', '-NonInteractive', '-EncodedCommand', $holderEncoded) `
                -WindowStyle Hidden `
                -PassThru
            $deadline = [DateTime]::UtcNow.AddSeconds(5)
            while (-not (Test-Path -LiteralPath $holderSignal) -and [DateTime]::UtcNow -lt $deadline) {
                Start-Sleep -Milliseconds 25
            }
            Test-Path -LiteralPath $holderSignal | Should Be $true

            $contender = Start-Process -FilePath $powerShellExe `
                -ArgumentList @('-NoProfile', '-NonInteractive', '-EncodedCommand', $contenderEncoded) `
                -WindowStyle Hidden `
                -PassThru
            $holder | Wait-Process -Timeout 10
            $contender | Wait-Process -Timeout 10
            $holder.Refresh()
            $contender.Refresh()
            $holder.ExitCode | Should Be 0
            $contender.ExitCode | Should Be 0

            $lines = @(Get-Content -LiteralPath $clockLog)
            $lines.Count | Should Be 2
            $lines[0] | Should Match "powershell_pid=$($holder.Id) status=holder service=test error=none"
            $lines[1] | Should Match "powershell_pid=$($contender.Id) status=contender service=test error=none"
        }
        finally {
            foreach ($process in @($holder, $contender)) {
                if ($process -and -not $process.HasExited) {
                    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
                }
            }
        }
    }
}

Describe 'Expected listener PID guard' {
    It 'accepts the one listener that matches the observed PID' {
        Assert-MarketAppExpectedListenerPid `
            -Port 65001 `
            -ExpectedPid 1234 `
            -ListenerProcessIds @(1234) | Should Be 1234
    }

    It 'fails closed when the listener PID changed' {
        $message = $null
        try {
            Assert-MarketAppExpectedListenerPid `
                -Port 65001 `
                -ExpectedPid 1234 `
                -ListenerProcessIds @(5678)
        }
        catch {
            $message = $_.Exception.Message
        }
        $message | Should Match 'expected 1234, actual 5678.*No process was changed'
    }

    It 'fails closed when the listener vanished or became ambiguous' {
        $missingMessage = $null
        try {
            Assert-MarketAppExpectedListenerPid `
                -Port 65001 `
                -ExpectedPid 1234 `
                -ListenerProcessIds @()
        }
        catch {
            $missingMessage = $_.Exception.Message
        }
        $missingMessage | Should Match 'actual none.*No process was changed'

        $ambiguousMessage = $null
        try {
            Assert-MarketAppExpectedListenerPid `
                -Port 65001 `
                -ExpectedPid 1234 `
                -ListenerProcessIds @(1234, 5678)
        }
        catch {
            $ambiguousMessage = $_.Exception.Message
        }
        $ambiguousMessage | Should Match 'actual 1234,5678.*No process was changed'
    }
}

Describe 'Exact process network quiescence' {
    InModuleScope market_app_supervisor {
        It 'counts every TCP state owned by the exact PID without prefix collisions' {
            $lines = @(
                '  TCP    127.0.0.1:8000       0.0.0.0:0          LISTENING       4242',
                '  TCP    10.0.0.5:51000       1.2.3.4:13000      ESTABLISHED     4242',
                '  TCP    10.0.0.5:51001       1.2.3.4:13000      BOUND           4242',
                '  TCP    10.0.0.5:51002       1.2.3.4:13000      ESTABLISHED     42420',
                '  UDP    0.0.0.0:5353          *:*                                4242'
            )

            Get-MarketAppProcessTcpConnectionCountFromNetstatLines `
                -ProcessId 4242 `
                -Lines $lines | Should Be 3
            @(Get-MarketAppProcessTcpConnectionStatesFromNetstatLines `
                -ProcessId 4242 `
                -Lines $lines) | Should Be @('LISTENING', 'ESTABLISHED', 'BOUND')
        }

        It 'requires exact PID exit and no active TCP rows' {
            Mock Get-MarketAppProcessRecord { $null }
            Mock Get-MarketAppProcessTcpConnectionStates { @() }

            $quiescent = Wait-MarketAppProcessNetworkQuiescence `
                -ProcessId 4242 `
                -TimeoutSeconds 0

            $quiescent.Quiescent | Should Be $true
            $quiescent.ProcessExists | Should Be $false
            $quiescent.TcpConnectionCount | Should Be 0
            $quiescent.ResidualClosingOnly | Should Be $false

            Mock Get-MarketAppProcessRecord { [pscustomobject]@{ ProcessId = 4242 } }
            $processStillAlive = Wait-MarketAppProcessNetworkQuiescence `
                -ProcessId 4242 `
                -TimeoutSeconds 0
            $processStillAlive.Quiescent | Should Be $false
            $processStillAlive.ProcessExists | Should Be $true

            Mock Get-MarketAppProcessRecord { $null }
            Mock Get-MarketAppProcessTcpConnectionStates { @('ESTABLISHED') }
            $socketsStillOwned = Wait-MarketAppProcessNetworkQuiescence `
                -ProcessId 4242 `
                -TimeoutSeconds 0
            $socketsStillOwned.Quiescent | Should Be $false
            $socketsStillOwned.TcpConnectionCount | Should Be 1
            $socketsStillOwned.ActiveTcpConnectionCount | Should Be 1
        }

        It 'accepts a confirmed-dead PID after two residual closing-state samples' {
            Mock Get-MarketAppProcessRecord { $null }
            Mock Get-MarketAppProcessTcpConnectionStates { @('FIN_WAIT_2', 'TIME_WAIT') }

            $quiescent = Wait-MarketAppProcessNetworkQuiescence `
                -ProcessId 4242 `
                -TimeoutSeconds 1 `
                -PollMilliseconds 10

            $quiescent.Quiescent | Should Be $true
            $quiescent.ProcessExists | Should Be $false
            $quiescent.TcpConnectionCount | Should Be 2
            $quiescent.ActiveTcpConnectionCount | Should Be 0
            $quiescent.ResidualClosingOnly | Should Be $true
            @($quiescent.TcpStates) | Should Be @('FIN_WAIT_2', 'TIME_WAIT')
            Assert-MockCalled Get-MarketAppProcessTcpConnectionStates -Times 2 -Scope It
        }

        It 'fails closed if the PID reappears while residual closing rows remain' {
            $script:processRead = 0
            Mock Get-MarketAppProcessRecord {
                $script:processRead++
                if ($script:processRead -eq 1) { return $null }
                return [pscustomobject]@{ ProcessId = 4242 }
            }
            Mock Get-MarketAppProcessTcpConnectionStates { @('FIN_WAIT_2') }

            $result = Wait-MarketAppProcessNetworkQuiescence `
                -ProcessId 4242 `
                -TimeoutSeconds 1 `
                -PollMilliseconds 10

            $result.Quiescent | Should Be $false
            $result.ProcessExists | Should Be $true
            $result.ResidualClosingOnly | Should Be $false
        }
    }
}

Describe 'Verified orphan backend cleanup' {
    InModuleScope market_app_supervisor {
        It 'matches the canonical backend argument without matching Copilot MCP helper filenames' {
            $projectRoot = 'C:\MarketPinPredictor'
            $records = @(
                [pscustomobject]@{
                    ProcessId = 4101
                    ParentProcessId = 4000
                    CreationDate = [datetime]'2026-09-16T02:00:00'
                    ExecutablePath = 'C:\MarketPinPredictor\.venv\Scripts\python.exe'
                    CommandLine = '"C:\MarketPinPredictor\.venv\Scripts\python.exe" "C:\MarketPinPredictor\tools\marketpin_mcp_server.py"'
                },
                [pscustomobject]@{
                    ProcessId = 4102
                    ParentProcessId = 4000
                    CreationDate = [datetime]'2026-09-16T02:00:01'
                    ExecutablePath = 'C:\MarketPinPredictor\.venv\Scripts\python.exe'
                    CommandLine = '"C:\MarketPinPredictor\.venv\Scripts\python.exe" "C:\MarketPinPredictor\server.py"'
                },
                [pscustomobject]@{
                    ProcessId = 4103
                    ParentProcessId = 4000
                    CreationDate = [datetime]'2026-09-16T02:00:02'
                    ExecutablePath = 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
                    CommandLine = 'powershell.exe -Command "Review C:\MarketPinPredictor\server.py"'
                }
            )

            $identities = @(Get-MarketAppDirectBackendProcessIdentities `
                -ProjectRoot $projectRoot `
                -RequiredCommandMarkers @('server.py', 'backend.app:app') `
                -ProcessRecords $records)

            $identities.Count | Should Be 1
            $identities[0].ProcessId | Should Be 4102
            Test-MarketAppCommandLineMarker `
                -CommandLine $records[0].CommandLine `
                -Marker 'server.py' `
                -ProjectRoot $projectRoot | Should Be $false
        }

        It 'requires the canonical executable and entrypoint for direct component identity' {
            $projectRoot = 'C:\MarketPinPredictor'
            $backend = [pscustomobject]@{
                ExecutablePath = 'C:\MarketPinPredictor\.venv\Scripts\python.exe'
                CommandLine = '"C:/MarketPinPredictor/.venv/Scripts/python.exe" "C:/MarketPinPredictor/server.py"'
            }
            $dashboard = [pscustomobject]@{
                ExecutablePath = 'C:\MarketPinPredictor\.venv\Scripts\streamlit.exe'
                CommandLine = '"C:\MarketPinPredictor\.venv\Scripts\streamlit.exe" run "C:\MarketPinPredictor\app.py"'
            }
            $prompt = [pscustomobject]@{
                ExecutablePath = 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
                CommandLine = 'powershell.exe -Command "inspect C:\MarketPinPredictor\server.py"'
            }

            Test-MarketAppDirectComponentProcessRecord `
                -ProcessRecord $backend `
                -ProjectRoot $projectRoot `
                -Component backend | Should Be $true
            Test-MarketAppDirectComponentProcessRecord `
                -ProcessRecord $dashboard `
                -ProjectRoot $projectRoot `
                -Component dashboard | Should Be $true
            Test-MarketAppDirectComponentProcessRecord `
                -ProcessRecord $prompt `
                -ProjectRoot $projectRoot `
                -Component backend | Should Be $false
        }

        It 'rejects lookalike Python entrypoints and accepts exact module arguments' {
            $projectRoot = 'C:\MarketPinPredictor'

            Test-MarketAppCommandLineMarker `
                -CommandLine 'python C:\MarketPinPredictor\simple_market_server.py' `
                -Marker 'server.py' `
                -ProjectRoot $projectRoot | Should Be $false
            Test-MarketAppCommandLineMarker `
                -CommandLine 'python C:\MarketPinPredictor\server.py.backup' `
                -Marker 'server.py' `
                -ProjectRoot $projectRoot | Should Be $false
            Test-MarketAppCommandLineMarker `
                -CommandLine 'python -m uvicorn backend.app:app --port 8000' `
                -Marker 'backend.app:app' `
                -ProjectRoot $projectRoot | Should Be $true
        }

        It 'extracts only positive exact remote-port TCP owners' {
            $lines = @(
                '  TCP    10.0.0.5:51000       1.2.3.4:13000      ESTABLISHED     4242',
                '  TCP    10.0.0.5:51001       1.2.3.4:13000      TIME_WAIT       0',
                '  TCP    10.0.0.5:13000       1.2.3.4:443        ESTABLISHED     4343',
                '  TCP    10.0.0.5:51002       1.2.3.4:113000     ESTABLISHED     4444',
                '  TCP    [::1]:51003           [::2]:13000        SYN_SENT        4545'
            )

            @(Get-MarketAppRemoteTcpOwnerProcessIdsFromNetstatLines `
                -RemotePort 13000 `
                -Lines $lines) | Should Be @(4242, 4545)
        }

        It 'is a no-op when no listener, backend process, or provider owner exists' {
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {}
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @() }

            $result = Invoke-MarketAppVerifiedOrphanBackendCleanup `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -InvocationId 'test-empty-orphan-cleanup' `
                -Caller 'Pester'

            $result.Cleaned | Should Be $false
            @($result.ProcessIds).Count | Should Be 0
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'fails closed without stopping an unverified remote-port owner' {
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {}
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(9999) }

            $caught = $null
            try {
                Invoke-MarketAppVerifiedOrphanBackendCleanup `
                    -ProjectRoot 'C:\MarketPinPredictor' `
                    -InvocationId 'test-unverified-provider-owner' `
                    -Caller 'Pester'
            }
            catch {
                $caught = $_.Exception.Message
            }

            $caught | Should Match 'unverified owner PID 9999.*no process was changed'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'stops only a revalidated backend tree and proves every exact PID quiescent' {
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {
                param($ProcessId)
                [pscustomobject]@{
                    Quiescent = $true
                    ProcessId = $ProcessId
                    ProcessExists = $false
                    TcpConnectionCount = 0
                    ElapsedMilliseconds = 1
                }
            }
            $script:directIdentityCall = 0
            $script:providerOwnerCall = 0
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities {
                $script:directIdentityCall++
                if ($script:directIdentityCall -eq 1) {
                    [pscustomobject]@{
                        ProcessId = 4000
                        ParentProcessId = 100
                        IdentityToken = '4000|1'
                    }
                }
                else {
                    @()
                }
            }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds {
                $script:providerOwnerCall++
                if ($script:providerOwnerCall -eq 1) { @(4001) } else { @() }
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                $ProcessId -eq 4001 -and $AncestorProcessId -eq 4000
            }
            Mock Get-MarketAppDescendantProcessIds { @(4001) }
            Mock Get-MarketAppProcessRecord {
                param($ProcessId)
                [pscustomobject]@{
                    ProcessId = $ProcessId
                    ParentProcessId = if ($ProcessId -eq 4001) { 4000 } else { 100 }
                    CreationDate = [datetime]'2026-09-09T07:45:00'
                    CommandLine = 'verified test backend'
                    ExecutablePath = 'C:\MarketPinPredictor\.venv\Scripts\python.exe'
                }
            }
            Mock Test-MarketAppVerifiedProcess { $true }

            $result = Invoke-MarketAppVerifiedOrphanBackendCleanup `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -InvocationId 'test-verified-orphan-cleanup' `
                -Caller 'Pester'

            $result.Cleaned | Should Be $true
            @($result.ProcessIds) | Should Be @(4001, 4000)
            @($result.ProviderOwnerProcessIds) | Should Be @(4001)
            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4001 }
            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4000 }
            Assert-MockCalled Wait-MarketAppProcessNetworkQuiescence -Times 2 -Scope It
        }

        It 'fails closed when a listener appears after discovery' {
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {}
            $script:listenerCheck = 0
            Mock Get-MarketAppListenerProcessIds {
                $script:listenerCheck++
                if ($script:listenerCheck -eq 1) { @() } else { @(7777) }
            }
            Mock Get-MarketAppDirectBackendProcessIdentities {
                [pscustomobject]@{
                    ProcessId = 4000
                    ParentProcessId = 100
                    IdentityToken = '4000|1'
                }
            }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @() }
            Mock Get-MarketAppDescendantProcessIds { @() }
            Mock Get-MarketAppProcessRecord {
                [pscustomobject]@{
                    ProcessId = 4000
                    ParentProcessId = 100
                    CreationDate = [datetime]'2026-09-09T07:45:00'
                }
            }
            Mock Test-MarketAppVerifiedProcess { $true }

            $caught = $null
            try {
                Invoke-MarketAppVerifiedOrphanBackendCleanup `
                    -ProjectRoot 'C:\MarketPinPredictor' `
                    -InvocationId 'test-listener-race' `
                    -Caller 'Pester'
            }
            catch {
                $caught = $_.Exception.Message
            }

            $caught | Should Match 'listener appeared.*during orphan-backend cleanup.*no process was changed'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'fails closed when a captured PID changes identity before stop' {
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {}
            $script:recordRead = 0
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities {
                [pscustomobject]@{
                    ProcessId = 4000
                    ParentProcessId = 100
                    IdentityToken = '4000|1'
                }
            }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @() }
            Mock Get-MarketAppDescendantProcessIds { @() }
            Mock Get-MarketAppProcessRecord {
                $script:recordRead++
                [pscustomobject]@{
                    ProcessId = 4000
                    ParentProcessId = 100
                    CreationDate = if ($script:recordRead -eq 1) {
                        [datetime]'2026-09-09T07:45:00'
                    }
                    else {
                        [datetime]'2026-09-10T07:45:00'
                    }
                }
            }
            Mock Test-MarketAppVerifiedProcess { $true }

            $caught = $null
            try {
                Invoke-MarketAppVerifiedOrphanBackendCleanup `
                    -ProjectRoot 'C:\MarketPinPredictor' `
                    -InvocationId 'test-pid-reuse' `
                    -Caller 'Pester'
            }
            catch {
                $caught = $_.Exception.Message
            }

            $caught | Should Match 'PID 4000 changed identity before stop'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'never reports success while any captured PID still owns TCP rows' {
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities {
                [pscustomobject]@{
                    ProcessId = 4000
                    ParentProcessId = 100
                    IdentityToken = '4000|1'
                }
            }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @() }
            Mock Get-MarketAppDescendantProcessIds { @() }
            Mock Get-MarketAppProcessRecord {
                [pscustomobject]@{
                    ProcessId = 4000
                    ParentProcessId = 100
                    CreationDate = [datetime]'2026-09-09T07:45:00'
                }
            }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Wait-MarketAppProcessNetworkQuiescence {
                [pscustomobject]@{
                    Quiescent = $false
                    ProcessId = 4000
                    ProcessExists = $false
                    TcpConnectionCount = 9
                    ElapsedMilliseconds = 30000
                }
            }

            $caught = $null
            try {
                Invoke-MarketAppVerifiedOrphanBackendCleanup `
                    -ProjectRoot 'C:\MarketPinPredictor' `
                    -InvocationId 'test-orphan-socket-quiescence' `
                    -Caller 'Pester'
            }
            catch {
                $caught = $_.Exception.Message
            }

            $caught | Should Match 'exact-PID network quiescence.*tcp_connection_count=9'
            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4000 }
            Assert-MockCalled Write-MarketAppSupervisorLog -Times 0 -Scope It -ParameterFilter {
                $Event -eq 'orphan_backend_cleanup_quiescent'
            }
        }
    }
}

Describe 'Launch ownership and cleanup' {
    InModuleScope market_app_supervisor {
        It 'selects only a listener descending from the captured launch PID' {
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                return $ProcessId -eq 2222 -and $AncestorProcessId -eq 1111
            }

            Get-MarketAppOwnedListenerPid `
                -Port 65002 `
                -LaunchProcessId 1111 `
                -ListenerProcessIds @(2222) | Should Be 2222
        }

        It 'returns no owner when another process won the port' {
            Mock Test-MarketAppProcessDescendsFrom { $false }

            Get-MarketAppOwnedListenerPid `
                -Port 65002 `
                -LaunchProcessId 1111 `
                -ListenerProcessIds @(9999) | Should BeNullOrEmpty
        }

        It 'returns no owner when the listener set is ambiguous' {
            Mock Test-MarketAppProcessDescendsFrom { $true }

            Get-MarketAppOwnedListenerPid `
                -Port 65002 `
                -LaunchProcessId 1111 `
                -ListenerProcessIds @(2222, 9999) | Should BeNullOrEmpty
        }

        It 'cleans only the captured launch tree' {
            Mock Get-MarketAppDescendantProcessIds { @(110, 120) }
            Mock Stop-Process {}

            Stop-MarketAppAttemptedLaunch -LaunchProcessId 100

            Assert-MockCalled Stop-Process -Times 1 -ParameterFilter { $Id -eq 100 }
            Assert-MockCalled Stop-Process -Times 1 -ParameterFilter { $Id -eq 110 }
            Assert-MockCalled Stop-Process -Times 1 -ParameterFilter { $Id -eq 120 }
            Assert-MockCalled Stop-Process -Times 0 -ParameterFilter { $Id -eq 999 }
        }
    }
}

Describe 'Owned endpoint response contracts' {
    It 'accepts only the exact Databento backend health endpoint and JSON shape' {
        $response = [pscustomobject]@{
            StatusCode = 200
            Content = (@{
                status = 'degraded'
                market_data_provider = 'databento'
                symbols_requested = @('SPX', 'NDX')
                universe_provenance = @{ trading_date = '2026-09-08' }
                orb_reference_sampler = @{ thread_alive = $true; interval_seconds = 5 }
            } | ConvertTo-Json -Depth 4 -Compress)
        }

        Test-MarketAppEndpointResponseContract `
            -Url 'http://127.0.0.1:8000/health' `
            -Port 8000 `
            -ExpectedEndpointContract 'BackendHealth' `
            -Response $response | Should Be $true

        Test-MarketAppEndpointResponseContract `
            -Url 'http://127.0.0.1:8000/not-health' `
            -Port 8000 `
            -ExpectedEndpointContract 'BackendHealth' `
            -Response $response | Should Be $false
    }

    It 'rejects a 404 or incomplete backend payload instead of treating it as ready' {
        $notFound = [pscustomobject]@{
            StatusCode = 404
            Content = '{"status":"degraded","market_data_provider":"databento"}'
        }
        Test-MarketAppEndpointResponseContract `
            -Url 'http://127.0.0.1:8000/health' `
            -Port 8000 `
            -ExpectedEndpointContract 'BackendHealth' `
            -Response $notFound | Should Be $false

        $incomplete = [pscustomobject]@{
            StatusCode = 200
            Content = '{"status":"healthy","market_data_provider":"databento"}'
        }
        Test-MarketAppEndpointResponseContract `
            -Url 'http://127.0.0.1:8000/health' `
            -Port 8000 `
            -ExpectedEndpointContract 'BackendHealth' `
            -Response $incomplete | Should Be $false
    }

    It 'requires the Streamlit health endpoint to return exact 200 ok text' {
        $ready = [pscustomobject]@{StatusCode=200;Content="ok`n"}
        $html = [pscustomobject]@{StatusCode=200;Content='<html>app shell</html>'}

        Test-MarketAppEndpointResponseContract `
            -Url 'http://127.0.0.1:8501/_stcore/health' `
            -Port 8501 `
            -ExpectedEndpointContract 'StreamlitHealth' `
            -Response $ready | Should Be $true
        Test-MarketAppEndpointResponseContract `
            -Url 'http://127.0.0.1:8501/_stcore/health' `
            -Port 8501 `
            -ExpectedEndpointContract 'StreamlitHealth' `
            -Response $html | Should Be $false
    }
}

Describe 'Automatic listener stop-time boundary' {
    InModuleScope market_app_supervisor {
        It 'abstains when an 08:24:59 decision reaches its final stop check at 08:25:00' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-09-08T08:25:00' }
            Mock Stop-Process {}

            $result = Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-09-08T08:24:59') `
                -InvocationId 'test-boundary' `
                -Caller 'Pester' `
                -RecoveryReason 'backend_readiness_recovery_requested'

            $result.Stopped | Should Be $false
            $result.Reason | Should Be 'protected_opening_boundary_reached'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
            Assert-MockCalled Write-MarketAppSupervisorLog -Times 1 -ParameterFilter {
                $Event -eq 'automatic_stop_deferred' -and
                $Message -match 'action=preserve_listener'
            }
        }

        It 'permits one explicitly authorized late salvage through the same exact-owner stop guard' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-09-08T08:25:00' }
            Mock Stop-Process {}
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Wait-MarketAppProcessNetworkQuiescence {
                [pscustomobject]@{
                    Quiescent = $true
                    ProcessId = 4242
                    ProcessExists = $false
                    TcpConnectionCount = 0
                    ElapsedMilliseconds = 3
                }
            }
            $state = [pscustomobject]@{
                HealthReachable = $true
                LiveHealthReachable = $true
                HealthEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-09-08'
                        cash_close_utc = '2026-09-08T20:00:00+00:00'
                    }
                }
                LiveEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-09-08'
                        cash_close_utc = '2026-09-08T20:00:00+00:00'
                    }
                }
            }

            $result = Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-09-08T08:25:00') `
                -InvocationId 'test-late-salvage' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_prior_day_backend_late_salvage' `
                -RuntimeState $state `
                -AllowLateSessionSalvage

            $result.Stopped | Should Be $true
            $result.Reason | Should Be 'stopped_late_session_salvage'
            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4242 }
            Assert-MockCalled Write-MarketAppSupervisorLog -Times 1 -ParameterFilter {
                $Event -eq 'automatic_stop_late_salvage_authorized' -and
                $Message -match 'action=stop_once'
            }
        }

        It 'does not claim a late salvage succeeded when exact-PID termination is denied' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-09-08T08:25:00' }
            Mock Stop-Process { throw 'access denied at stop time' }

            { Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8501 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('streamlit', 'app.py') `
                -Component 'dashboard' `
                -DecisionTime ([datetime]'2026-09-08T08:25:00') `
                -InvocationId 'test-stop-denied' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_prior_day_dashboard_late_salvage' `
                -AllowLateSessionSalvage } | Should Throw 'access denied at stop time'

            Assert-MockCalled Stop-Process -Times 1 -ParameterFilter { $Id -eq 4242 }
            # Only prepared + late-authorized can be written before the denied
            # stop; a third verified-stop event would be a false success.
            Assert-MockCalled Write-MarketAppSupervisorLog -Times 2
        }

        It 'fails closed if another listener acquires the service port after stop' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-09-08T08:24:00' }
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {
                [pscustomobject]@{
                    Quiescent = $true
                    ProcessId = 4242
                    ProcessExists = $false
                    TcpConnectionCount = 1
                    ActiveTcpConnectionCount = 0
                    ResidualClosingOnly = $true
                    TcpStates = @('FIN_WAIT_2')
                    ElapsedMilliseconds = 250
                }
            }
            Mock Get-MarketAppListenerProcessIds { @(9001) }

            { Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-09-08T08:24:00') `
                -InvocationId 'test-post-stop-listener-race' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_prior_day_backend_preopen_refresh' } |
                Should Throw 'replacement ownership is ambiguous'
        }

        It 'abstains when a dead-handoff stop reaches the verified early cash close' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-11-27T12:00:00' }
            Mock Stop-Process {}
            $state = [pscustomobject]@{
                HealthReachable = $true
                LiveHealthReachable = $true
                HealthEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-11-27'
                        cash_close_utc = '2026-11-27T18:00:00+00:00'
                    }
                }
                LiveEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-11-27'
                        cash_close_utc = '2026-11-27T18:00:00+00:00'
                    }
                }
            }

            $result = Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-11-27T11:59:59') `
                -InvocationId 'test-early-close-boundary' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_current_day_dead_handoff_late_salvage' `
                -RuntimeState $state `
                -AllowLateSessionSalvage

            $result.Stopped | Should Be $false
            $result.Reason | Should Be 'dead_handoff_outside_cash_session'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'abstains when a prior-day backend stop crosses the verified early cash close' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-11-27T12:00:00' }
            Mock Stop-Process {}
            $state = [pscustomobject]@{
                HealthReachable = $true
                LiveHealthReachable = $true
                HealthEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-11-27'
                        cash_close_utc = '2026-11-27T18:00:00+00:00'
                    }
                }
                LiveEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-11-27'
                        cash_close_utc = '2026-11-27T18:00:00+00:00'
                    }
                }
            }

            $result = Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-11-27T11:59:59') `
                -InvocationId 'test-prior-day-early-close-boundary' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_prior_day_backend_late_salvage' `
                -RuntimeState $state `
                -AllowLateSessionSalvage

            $result.Stopped | Should Be $false
            $result.Reason | Should Be 'prior_day_backend_outside_cash_session'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'preserves ordinary-session dead-handoff salvage before the verified normal close' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-09-08T12:00:00' }
            Mock Stop-Process {}
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Wait-MarketAppProcessNetworkQuiescence {
                [pscustomobject]@{
                    Quiescent = $true
                    ProcessId = 4242
                    ProcessExists = $false
                    TcpConnectionCount = 0
                    ElapsedMilliseconds = 3
                }
            }
            $state = [pscustomobject]@{
                HealthReachable = $true
                LiveHealthReachable = $true
                HealthEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-09-08'
                        cash_close_utc = '2026-09-08T20:00:00+00:00'
                    }
                }
                LiveEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-09-08'
                        cash_close_utc = '2026-09-08T20:00:00+00:00'
                    }
                }
            }

            $result = Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-09-08T11:59:59') `
                -InvocationId 'test-normal-close-boundary' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_current_day_dead_handoff_late_salvage' `
                -RuntimeState $state `
                -AllowLateSessionSalvage

            $result.Stopped | Should Be $true
            $result.Reason | Should Be 'stopped_late_session_salvage'
            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4242 }
        }

        It 'fails closed when health and live disagree on the cash-close instant' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-11-27T11:55:00' }
            Mock Stop-Process {}
            $state = [pscustomobject]@{
                HealthReachable = $true
                LiveHealthReachable = $true
                HealthEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-11-27'
                        cash_close_utc = '2026-11-27T18:00:00+00:00'
                    }
                }
                LiveEvidence = [pscustomobject]@{
                    subscription_window = [pscustomobject]@{
                        trading_date = '2026-11-27'
                        cash_close_utc = '2026-11-27T21:00:00+00:00'
                    }
                }
            }

            $result = Invoke-MarketAppBoundedAutomaticListenerStop `
                -Port 8000 `
                -ExpectedPid 4242 `
                -ProjectRoot 'C:\MarketPinPredictor' `
                -RequiredCommandMarkers @('server.py') `
                -Component 'backend' `
                -DecisionTime ([datetime]'2026-11-27T11:54:59') `
                -InvocationId 'test-close-mismatch' `
                -Caller 'Pester' `
                -RecoveryReason 'verified_current_day_dead_handoff_late_salvage' `
                -RuntimeState $state `
                -AllowLateSessionSalvage

            $result.Stopped | Should Be $false
            $result.Reason | Should Be 'dead_handoff_cash_close_unverified'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'fails closed after stop when the exact PID still owns provider sockets' {
            Mock Assert-MarketAppExpectedListenerPid { 4242 }
            Mock Test-MarketAppVerifiedProcess { $true }
            Mock Write-MarketAppSupervisorLog {}
            Mock Get-Date { [datetime]'2026-09-08T08:00:00' }
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence {
                [pscustomobject]@{
                    Quiescent = $false
                    ProcessId = 4242
                    ProcessExists = $false
                    TcpConnectionCount = 9
                    ElapsedMilliseconds = 30000
                }
            }

            $caught = $null
            try {
                Invoke-MarketAppBoundedAutomaticListenerStop `
                    -Port 8000 `
                    -ExpectedPid 4242 `
                    -ProjectRoot 'C:\MarketPinPredictor' `
                    -RequiredCommandMarkers @('server.py') `
                    -Component 'backend' `
                    -DecisionTime ([datetime]'2026-09-08T08:00:00') `
                    -InvocationId 'test-provider-socket-quiescence' `
                    -Caller 'Pester' `
                    -RecoveryReason 'backend_readiness_recovery_requested'
            }
            catch {
                $caught = $_.Exception.Message
            }
            $caught | Should Match 'exact-PID network quiescence.*tcp_connection_count=9'

            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4242 }
            Assert-MockCalled Write-MarketAppSupervisorLog -Times 0 -Scope It -ParameterFilter {
                $Event -eq 'automatic_stop_verified_listener'
            }
        }
    }
}

Describe 'Closing-tape process ownership' {
    InModuleScope market_app_supervisor {
        It 'returns only a verified recorder PID from the daily evidence file' {
            $root = Join-Path $TestDrive 'verified-recorder'
            $dayDir = Join-Path $root 'data\closing_tape\2026-08-25'
            New-Item -ItemType Directory -Path $dayDir -Force | Out-Null
            Set-Content -LiteralPath (Join-Path $dayDir 'recorder.pid') -Value '4321'
            Mock Test-MarketAppVerifiedProcess { $true }

            Get-MarketAppVerifiedRecorderProcessId `
                -ProjectRoot $root `
                -TradingDate '2026-08-25' | Should Be 4321
        }

        It 'fails closed for stale, malformed, or unverified recorder evidence' {
            $root = Join-Path $TestDrive 'unverified-recorder'
            $dayDir = Join-Path $root 'data\closing_tape\2026-08-25'
            New-Item -ItemType Directory -Path $dayDir -Force | Out-Null
            $pidPath = Join-Path $dayDir 'recorder.pid'
            Set-Content -LiteralPath $pidPath -Value 'not-a-pid'
            Get-MarketAppVerifiedRecorderProcessId `
                -ProjectRoot $root `
                -TradingDate '2026-08-25' | Should BeNullOrEmpty

            Set-Content -LiteralPath $pidPath -Value '9876'
            Mock Test-MarketAppVerifiedProcess { $false }
            Get-MarketAppVerifiedRecorderProcessId `
                -ProjectRoot $root `
                -TradingDate '2026-08-25' | Should BeNullOrEmpty
        }
    }
}

Describe 'Closing-tape status health' {
    It 'accepts a fresh running status' {
        $root = Join-Path $TestDrive 'fresh-recorder-status'
        $dayDir = Join-Path $root 'data\closing_tape\2026-08-25'
        New-Item -ItemType Directory -Path $dayDir -Force | Out-Null
        @{
            observed_at_utc = '2026-08-25T16:00:00+00:00'
            session = @{ status = 'running' }
        } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $dayDir 'status.json')

        $status = Get-MarketAppRecorderStatus `
            -ProjectRoot $root `
            -TradingDate '2026-08-25' `
            -NowUtc ([datetime]'2026-08-25T16:00:30Z')

        $status.Healthy | Should Be $true
        $status.AgeSeconds | Should Be 30
    }

    It 'reports stale, terminal, missing, and malformed evidence without claiming health' {
        $root = Join-Path $TestDrive 'bad-recorder-status'
        $dayDir = Join-Path $root 'data\closing_tape\2026-08-25'
        New-Item -ItemType Directory -Path $dayDir -Force | Out-Null
        $statusPath = Join-Path $dayDir 'status.json'

        @{
            observed_at_utc = '2026-08-25T15:55:00+00:00'
            session = @{ status = 'running' }
        } | ConvertTo-Json | Set-Content -LiteralPath $statusPath
        $stale = Get-MarketAppRecorderStatus `
            -ProjectRoot $root `
            -TradingDate '2026-08-25' `
            -NowUtc ([datetime]'2026-08-25T16:00:00Z')
        $stale.Healthy | Should Be $false
        $stale.Reason | Should Match 'older than'

        @{
            observed_at_utc = '2026-08-25T16:00:00+00:00'
            session = @{ status = 'incomplete' }
        } | ConvertTo-Json | Set-Content -LiteralPath $statusPath
        $terminal = Get-MarketAppRecorderStatus `
            -ProjectRoot $root `
            -TradingDate '2026-08-25' `
            -NowUtc ([datetime]'2026-08-25T16:00:01Z')
        $terminal.Healthy | Should Be $false
        $terminal.State | Should Be 'incomplete'

        Set-Content -LiteralPath $statusPath -Value '{bad json'
        (Get-MarketAppRecorderStatus `
            -ProjectRoot $root `
            -TradingDate '2026-08-25').State | Should Be 'unreadable'

        Remove-Item -LiteralPath $statusPath
        (Get-MarketAppRecorderStatus `
            -ProjectRoot $root `
            -TradingDate '2026-08-25').State | Should Be 'missing'
    }
}

Describe 'Supervisor logging' {
    It 'records invocation, caller, PowerShell PID, and event without command lines' {
        Write-MarketAppSupervisorLog `
            -ProjectRoot $TestDrive `
            -InvocationId 'test-invocation' `
            -Caller 'Pester' `
            -Event 'unit_test' `
            -Message 'component=none action=noop'

        $line = Get-Content -LiteralPath (Join-Path $TestDrive 'logs\runtime\watchdog.log') -Tail 1
        $line | Should Match 'invocation=test-invocation'
        $line | Should Match 'caller=Pester'
        $line | Should Match "powershell_pid=$PID"
        $line | Should Match 'event=unit_test component=none action=noop'
    }
}

Describe 'Per-launch runtime logs' {
    It 'retains every launch while keeping stable current-log paths' {
        $root = Join-Path $TestDrive 'launch-logs'
        $runtimeDir = Join-Path $root 'logs\runtime'
        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
        Set-Content -LiteralPath (Join-Path $runtimeDir 'backend.stdout.log') -Value 'legacy stdout'
        Set-Content -LiteralPath (Join-Path $runtimeDir 'backend.stderr.log') -Value 'legacy stderr'

        $first = New-MarketAppLaunchLogPaths `
            -ProjectRoot $root `
            -Component 'backend' `
            -InvocationId 'first-launch' `
            -NowUtc ([datetime]'2026-08-26T01:02:03Z')
        Set-Content -LiteralPath $first.StandardOutputPath -Value 'first launch stdout'
        Set-Content -LiteralPath $first.StandardErrorPath -Value 'first launch stderr'

        (Get-Content -LiteralPath $first.RetainedStandardOutputPath -Raw) | Should Match 'first launch stdout'
        (Get-Content -LiteralPath $first.RetainedStandardErrorPath -Raw) | Should Match 'first launch stderr'
        $legacyStdout = @(Get-ChildItem -LiteralPath (Split-Path $first.RetainedStandardOutputPath) `
            -Filter 'backend.rollover.*.stdout.log')
        $legacyStderr = @(Get-ChildItem -LiteralPath (Split-Path $first.RetainedStandardErrorPath) `
            -Filter 'backend.rollover.*.stderr.log')
        $legacyStdout.Count | Should Be 1
        $legacyStderr.Count | Should Be 1
        (Get-Content -LiteralPath $legacyStdout[0].FullName -Raw) | Should Match 'legacy stdout'
        (Get-Content -LiteralPath $legacyStderr[0].FullName -Raw) | Should Match 'legacy stderr'

        $second = New-MarketAppLaunchLogPaths `
            -ProjectRoot $root `
            -Component 'backend' `
            -InvocationId 'second-launch' `
            -NowUtc ([datetime]'2026-08-26T01:03:04Z')
        Set-Content -LiteralPath $second.StandardOutputPath -Value 'second launch stdout'

        $second.RetainedStandardOutputPath | Should Not Be $first.RetainedStandardOutputPath
        (Get-Content -LiteralPath $first.RetainedStandardOutputPath -Raw) | Should Match 'first launch stdout'
        (Get-Content -LiteralPath $second.RetainedStandardOutputPath -Raw) | Should Match 'second launch stdout'
        (Get-Content -LiteralPath (Join-Path $runtimeDir 'backend.stdout.log') -Raw) | Should Match 'second launch stdout'
    }

    It 'captures child stdout and stderr through stable links into retained files' {
        $root = Join-Path $TestDrive 'redirected-launch-logs'
        $paths = New-MarketAppLaunchLogPaths `
            -ProjectRoot $root `
            -Component 'backend' `
            -InvocationId 'redirect-smoke' `
            -NowUtc ([datetime]'2026-08-26T01:04:05Z')
        $childScript = '[Console]::Out.WriteLine("stdout-ok"); [Console]::Error.WriteLine("stderr-ok")'
        $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($childScript))
        $powershell = (Get-Command powershell.exe -ErrorAction Stop).Source

        $process = Start-Process `
            -FilePath $powershell `
            -ArgumentList @('-NoProfile', '-NonInteractive', '-EncodedCommand', $encoded) `
            -RedirectStandardOutput $paths.StandardOutputPath `
            -RedirectStandardError $paths.StandardErrorPath `
            -WindowStyle Hidden `
            -PassThru `
            -Wait

        $process.ExitCode | Should Be 0
        (Get-Content -LiteralPath $paths.RetainedStandardOutputPath -Raw) | Should Match 'stdout-ok'
        (Get-Content -LiteralPath $paths.RetainedStandardErrorPath -Raw) | Should Match 'stderr-ok'
    }

    It 'uses the retained launch path when a stopped process still locks a stable log' {
        $root = Join-Path $TestDrive 'locked-stable-log'
        $runtimeDir = Join-Path $root 'logs\runtime'
        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
        $stableStdout = Join-Path $runtimeDir 'dashboard.stdout.log'
        Set-Content -LiteralPath $stableStdout -Value 'locked legacy stdout'
        $handle = [System.IO.File]::Open(
            $stableStdout,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::Read
        )
        try {
            $paths = New-MarketAppLaunchLogPaths `
                -ProjectRoot $root `
                -Component 'dashboard' `
                -InvocationId 'locked-log-recovery' `
                -NowUtc ([datetime]'2026-09-01T08:25:00Z')
        }
        finally {
            $handle.Dispose()
        }

        $paths.StableStdoutAvailable | Should Be $false
        $paths.StandardOutputPath | Should Be $paths.RetainedStandardOutputPath
        (Get-Content -LiteralPath $stableStdout -Raw) | Should Match 'locked legacy stdout'
    }
}

Describe 'Post-close finalizer result handling' {
    It 'accepts a structured incomplete result as degraded evidence' {
        $resolution = Resolve-MarketAppPostCloseFinalizeResult `
            -Output @('{"action":"incomplete","session_id":"session-1","issues":["gap"]}') `
            -ExitCode 2

        $resolution.ExpectedIncomplete | Should Be $true
        $resolution.Result.action | Should Be 'incomplete'
    }

    It 'still fails closed for an unexpected nonzero result or malformed output' {
        $unexpectedExit = $null
        try {
            Resolve-MarketAppPostCloseFinalizeResult `
                -Output @('{"action":"finalized"}') `
                -ExitCode 2
        }
        catch {
            $unexpectedExit = $_
        }
        $unexpectedExit | Should Not BeNullOrEmpty

        $malformedOutput = $null
        try {
            Resolve-MarketAppPostCloseFinalizeResult `
                -Output @('not json') `
                -ExitCode 1
        }
        catch {
            $malformedOutput = $_
        }
        $malformedOutput | Should Not BeNullOrEmpty
    }

    It 'classifies an empty child result without a property dereference error' {
        $caught = $null
        try {
            Resolve-MarketAppPostCloseFinalizeResult -Output @() -ExitCode 1
        }
        catch {
            $caught = $_
        }

        $caught | Should Not BeNullOrEmpty
        $caught.Exception.Message | Should Match 'returned no output'
        $caught.Exception.GetType().Name | Should Not Be 'PropertyNotFoundException'
    }
}

Describe 'Pre-start universe cache preparation result handling' {
    It 'accepts a validated current-day cache' {
        $resolution = Resolve-MarketAppUniverseCachePreparationResult `
            -Output @('{"status":"ready","preparation_mode":"cache_only","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false}}') `
            -ExitCode 0 `
            -ExpectedPreparationMode 'cache_only'

        $resolution.UsesFallback | Should Be $false
        $resolution.ProvenanceLabel | Should Be 'CURRENT_DAY_CACHE'
    }

    It 'keeps prior-session startup visibly labeled as fallback' {
        $resolution = Resolve-MarketAppUniverseCachePreparationResult `
            -Output @('{"status":"fallback","preparation_mode":"cache_only","provenance_label":"PRIOR_SESSION_FALLBACK","current_day_cache_ready":false,"provenance":{"mode":"prior_cache_filtered","is_fallback":true}}') `
            -ExitCode 0 `
            -ExpectedPreparationMode 'cache_only'

        $resolution.UsesFallback | Should Be $true
        $resolution.ProvenanceLabel | Should Be 'PRIOR_SESSION_FALLBACK'
    }

    It 'rejects contradictory or failed cache provenance' {
        $contradictory = $null
        try {
            Resolve-MarketAppUniverseCachePreparationResult `
                -Output @('{"status":"fallback","preparation_mode":"cache_only","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":false,"provenance":{"mode":"prior_cache_filtered","is_fallback":true}}') `
                -ExitCode 0 `
                -ExpectedPreparationMode 'cache_only'
        }
        catch {
            $contradictory = $_
        }
        $contradictory | Should Not BeNullOrEmpty

        $failed = $null
        try {
            Resolve-MarketAppUniverseCachePreparationResult `
                -Output @('{"status":"failed","preparation_mode":"cache_only","error":"no cache"}') `
                -ExitCode 1 `
                -ExpectedPreparationMode 'cache_only'
        }
        catch {
            $failed = $_
        }
        $failed | Should Not BeNullOrEmpty
    }

    It 'requires the explicit cache-only mode marker' {
        $missingMarker = $null
        try {
            Resolve-MarketAppUniverseCachePreparationResult `
                -Output @('{"status":"ready","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false}}') `
                -ExitCode 0 `
                -ExpectedPreparationMode 'cache_only'
        }
        catch {
            $missingMarker = $_
        }
        $missingMarker | Should Not BeNullOrEmpty

        $wrongMarker = $null
        try {
            Resolve-MarketAppUniverseCachePreparationResult `
                -Output @('{"status":"ready","preparation_mode":"provider_allowed","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false}}') `
                -ExitCode 0 `
                -ExpectedPreparationMode 'cache_only'
        }
        catch {
            $wrongMarker = $_
        }
        $wrongMarker | Should Not BeNullOrEmpty
    }
}

Describe 'Bounded pre-start universe cache preparation' {
    It 'terminates the exact real Python process tree after a bounded timeout' {
        $root = Join-Path $TestDrive 'real-timeout-child'
        $toolDir = Join-Path $root 'tools'
        New-Item -ItemType Directory -Path $toolDir -Force | Out-Null
        $sleeper = Join-Path $toolDir 'sleep_preparer.py'
        Set-Content -LiteralPath $sleeper -Value @('import time', 'time.sleep(30)')
        $python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
        $module = Get-Module market_app_supervisor

        $result = & $module {
            param($Root, $Python, $Tool)
            Invoke-MarketAppUniversePreparationChild `
                -ProjectRoot $Root `
                -PythonExe $Python `
                -ToolPath $Tool `
                -Symbols 'SPX,NDX' `
                -InvocationId 'real-timeout-child' `
                -TimeoutMilliseconds 100 `
                -DeadlineUtc ([DateTime]::UtcNow.AddSeconds(10)) `
                -LogComponent 'universe-timeout-test'
        } $root $python $sleeper

        $result.TimedOut | Should Be $true
        $result.ExitedAndQuiescent | Should Be $true
        Get-Process -Id $result.ProcessId -ErrorAction SilentlyContinue |
            Should BeNullOrEmpty
        @(
            Get-CimInstance Win32_Process -ErrorAction Stop |
                Where-Object {
                    ([string]$_.CommandLine).IndexOf(
                        $sleeper,
                        [StringComparison]::OrdinalIgnoreCase
                    ) -ge 0
                }
        ).Count | Should Be 0
    }

    It 'allows provider discovery only when the full pre-open proof and launch budget remains' {
        Test-MarketAppUniverseProviderDiscoveryAllowed `
            -Now ([datetime]'2026-09-08T08:15:00') | Should Be $true
        Test-MarketAppUniverseProviderDiscoveryAllowed `
            -Now ([datetime]'2026-09-08T08:23:00') | Should Be $false
        Test-MarketAppUniverseProviderDiscoveryAllowed `
            -Now ([datetime]'2026-09-08T08:25:00') | Should Be $false
        Test-MarketAppUniverseProviderDiscoveryAllowed `
            -Now ([datetime]'2026-09-05T12:00:00') | Should Be $false
    }

    It 'passes an explicit trading date to the preparation CLI' {
        $root = Join-Path $TestDrive 'explicit-trading-date-child'
        $toolDir = Join-Path $root 'tools'
        New-Item -ItemType Directory -Path $toolDir -Force | Out-Null
        $argumentReporter = Join-Path $toolDir 'argument_reporter.py'
        Set-Content -LiteralPath $argumentReporter -Encoding ASCII -Value @(
            'import json',
            'import sys',
            'print(json.dumps(sys.argv[1:]))'
        )
        $python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
        $module = Get-Module market_app_supervisor

        $result = & $module {
            param($Root, $Python, $Tool)
            Invoke-MarketAppUniversePreparationChild `
                -ProjectRoot $Root `
                -PythonExe $Python `
                -ToolPath $Tool `
                -Symbols 'SPX,NDX,VIX,RUT' `
                -TradingDate '2026-09-09' `
                -InvocationId 'explicit-trading-date-child' `
                -TimeoutMilliseconds 5000 `
                -DeadlineUtc ([DateTime]::UtcNow.AddSeconds(10)) `
                -LogComponent 'universe-date-test' `
                -CacheOnly
        } $root $python $argumentReporter

        $result.Completed | Should Be $true
        $result.ExitedAndQuiescent | Should Be $true
        $arguments = @((($result.Output -join [Environment]::NewLine) | ConvertFrom-Json))
        ($arguments -join '|') | Should Be '--symbols|SPX,NDX,VIX,RUT|--trading-date|2026-09-09|--cache-only'
    }

    It 'approves startup only after a cache-only child exits quiescent with the required marker' {
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                if ($CacheOnly) {
                    return [pscustomobject]@{
                        Completed = $true
                        TimedOut = $false
                        ExitedAndQuiescent = $true
                        ExitCode = 0
                        Output = @('{"status":"ready","preparation_mode":"cache_only","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false}}')
                    }
                }
                return [pscustomobject]@{
                    Completed = $true
                    TimedOut = $false
                    ExitedAndQuiescent = $true
                    ExitCode = 0
                    Output = @('{"status":"staged","preparation_mode":"provider_allowed"}')
                }
            }

            $result = Invoke-MarketAppUniverseCachePreparation `
                -ProjectRoot $ProjectRoot `
                -PythonExe 'python.exe' `
                -Symbols 'SPX,NDX' `
                -Deadline ([DateTime]::UtcNow.AddSeconds(60)) `
                -InvocationId 'bounded-success'

            $result.StartupMayContinue | Should Be $true
            $result.Outcome | Should Be 'provider_completed_cache_only_validated'
            Assert-MockCalled -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -Times 2
            Assert-MockCalled -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -Times 1 -ParameterFilter {
                [bool]$CacheOnly
            }
        }

        It 'fails closed when the provider child exit or quiescence is unverified' {
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                [pscustomobject]@{
                    Completed = $false
                    TimedOut = $true
                    ExitedAndQuiescent = $false
                    ExitCode = $null
                    Output = @()
                }
            }

            $result = Invoke-MarketAppUniverseCachePreparation `
                -ProjectRoot $ProjectRoot `
                -PythonExe 'python.exe' `
                -Symbols 'SPX,NDX' `
                -Deadline ([DateTime]::UtcNow.AddSeconds(60)) `
                -InvocationId 'bounded-unverified-exit'

            $result.StartupMayContinue | Should Be $false
            $result.Outcome | Should Be 'provider_child_exit_or_quiescence_unverified'
            Assert-MockCalled -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -Times 1
        }

        It 'fails closed when cache-only output lacks the required mode marker' {
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                if ($CacheOnly) {
                    return [pscustomobject]@{
                        Completed = $true
                        TimedOut = $false
                        ExitedAndQuiescent = $true
                        ExitCode = 0
                        Output = @('{"status":"ready","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false}}')
                    }
                }
                return [pscustomobject]@{
                    Completed = $false
                    TimedOut = $true
                    ExitedAndQuiescent = $true
                    ExitCode = $null
                    Output = @()
                }
            }

            $result = Invoke-MarketAppUniverseCachePreparation `
                -ProjectRoot $ProjectRoot `
                -PythonExe 'python.exe' `
                -Symbols 'SPX,NDX' `
                -Deadline ([DateTime]::UtcNow.AddSeconds(30)) `
                -InvocationId 'bounded-invalid-cache-only'

            $result.StartupMayContinue | Should Be $false
            $result.Outcome | Should Be 'cache_only_result_invalid'
        }

        It 'skips provider discovery near the deadline but still permits validated cache-only startup' {
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                [pscustomobject]@{
                    Completed = $true
                    TimedOut = $false
                    ExitedAndQuiescent = $true
                    ExitCode = 0
                    Output = @('{"status":"fallback","preparation_mode":"cache_only","provenance_label":"PRIOR_SESSION_FALLBACK","current_day_cache_ready":false,"provenance":{"mode":"prior_cache_filtered","is_fallback":true}}')
                }
            }

            $result = Invoke-MarketAppUniverseCachePreparation `
                -ProjectRoot $ProjectRoot `
                -PythonExe 'python.exe' `
                -Symbols 'SPX,NDX' `
                -ProviderTimeoutSeconds 1 `
                -CacheValidationTimeoutSeconds 1 `
                -Deadline ([DateTime]::UtcNow.AddSeconds(5)) `
                -InvocationId 'bounded-cache-only'

            $result.StartupMayContinue | Should Be $true
            $result.Outcome | Should Be 'provider_skipped_cache_only_validated'
            $result.UsesFallback | Should Be $true
            Assert-MockCalled -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -Times 1 -ParameterFilter {
                [bool]$CacheOnly
            }
        }

        It 'forwards an explicit trading date to both children and accepts an exact cache-only match' {
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                if ($CacheOnly) {
                    return [pscustomobject]@{
                        Completed = $true
                        TimedOut = $false
                        ExitedAndQuiescent = $true
                        ExitCode = 0
                        Output = @('{"status":"ready","preparation_mode":"cache_only","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false,"trading_date":"2026-09-09"}}')
                    }
                }
                return [pscustomobject]@{
                    Completed = $true
                    TimedOut = $false
                    ExitedAndQuiescent = $true
                    ExitCode = 0
                    Output = @('{"status":"staged","preparation_mode":"provider_allowed"}')
                }
            }

            $result = Invoke-MarketAppUniverseCachePreparation `
                -ProjectRoot $ProjectRoot `
                -PythonExe 'python.exe' `
                -Symbols 'SPX,NDX,VIX,RUT' `
                -TradingDate '2026-09-09' `
                -Deadline ([DateTime]::UtcNow.AddSeconds(60)) `
                -InvocationId 'bounded-explicit-date'

            $result.StartupMayContinue | Should Be $true
            Assert-MockCalled -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -Times 2 -ParameterFilter {
                $TradingDate -eq '2026-09-09'
            }
            Assert-MockCalled -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -Times 1 -ParameterFilter {
                $TradingDate -eq '2026-09-09' -and [bool]$CacheOnly
            }
        }

        It 'fails closed when explicit-date cache-only provenance does not match' {
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                [pscustomobject]@{
                    Completed = $true
                    TimedOut = $false
                    ExitedAndQuiescent = $true
                    ExitCode = 0
                    Output = @('{"status":"ready","preparation_mode":"cache_only","provenance_label":"CURRENT_DAY_CACHE","current_day_cache_ready":true,"provenance":{"mode":"current_day_cache","is_fallback":false,"trading_date":"2026-09-08"}}')
                }
            }

            $result = Invoke-MarketAppUniverseCachePreparation `
                -ProjectRoot $ProjectRoot `
                -PythonExe 'python.exe' `
                -Symbols 'SPX,NDX,VIX,RUT' `
                -TradingDate '2026-09-09' `
                -ProviderTimeoutSeconds 1 `
                -CacheValidationTimeoutSeconds 1 `
                -Deadline ([DateTime]::UtcNow.AddSeconds(5)) `
                -InvocationId 'bounded-date-mismatch'

            $result.StartupMayContinue | Should Be $false
            $result.Outcome | Should Be 'cache_only_trading_date_mismatch'
            $result.ProvenanceLabel | Should Be 'NO_LAUNCH_SAFE_UNIVERSE'
            $result.Result | Should BeNullOrEmpty
            $result.ExpectedTradingDate | Should Be '2026-09-09'
            $result.ObservedTradingDate | Should Be '2026-09-08'
        }

        It 'rejects a calendar-invalid explicit trading date before launching a child' {
            $script:invalidDateChildCalled = $false
            Mock -CommandName Invoke-MarketAppUniversePreparationChild -ModuleName market_app_supervisor -MockWith {
                $script:invalidDateChildCalled = $true
                throw 'child must not run'
            }
            $caught = $null
            try {
                Invoke-MarketAppUniverseCachePreparation `
                    -ProjectRoot $ProjectRoot `
                    -PythonExe 'python.exe' `
                    -Symbols 'SPX,NDX,VIX,RUT' `
                    -TradingDate '2026-02-30' `
                    -Deadline ([DateTime]::UtcNow.AddSeconds(60)) `
                    -InvocationId 'bounded-invalid-date' | Out-Null
            }
            catch {
                $caught = $_
            }

            $caught | Should Not BeNullOrEmpty
            $script:invalidDateChildCalled | Should Be $false
        }
}

Describe '07:00 current-day universe pre-stage' {
    BeforeEach {
        Mock Get-MarketAppUniversePreparationDeadline {
            $Now.AddMinutes(4)
        }
        Mock Test-MarketAppUniverseProviderDiscoveryAllowed { $true }
        Mock Invoke-MarketAppUniverseCachePreparation {
            New-TestBackendUniversePreparation -TradingDate '2026-09-09'
        }
    }

    It 'prepares all four indexes with an exact target date and bounded 07:40 proof window' {
        $now = [datetime]'2026-09-09T07:00:00'
        $notAfter = [datetime]'2026-09-09T07:40:00'

        $result = Invoke-MarketAppCurrentDayUniversePrestage `
            -ProjectRootPath $ProjectRoot `
            -PythonPath 'python.exe' `
            -Symbols 'SPX,NDX,VIX,RUT' `
            -ExpectedContractCap 3200 `
            -Now $now `
            -NotAfter $notAfter `
            -PrestageInvocationId '0700-prestage-test'

        $result.ProvenanceLabel | Should Be 'CURRENT_DAY_CACHE'
        $result.UsesFallback | Should Be $false
        Assert-MockCalled Get-MarketAppUniversePreparationDeadline -Times 1 -Scope It -ParameterFilter {
            $Now -eq [datetime]'2026-09-09T07:00:00' -and
            $NotAfter -eq [datetime]'2026-09-09T07:40:00'
        }
        Assert-MockCalled Test-MarketAppUniverseProviderDiscoveryAllowed -Times 1 -Scope It -ParameterFilter {
            $Now -eq [datetime]'2026-09-09T07:00:00' -and
            $OpeningProtectionBoundary -eq [datetime]'2026-09-09T07:40:00'
        }
        Assert-MockCalled Invoke-MarketAppUniverseCachePreparation -Times 1 -Scope It -ParameterFilter {
            $Symbols -eq 'SPX,NDX,VIX,RUT' -and
            $TradingDate -eq '2026-09-09' -and
            $InvocationId -eq '0700-prestage-test' -and
            -not [bool]$SkipProviderDiscovery
        }
    }

    It 'rejects prior-session fallback without weakening the normal launch fallback policy' {
        Mock Invoke-MarketAppUniverseCachePreparation {
            New-TestBackendUniversePreparation -UsesFallback $true -TradingDate '2026-09-09'
        }

        {
            Invoke-MarketAppCurrentDayUniversePrestage `
                -ProjectRootPath $ProjectRoot `
                -PythonPath 'python.exe' `
                -Symbols 'SPX,NDX,VIX,RUT' `
                -ExpectedContractCap 3200 `
                -Now ([datetime]'2026-09-09T07:00:00') `
                -NotAfter ([datetime]'2026-09-09T07:40:00') `
                -PrestageInvocationId 'fallback-prestage-test'
        } | Should Throw 'failed closed'
    }

    It 'rejects incomplete exact provenance and subscription-cap proof' {
        Mock Invoke-MarketAppUniverseCachePreparation {
            $candidate = New-TestBackendUniversePreparation -TradingDate '2026-09-09'
            $candidate.Result.provenance.source_date = '2026-09-08'
            $candidate.Result.selected_contract_count = 3201
            return $candidate
        }

        {
            Invoke-MarketAppCurrentDayUniversePrestage `
                -ProjectRootPath $ProjectRoot `
                -PythonPath 'python.exe' `
                -Symbols 'SPX,NDX,VIX,RUT' `
                -ExpectedContractCap 3200 `
                -Now ([datetime]'2026-09-09T07:00:00') `
                -NotAfter ([datetime]'2026-09-09T07:40:00') `
                -PrestageInvocationId 'invalid-proof-prestage-test'
        } | Should Throw 'failed closed'
    }

    It 'abstains before provider discovery at or after the protected boundary' {
        {
            Invoke-MarketAppCurrentDayUniversePrestage `
                -ProjectRootPath $ProjectRoot `
                -PythonPath 'python.exe' `
                -Symbols 'SPX,NDX,VIX,RUT' `
                -ExpectedContractCap 3200 `
                -Now ([datetime]'2026-09-09T07:40:00') `
                -NotAfter ([datetime]'2026-09-09T07:40:00') `
                -PrestageInvocationId 'late-prestage-test'
        } | Should Throw 'protected 07:40 CT boundary'

        Assert-MockCalled Invoke-MarketAppUniverseCachePreparation -Times 0 -Scope It
    }
}

Describe 'Prepared automatic backend replacement' {
    BeforeAll {
        $preparedStopOriginalSymbols = $env:DATABENTO_SYMBOLS
    }

    BeforeEach {
        $script:PythonExe = 'python.exe'
        $script:InvocationId = 'prepared-stop-pester'
        $script:Caller = 'Pester'
        $env:DATABENTO_SYMBOLS = 'SPX,NDX,VIX,RUT'
        $script:testPreparedUniverse = New-TestBackendUniversePreparation

        Mock Test-MarketAppUniverseProviderDiscoveryAllowed { $true }
        Mock Invoke-MarketAppUniverseCachePreparation {
            $script:testPreparedUniverse
        }
        Mock Invoke-MarketAppBoundedAutomaticListenerStop {
            [pscustomobject]@{
                Stopped = $true
                Reason = 'stopped_before_protected_opening_boundary'
            }
        }
        Mock Assert-OpeningAcceptanceMutationPreflight {}
        Mock Write-WatchdogLog {}
    }

    AfterAll {
        $env:DATABENTO_SYMBOLS = $preparedStopOriginalSymbols
    }

    It 'returns the exact prevalidated preparation after one successful stop' {
        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'stale_backend_trading_date'

        $result.Stopped | Should Be $true
        [object]::ReferenceEquals(
            $script:testPreparedUniverse,
            $result.Preparation
        ) | Should Be $true
        Assert-MockCalled Invoke-MarketAppUniverseCachePreparation -Times 1 -Scope It `
            -ParameterFilter { $TradingDate -eq '2026-09-09' }
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 1 -Scope It -ParameterFilter {
            $ExpectedPid -eq 44224 -and
            $RecoveryReason -eq 'stale_backend_trading_date'
        }
    }

    It 'preserves the listener when preparation throws' {
        Mock Invoke-MarketAppUniverseCachePreparation { throw 'synthetic preparation failure' }

        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'stale_backend_trading_date'

        $result.Stopped | Should Be $false
        $result.Preparation | Should BeNullOrEmpty
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
        Assert-MockCalled Write-WatchdogLog -Times 1 -Scope It -ParameterFilter {
            $Event -eq 'backend_replacement_universe_preparation_failed' -and
            $Message -match 'action=preserve_verified_listener'
        }
    }

    It 'preserves the listener when bounded preparation cannot authorize startup' {
        $script:testPreparedUniverse = New-TestBackendUniversePreparation `
            -StartupMayContinue $false `
            -TimedOut $true

        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'stale_backend_trading_date'

        $result.Stopped | Should Be $false
        $result.Reason | Should Be 'universe_preparation_blocked'
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
        Assert-MockCalled Write-WatchdogLog -Times 1 -Scope It -ParameterFilter {
            $Event -eq 'backend_replacement_universe_preparation_blocked' -and
            $Message -match 'timed_out=True' -and
            $Message -match 'action=preserve_verified_listener'
        }
    }

    It 'preserves the listener when preparation is for the wrong trading date' {
        $script:testPreparedUniverse = New-TestBackendUniversePreparation `
            -TradingDate '2026-09-08'

        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'stale_backend_trading_date'

        $result.Stopped | Should Be $false
        $result.Reason | Should Be 'universe_preparation_trading_date_mismatch'
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
    }

    It 'preserves a same-day core listener when RUT preparation is fallback-only' {
        $script:testPreparedUniverse = New-TestBackendUniversePreparation `
            -UsesFallback $true

        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'rut_canary_preopen_upgrade' `
            -RequireCurrentDay

        $result.Stopped | Should Be $false
        $result.Reason | Should Be 'rut_upgrade_requires_current_day_universe'
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
        Assert-MockCalled Write-WatchdogLog -Times 1 -Scope It -ParameterFilter {
            $Event -eq 'rut_canary_preopen_upgrade_deferred' -and
            $Message -match 'action=preserve_verified_listener'
        }
    }

    It 'preserves the prior-day listener when the refresh proof has a stale source date' {
        $script:testPreparedUniverse.Result.provenance.source_date = '2026-09-08'

        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'verified_prior_day_backend_preopen_refresh' `
            -RequireCurrentDay

        $result.Stopped | Should Be $false
        $result.Reason | Should Be 'prior_day_refresh_requires_current_day_universe'
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
    }

    It 'preserves the prior-day listener when the refresh hash is malformed' {
        $script:testPreparedUniverse.Result.selected_universe_sha256 = 'not-a-sha256'

        $result = Invoke-BackendPreparedAutomaticListenerStop `
            -ExpectedPid 44224 `
            -DecisionTime ([datetime]'2026-09-09T07:45:00') `
            -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
            -ExpectedTradingDate '2026-09-09' `
            -ExpectedContractCap 3200 `
            -RecoveryReason 'verified_prior_day_backend_preopen_refresh' `
            -RequireCurrentDay

        $result.Stopped | Should Be $false
        $result.Reason | Should Be 'prior_day_refresh_requires_current_day_universe'
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
    }

    It 'preserves the prior-day listener when the refresh count is zero or over cap' {
        foreach ($invalidCount in @(0, 3201)) {
            $script:testPreparedUniverse = New-TestBackendUniversePreparation
            $script:testPreparedUniverse.Result.selected_contract_count = $invalidCount

            $result = Invoke-BackendPreparedAutomaticListenerStop `
                -ExpectedPid 44224 `
                -DecisionTime ([datetime]'2026-09-09T07:45:00') `
                -PreparationDeadline ([datetime]'2026-09-09T08:20:00') `
                -ExpectedTradingDate '2026-09-09' `
                -ExpectedContractCap 3200 `
                -RecoveryReason 'verified_prior_day_backend_preopen_refresh' `
                -RequireCurrentDay

            $result.Stopped | Should Be $false
            $result.Reason | Should Be 'prior_day_refresh_requires_current_day_universe'
        }
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Scope It
    }
}

Describe 'Missing trading-date pre-open recovery decision' {
    $now = [datetime]'2026-09-08T07:45:00'

    It 'restarts an exact ownership-verified listener that predates the session' {
        $decision = Resolve-MarketAppMissingTradingDateAction `
            -Now $now `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00')

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_listener_preopen'
    }

    It 'preserves an ambiguous listener set' {
        (Resolve-MarketAppMissingTradingDateAction `
            -Now $now `
            -ListenerCount 2 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00')).Action | Should Be 'preserve'
    }

    It 'preserves a listener whose ownership is not verified' {
        (Resolve-MarketAppMissingTradingDateAction `
            -Now $now `
            -ListenerCount 1 `
            -OwnershipVerified $false `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00')).Reason | Should Be 'listener_ownership_unverified'
    }

    It 'preserves a same-day listener that may still be warming' {
        (Resolve-MarketAppMissingTradingDateAction `
            -Now $now `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:40:00')).Reason | Should Be 'listener_started_current_day'
    }

    It 'allows a verified prior-day listener salvage at the exact 08:25 boundary' {
        $decision = Resolve-MarketAppMissingTradingDateAction `
            -Now ([datetime]'2026-09-08T08:25:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00')

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_listener_late_salvage'
    }

    It 'allows the same bounded salvage after a late wake' {
        $decision = Resolve-MarketAppMissingTradingDateAction `
            -Now ([datetime]'2026-09-08T11:05:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00')

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_listener_late_salvage'
    }

    It 'keeps late salvage idempotent after a verified replacement succeeded' {
        $decision = Resolve-MarketAppMissingTradingDateAction `
            -Now ([datetime]'2026-09-08T11:10:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -RecoveryAlreadySucceeded $true

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'session_recovery_already_succeeded'
    }
}

function New-TestBackendReadinessState {
    param(
        [string]$TradingDate = '2026-09-08',
        [string]$CashCloseUtc = '2026-09-08T20:00:00+00:00'
    )

    $epoch = ('a' * 64) -join ''
    $boundary = New-TestSubscriptionBoundaryState `
        -TradingDate $TradingDate `
        -HealthCashCloseUtc $CashCloseUtc
    return [pscustomobject]@{
        HealthReachable = $true
        LiveHealthReachable = $true
        HealthProvider = 'databento'
        LiveProvider = 'databento'
        StreamingActive = $true
        SubscriptionAllowed = $true
        SubscriptionSessionState = 'preopen'
        OrbSamplerAlive = $true
        OrbSamplerIntervalSeconds = 5
        TradingDate = $TradingDate
        HealthSubscriptionEpochId = $epoch
        LiveSubscriptionEpochId = $epoch
        HealthActiveGeneration = 1
        LiveActiveGeneration = 1
        HealthHandoffStatus = 'active'
        LiveHandoffStatus = 'active'
        ConfiguredSymbols = @('SPX', 'NDX', 'VIX', 'RUT')
        StreamConnected = $false
        StreamProgressing = $false
        CollectionReady = $false
        CalculationReady = $false
        PredictionPipelineOk = $false
        RequiredMissingSymbols = @()
        RequiredInvalidSymbols = @()
        RequiredEpochMismatchSymbols = @()
        RequiredGenerationMismatchSymbols = @()
        RequiredZeroFreshQuoteSymbols = @()
        RequiredStaleSymbols = @()
        HealthEvidence = $boundary.HealthEvidence
        LiveEvidence = $boundary.LiveEvidence
    }
}

Describe 'Verified backend cash-close boundary' {
    It 'accepts an exact same-day health/live early close and converts it to CT' {
        $state = New-TestSubscriptionBoundaryState `
            -TradingDate '2026-11-27' `
            -HealthCashCloseUtc '2026-11-27T18:00:00+00:00'

        $resolution = Resolve-MarketAppVerifiedCashClose `
            -RuntimeState $state `
            -Now ([datetime]'2026-11-27T12:05:00')

        $resolution.Verified | Should Be $true
        $resolution.CashCloseCt.ToString('yyyy-MM-ddTHH:mm:ss') |
            Should Be '2026-11-27T12:00:00'
    }

    It 'rejects wrong-day or divergent endpoint boundary evidence' {
        $wrongDay = New-TestSubscriptionBoundaryState `
            -TradingDate '2026-11-26' `
            -HealthCashCloseUtc '2026-11-27T18:00:00+00:00'
        $wrongDayResult = Resolve-MarketAppVerifiedCashClose `
            -RuntimeState $wrongDay `
            -Now ([datetime]'2026-11-27T11:00:00')
        $wrongDayResult.Verified | Should Be $false
        $wrongDayResult.Reason | Should Be 'subscription_window_trading_date_mismatch'

        $divergent = New-TestSubscriptionBoundaryState `
            -TradingDate '2026-11-27' `
            -HealthCashCloseUtc '2026-11-27T18:00:00+00:00' `
            -LiveCashCloseUtc '2026-11-27T21:00:00+00:00'
        $divergentResult = Resolve-MarketAppVerifiedCashClose `
            -RuntimeState $divergent `
            -Now ([datetime]'2026-11-27T11:00:00')
        $divergentResult.Verified | Should Be $false
        $divergentResult.Reason | Should Be 'cash_close_utc_mismatch'
    }
}

Describe 'Bounded backend readiness recovery decision' {
    It 'preserves a healthy pre-open backend' {
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:20:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState (New-TestBackendReadinessState)

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'healthy'
        @($decision.FailureReasons).Count | Should Be 0
    }

    It 'refreshes an exact owned prior-day backend throughout the bounded pre-open window' {
        foreach ($decisionTime in @(
            [datetime]'2026-09-08T07:45:00',
            [datetime]'2026-09-08T08:24:59'
        )) {
            $decision = Resolve-MarketAppBackendReadinessAction `
                -Now $decisionTime `
                -ListenerCount 1 `
                -OwnershipVerified $true `
                -ListenerStartTime ([datetime]'2026-09-07T23:55:00') `
                -RuntimeState (New-TestBackendReadinessState)

            $decision.Action | Should Be 'restart'
            $decision.Reason | Should Be 'verified_prior_day_backend_preopen_refresh'
            (@($decision.FailureReasons) -contains 'prior_day_backend_requires_current_code') |
                Should Be $true
        }
    }

    It 'does not refresh a prior-day backend before the bounded subscription start' {
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T07:44:59') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-07T23:55:00') `
            -RuntimeState (New-TestBackendReadinessState)

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'healthy'
    }

    It 'preserves a current-day listener during its bounded start grace' {
        $state = New-TestBackendReadinessState
        $state.OrbSamplerAlive = $false
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T07:46:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'current_day_start_grace'
        (@($decision.FailureReasons) -contains 'orb_sampler_not_alive') | Should Be $true
    }

    It 'restarts one ownership-verified aged listener with a dead sampler and live endpoint' {
        $state = New-TestBackendReadinessState
        $state.OrbSamplerAlive = $false
        $state.LiveHealthReachable = $false
        $state.LiveProvider = $null
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_aged_backend_readiness_failure'
        (@($decision.FailureReasons) -contains 'orb_sampler_not_alive') | Should Be $true
        (@($decision.FailureReasons) -contains 'live_health_unreachable') | Should Be $true
    }

    It 'preserves an unhealthy listener when ownership is ambiguous' {
        $state = New-TestBackendReadinessState
        $state.OrbSamplerAlive = $false
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 2 `
            -OwnershipVerified $false `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'listener_count_not_exactly_one'
    }

    It 'never restarts at or after the protected 08:25 boundary' {
        $state = New-TestBackendReadinessState
        $state.OrbSamplerAlive = $false
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:25:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'protected_opening_boundary_reached'
    }

    It 'salvages a prior-day backend with an obsolete session contract at 08:25 and after a late wake' {
        foreach ($decisionTime in @(
            [datetime]'2026-09-08T08:25:00',
            [datetime]'2026-09-08T11:05:00'
        )) {
            $state = New-TestBackendReadinessState
            $state.HealthSubscriptionEpochId = $null
            $state.LiveSubscriptionEpochId = $null
            $decision = Resolve-MarketAppBackendReadinessAction `
                -Now $decisionTime `
                -ListenerCount 1 `
                -OwnershipVerified $true `
                -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
                -RuntimeState $state

            $decision.Action | Should Be 'restart'
            $decision.Reason | Should Be 'verified_prior_day_backend_late_salvage'
            (@($decision.FailureReasons) -contains 'health_subscription_epoch_invalid') | Should Be $true
        }
    }

    It 'does not call an unproven prior-day backend healthy at the 08:25 boundary' {
        $state = New-TestBackendReadinessState
        $state.RequiredMissingSymbols = @('SPX', 'NDX')
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:25:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-07T23:55:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_backend_late_salvage'
        (@($decision.FailureReasons) -contains 'current_session_contract_unproven') | Should Be $true
    }

    It 'preserves a prior-day backend that proves the full current-session contract at 08:25' {
        $state = New-TestBackendReadinessState
        $state.StreamConnected = $true
        $state.StreamProgressing = $true
        $state.CollectionReady = $true
        $state.CalculationReady = $true
        $state.PredictionPipelineOk = $true
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:25:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-07T23:55:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'current_session_contract_progressing'
        @($decision.FailureReasons).Count | Should Be 0
    }

    It 'preserves a pre-midnight backend that proves current-session current-contract progress' {
        $state = New-TestBackendReadinessState
        $state.SubscriptionSessionState = 'regular_session'
        $state.StreamConnected = $true
        $state.StreamProgressing = $true
        $state.CollectionReady = $true
        $state.CalculationReady = $true
        $state.PredictionPipelineOk = $true
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T09:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-07T23:55:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'current_session_contract_progressing'
        @($decision.FailureReasons).Count | Should Be 0
    }

    It 'preserves a prior-day backend carrying the current session final state after cash close' {
        $state = New-TestBackendReadinessState
        $state.SubscriptionAllowed = $false
        $state.SubscriptionSessionState = 'post_close'
        $state.HealthHandoffStatus = 'off_hours'
        $state.LiveHandoffStatus = 'off_hours'
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T15:05:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-07T23:55:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'current_session_post_close_preserved'
        (@($decision.FailureReasons) -contains 'health_handoff_not_active') | Should Be $true
        (@($decision.FailureReasons) -contains 'live_handoff_not_active') | Should Be $true
    }

    It 'preserves current-session final state immediately after a verified early close' {
        $state = New-TestBackendReadinessState `
            -TradingDate '2026-11-27' `
            -CashCloseUtc '2026-11-27T18:00:00+00:00'
        $state.SubscriptionAllowed = $false
        $state.SubscriptionSessionState = 'post_close'
        $state.HealthHandoffStatus = 'off_hours'
        $state.LiveHandoffStatus = 'off_hours'

        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-11-27T12:05:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-11-26T23:55:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'current_session_post_close_preserved'
    }

    It 'abstains from post-noon recovery when cash-close evidence is unverified' {
        $state = New-TestBackendReadinessState `
            -TradingDate '2026-11-27' `
            -CashCloseUtc '2026-11-27T18:00:00+00:00'
        $state.LiveEvidence.subscription_window.cash_close_utc = '2026-11-27T21:00:00+00:00'
        $state.HealthSubscriptionEpochId = $null

        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-11-27T12:05:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-11-26T23:55:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'cash_close_boundary_unverified'
        (@($decision.FailureReasons) -contains 'subscription_cash_close_unverified') |
            Should Be $true
    }

    It 'does not apply post-close preservation to a stale trading date' {
        $state = New-TestBackendReadinessState
        $state.TradingDate = '2026-09-04'
        $state.SubscriptionAllowed = $false
        $state.SubscriptionSessionState = 'post_close'
        $state.HealthHandoffStatus = 'off_hours'
        $state.LiveHandoffStatus = 'off_hours'
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T15:05:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_backend_late_salvage'
    }

    It 'keeps prior-day late salvage idempotent after a replacement succeeds' {
        $state = New-TestBackendReadinessState
        $state.HealthSubscriptionEpochId = $null
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T11:10:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -RuntimeState $state `
            -RecoveryAlreadyAttempted $true

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'session_recovery_already_succeeded'
    }

    It 'suppresses a repeated readiness restart in the same session' {
        $state = New-TestBackendReadinessState
        $state.OrbSamplerAlive = $false
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState $state `
            -RecoveryAlreadyAttempted $true

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'session_recovery_already_attempted'
    }

    It 'reports regular-session live progress and fresh-core gate failures without restarting' {
        $state = New-TestBackendReadinessState
        $state.SubscriptionSessionState = 'regular_session'
        $state.RequiredInvalidSymbols = @('SPX')
        $state.RequiredZeroFreshQuoteSymbols = @('NDX')
        $decision = Resolve-MarketAppBackendReadinessAction `
            -Now ([datetime]'2026-09-08T08:31:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -RuntimeState $state

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'protected_opening_boundary_reached'
        (@($decision.FailureReasons) -contains 'live_stream_not_progressing') | Should Be $true
        (@($decision.FailureReasons) -contains 'required_symbols_invalid') | Should Be $true
        (@($decision.FailureReasons) -contains 'required_zero_fresh_quotes') | Should Be $true
    }
}

Describe 'Bounded dashboard readiness recovery decision' {
    It 'preserves a responding dashboard only when its sole listener is owned' {
        $decision = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -EndpointReady $true

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'healthy'

        $unowned = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $false `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -EndpointReady $true
        $unowned.Action | Should Be 'preserve'
        $unowned.Reason | Should Be 'listener_ownership_unverified'
    }

    It 'restarts one verified aged dashboard whose exact health contract fails' {
        $decision = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -EndpointReady $false

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_aged_dashboard_readiness_failure'
        (@($decision.FailureReasons) -contains 'dashboard_health_unreachable_or_invalid') | Should Be $true
    }

    It 'restarts a healthy verified prior-day dashboard before the opening boundary' {
        $decision = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T07:45:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $true

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_dashboard_preopen'
        (@($decision.FailureReasons) -contains 'dashboard_started_prior_day') | Should Be $true
    }

    It 'does not repeatedly restart a prior-day dashboard after a session attempt' {
        $decision = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $true `
            -RecoveryAlreadyAttempted $true

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'session_recovery_already_succeeded'
        (@($decision.FailureReasons) -contains 'dashboard_started_prior_day') | Should Be $true
    }

    It 'keeps start grace and the same-day recovery latch fail-closed' {
        $warming = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T07:46:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -EndpointReady $false
        $warming.Reason | Should Be 'current_day_start_grace'

        $latched = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:00:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -EndpointReady $false `
            -RecoveryAlreadyAttempted $true
        $latched.Action | Should Be 'preserve'
        $latched.Reason | Should Be 'session_recovery_already_attempted'
    }

    It 'never restarts the dashboard at or after the protected 08:25 boundary' {
        $decision = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:25:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-08T07:45:00') `
            -EndpointReady $false

        $decision.Action | Should Be 'preserve'
        $decision.Reason | Should Be 'protected_opening_boundary_reached'
    }

    It 'salvages a prior-day dashboard at the exact opening boundary even when its old endpoint responds' {
        $decision = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:25:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $true

        $decision.Action | Should Be 'restart'
        $decision.Reason | Should Be 'verified_prior_day_dashboard_late_salvage'
        (@($decision.FailureReasons) -contains 'dashboard_started_prior_day') | Should Be $true
    }

    It 'salvages a prior-day dashboard after a late wake and latches only a prior success' {
        $retry = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T11:05:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $false `
            -RecoveryAlreadyAttempted $false
        $retry.Action | Should Be 'restart'
        $retry.Reason | Should Be 'verified_prior_day_dashboard_late_salvage'

        $latched = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T11:10:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $false `
            -RecoveryAlreadyAttempted $true
        $latched.Action | Should Be 'preserve'
        $latched.Reason | Should Be 'session_recovery_already_succeeded'
    }
}

Describe 'Session recovery journal success latch' {
    It 'keeps a requested dashboard recovery retryable until replacement success is recorded' {
        $root = Join-Path $TestDrive 'dashboard-retry-journal'
        $runtimeDir = Join-Path $root 'logs\runtime'
        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
        $journal = Join-Path $runtimeDir 'watchdog.log'
        Set-Content -LiteralPath $journal -Value '2026-09-08 08:25:00 -05:00 event=dashboard_session_recovery_requested trading_date=2026-09-08 action=request_exact_owner_stop'

        $state = Get-MarketAppSessionRecoveryJournalState `
            -ProjectRoot $root `
            -Component 'dashboard' `
            -TradingDate '2026-09-08'
        $state.Requested | Should Be $true
        $state.Succeeded | Should Be $false

        $retry = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:30:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $false `
            -RecoveryAlreadyAttempted $state.Succeeded
        $retry.Action | Should Be 'restart'
        $retry.Reason | Should Be 'verified_prior_day_dashboard_late_salvage'
    }

    It 'becomes idempotent only after exact replacement success is recorded' {
        $root = Join-Path $TestDrive 'dashboard-success-journal'
        $runtimeDir = Join-Path $root 'logs\runtime'
        New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
        $journal = Join-Path $runtimeDir 'watchdog.log'
        Set-Content -LiteralPath $journal -Value @(
            '2026-09-08 08:25:00 -05:00 event=dashboard_session_recovery_requested trading_date=2026-09-08 action=request_exact_owner_stop',
            '2026-09-08 08:25:05 -05:00 event=dashboard_session_recovery_succeeded trading_date=2026-09-08 replacement_listener_pid=5252 action=latched'
        )

        $state = Get-MarketAppSessionRecoveryJournalState `
            -ProjectRoot $root `
            -Component 'dashboard' `
            -TradingDate '2026-09-08'
        $state.Requested | Should Be $true
        $state.Succeeded | Should Be $true

        $latched = Resolve-MarketAppDashboardReadinessAction `
            -Now ([datetime]'2026-09-08T08:30:00') `
            -ListenerCount 1 `
            -OwnershipVerified $true `
            -ListenerStartTime ([datetime]'2026-09-04T17:09:00') `
            -EndpointReady $false `
            -RecoveryAlreadyAttempted $state.Succeeded
        $latched.Action | Should Be 'preserve'
        $latched.Reason | Should Be 'session_recovery_already_succeeded'
    }
}

Describe 'AutoStart trigger self-heal exact task contract' {
    BeforeEach {
        $script:expectedLogonSid = 'S-1-5-21-1000-1000-1000-1001'
        $script:expectedPowerShell = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
    }

    It 'accepts the full four-weekly-trigger contract and classifies only exact legacy or missing-boot shapes as repairable' {
        $valid = Test-MarketStartupTaskContract `
            -Task (New-TestMarketStartupTask) `
            -ExpectedTaskName $StartupTaskName `
            -ExpectedTaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid `
            -ExpectedPowerShellExe $script:expectedPowerShell
        $missing = Test-MarketStartupTaskContract `
            -Task (New-TestMarketStartupTask -BootCount 0) `
            -ExpectedTaskName $StartupTaskName `
            -ExpectedTaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid `
            -ExpectedPowerShellExe $script:expectedPowerShell
        $legacy = Test-MarketStartupTaskContract `
            -Task (New-TestMarketStartupTask -WeeklyTimes @('07:00','07:45','08:15')) `
            -ExpectedTaskName $StartupTaskName `
            -ExpectedTaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid `
            -ExpectedPowerShellExe $script:expectedPowerShell

        $valid.Valid | Should Be $true
        $valid.RepairableMissingBootTrigger | Should Be $false
        $valid.RepairableMissingUniversePrestageRetry | Should Be $false
        $missing.Valid | Should Be $false
        $missing.RepairableMissingBootTrigger | Should Be $true
        $missing.RepairableMissingUniversePrestageRetry | Should Be $false
        (@($missing.Reasons) -join ',') | Should Be 'boot_trigger_missing'
        $legacy.Valid | Should Be $false
        $legacy.RepairableMissingBootTrigger | Should Be $false
        $legacy.RepairableMissingUniversePrestageRetry | Should Be $true
        (@($legacy.Reasons) -join ',') | Should Be 'trigger_weekly_count_mismatch,trigger_weekly_shape_mismatch'
    }

    It 'rejects duplicate, disabled, delayed, or otherwise tampered missing-boot tasks' {
        $duplicate = New-TestMarketStartupTask -BootCount 2
        $disabled = New-TestMarketStartupTask -BootEnabled $false
        $delayed = New-TestMarketStartupTask -BootDelay 'PT5M'
        $tampered = New-TestMarketStartupTask -BootCount 0 -WakeToRun $false
        foreach ($task in @($duplicate,$disabled,$delayed,$tampered)) {
            $result = Test-MarketStartupTaskContract `
                -Task $task `
                -ExpectedTaskName $StartupTaskName `
                -ExpectedTaskPath $StartupTaskPath `
                -ExpectedProjectRoot $ProjectRoot `
                -ExpectedLogonRecoverySid $script:expectedLogonSid `
                -ExpectedPowerShellExe $script:expectedPowerShell
            $result.Valid | Should Be $false
            $result.RepairableMissingBootTrigger | Should Be $false
        }
    }

    It 'adds only one boot trigger and verifies the complete task contract afterward' {
        $missing = New-TestMarketStartupTask -BootCount 0
        $repaired = New-TestMarketStartupTask
        $realExistingTriggers = @(
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:00'),
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:15'),
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:45'),
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '08:15'),
            (New-ScheduledTaskTrigger -AtLogOn -User $script:expectedLogonSid)
        )
        $script:testBootTrigger = New-ScheduledTaskTrigger -AtStartup
        $missing.Triggers = $realExistingTriggers
        $repaired.Triggers = @($script:testBootTrigger) + $realExistingTriggers
        $script:startupGetCount = 0
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask {
            $script:startupGetCount += 1
            if ($script:startupGetCount -eq 1) { return $missing }
            return $repaired
        }
        Mock New-ScheduledTaskTrigger {
            $script:testBootTrigger
        }
        Mock Set-ScheduledTask {}

        $result = Repair-MarketStartupBootRecovery `
            -TaskName $StartupTaskName `
            -TaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid

        $result.Status | Should Be 'startup_boot_recovery_repaired'
        $result.Error | Should Be $null
        Assert-MockCalled New-ScheduledTaskTrigger -Times 1 -Scope It -ParameterFilter { $AtStartup }
        Assert-MockCalled Set-ScheduledTask -Times 1 -Scope It -ParameterFilter {
            $TaskName -eq 'MarketPinPredictor_AutoStart' -and
            $TaskPath -eq '\' -and
            @($Trigger).Count -eq 6
        }
    }

    It 'refuses wrong or malformed three-weekly-trigger schedules without mutation' {
        $wrong = New-TestMarketStartupTask -WeeklyTimes @('07:00','07:30','08:15')
        $malformed = New-TestMarketStartupTask -WeeklyTimes @('07:00','07:45','08:15')
        @($malformed.Triggers | Where-Object {
            $_.CimClass.CimClassName -eq 'MSFT_TaskWeeklyTrigger'
        })[0].RandomDelay = 'PT1M'
        $script:taskUnderTest = $wrong
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask { $script:taskUnderTest }
        Mock New-ScheduledTaskTrigger { New-TestMarketTrigger -ClassName 'MSFT_TaskWeeklyTrigger' }
        Mock Set-ScheduledTask {}

        foreach ($task in @($wrong,$malformed)) {
            $script:taskUnderTest = $task
            $contract = Test-MarketStartupTaskContract `
                -Task $task `
                -ExpectedTaskName $StartupTaskName `
                -ExpectedTaskPath $StartupTaskPath `
                -ExpectedProjectRoot $ProjectRoot `
                -ExpectedLogonRecoverySid $script:expectedLogonSid `
                -ExpectedPowerShellExe $script:expectedPowerShell
            $result = Repair-MarketStartupBootRecovery `
                -ExpectedProjectRoot $ProjectRoot `
                -ExpectedLogonRecoverySid $script:expectedLogonSid

            $contract.RepairableMissingUniversePrestageRetry | Should Be $false
            $result.Status | Should Be 'startup_boot_recovery_pending'
            $result.Error | Should Match 'trigger_weekly_shape_mismatch'
        }
        Assert-MockCalled Set-ScheduledTask -Times 0 -Scope It
    }

    It 'is idempotent when ready and never mutates a task with another mismatch' {
        $ready = New-TestMarketStartupTask
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask { $ready }
        Mock New-ScheduledTaskTrigger { New-TestMarketTrigger -ClassName 'MSFT_TaskBootTrigger' }
        Mock Set-ScheduledTask {}
        $result = Repair-MarketStartupBootRecovery `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid
        $result.Status | Should Be 'startup_boot_recovery_ready'
        Assert-MockCalled Set-ScheduledTask -Times 0 -Scope It

        $ready.Settings.WakeToRun = $false
        $result = Repair-MarketStartupBootRecovery `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid
        $result.Status | Should Be 'startup_boot_recovery_pending'
        $result.Error | Should Match 'settings_wake_to_run_disabled'
        Assert-MockCalled Set-ScheduledTask -Times 0 -Scope It
    }
}

Describe 'AutoStart exact legacy universe pre-stage retry repair' {
    It 'adds only the exact 07:15 weekly trigger to the legacy schedule and verifies the full contract' {
        $script:expectedLogonSid = 'S-1-5-21-1000-1000-1000-1001'
        $legacy = New-TestMarketStartupTask -WeeklyTimes @('07:00','07:45','08:15')
        $repaired = New-TestMarketStartupTask
        $realExistingTriggers = @(
            (New-ScheduledTaskTrigger -AtStartup),
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:00'),
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:45'),
            (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '08:15'),
            (New-ScheduledTaskTrigger -AtLogOn -User $script:expectedLogonSid)
        )
        $script:testRetryTrigger = New-ScheduledTaskTrigger `
            -Weekly `
            -WeeksInterval 1 `
            -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
            -At '07:15'
        @($realExistingTriggers | Where-Object {
            $_.CimClass.CimClassName -eq 'MSFT_TaskBootTrigger'
        }).Count | Should Be 1
        @($realExistingTriggers | Where-Object {
            $_.CimClass.CimClassName -eq 'MSFT_TaskWeeklyTrigger'
        }).Count | Should Be 3
        @($realExistingTriggers | Where-Object {
            $_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger'
        }).Count | Should Be 1
        $legacy.Triggers = $realExistingTriggers
        $repaired.Triggers = @($realExistingTriggers) + $script:testRetryTrigger
        $script:startupGetCount = 0
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask {
            $script:startupGetCount += 1
            if ($script:startupGetCount -eq 1) { return $legacy }
            return $repaired
        }
        Mock New-ScheduledTaskTrigger { $script:testRetryTrigger }
        Mock Set-ScheduledTask {}

        $result = Repair-MarketStartupBootRecovery `
            -TaskName $StartupTaskName `
            -TaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedLogonRecoverySid $script:expectedLogonSid

        $result.Error | Should Be $null
        $result.Status | Should Be 'startup_universe_prestage_retry_repaired'
        Assert-MockCalled New-ScheduledTaskTrigger -Times 1 -Scope It -ParameterFilter {
            $Weekly -and $WeeksInterval -eq 1 -and $At -eq '07:15'
        }
        Assert-MockCalled Set-ScheduledTask -Times 1 -Scope It -ParameterFilter {
            $TaskName -eq 'MarketPinPredictor_AutoStart' -and
            $TaskPath -eq '\' -and
            @($Trigger).Count -eq 6 -and
            @($Trigger | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskBootTrigger' }).Count -eq 1 -and
            @($Trigger | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger' }).Count -eq 1
        }
    }
}

Describe 'Watchdog self-heal exact task contract' {
    It 'accepts the complete canonical watchdog task contract' {
        $task = New-TestMarketWatchdogTask
        $result = Test-MarketWatchdogTaskContract `
            -Task $task `
            -ExpectedTaskName $WatchdogTaskName `
            -ExpectedTaskPath $WatchdogTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedPrincipalSid 'S-1-5-18' `
            -ExpectedPowerShellExe (Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe')

        $result.Valid | Should Be $true
        $result.RepairableLimitedPrincipal | Should Be $false
        @($result.Reasons).Count | Should Be 0
    }

    It 'rejects tampered identity, path, logon, executable, workdir, tokens, and action count' {
        $otherSid = 'S-1-5-19'
        $cases = @(
            @{Expected='task_name_mismatch'; Task=(New-TestMarketWatchdogTask -TaskName 'MarketPinPredictor_Watchdog_Copy')},
            @{Expected='task_path_mismatch'; Task=(New-TestMarketWatchdogTask -TaskPath '\Tampered\')},
            @{Expected='principal_user_sid_mismatch'; Task=(New-TestMarketWatchdogTask -UserId $otherSid)},
            @{Expected='principal_logon_type_mismatch'; Task=(New-TestMarketWatchdogTask -LogonType 'Password')},
            @{Expected='principal_logon_type_mismatch'; Task=(New-TestMarketWatchdogTask -LogonType 'Interactive')},
            @{Expected='principal_logon_type_mismatch'; Task=(New-TestMarketWatchdogTask -LogonType 'S4U')},
            @{Expected='principal_run_level_mismatch'; Task=(New-TestMarketWatchdogTask -RunLevel 'Limited')},
            @{Expected='action_executable_mismatch'; Task=(New-TestMarketWatchdogTask -Execute 'C:\Windows\System32\cmd.exe')},
            @{Expected='action_working_directory_mismatch'; Task=(New-TestMarketWatchdogTask -WorkingDirectory 'C:\Windows')},
            @{Expected='action_arguments_mismatch'; Task=(New-TestMarketWatchdogTask -Arguments "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$(Join-Path $ProjectRoot 'start_market_day.ps1')`" -EnableRutCanary -SkipClockSync -Extra")},
            @{Expected='action_count_mismatch'; Task=(New-TestMarketWatchdogTask -ActionCount 2)}
        )

        foreach ($case in $cases) {
            $result = Test-MarketWatchdogTaskContract `
                -Task $case.Task `
                -ExpectedTaskName $WatchdogTaskName `
                -ExpectedTaskPath $WatchdogTaskPath `
                -ExpectedProjectRoot $ProjectRoot `
                -ExpectedPrincipalSid 'S-1-5-18' `
                -ExpectedPowerShellExe (Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe')

            $result.Valid | Should Be $false
            (@($result.Reasons) -contains $case.Expected) | Should Be $true
        }
    }

    It 'rejects drift in every watchdog recurrence and recovery setting family' {
        $disabled = New-TestMarketWatchdogTask
        $disabled.Settings.Enabled = $false
        $parallel = New-TestMarketWatchdogTask
        $parallel.Settings.MultipleInstances = 'Parallel'
        $wrongLimit = New-TestMarketWatchdogTask
        $wrongLimit.Settings.ExecutionTimeLimit = 'PT1H'
        $batteryBound = New-TestMarketWatchdogTask
        $batteryBound.Settings.DisallowStartIfOnBatteries = $true
        $notWakeable = New-TestMarketWatchdogTask
        $notWakeable.Settings.WakeToRun = $false
        $replaysMissedRuns = New-TestMarketWatchdogTask
        $replaysMissedRuns.Settings.StartWhenAvailable = $true
        $retries = New-TestMarketWatchdogTask
        $retries.Settings.RestartCount = 3
        $retries.Settings.RestartInterval = 'PT1M'
        $missingTrigger = New-TestMarketWatchdogTask
        $missingTrigger.Triggers = @()
        $wrongStart = New-TestMarketWatchdogTask
        $wrongStart.Triggers[0].StartBoundary = '2026-09-05T07:55:00-05:00'
        $wrongDays = New-TestMarketWatchdogTask
        $wrongDays.Triggers[0].DaysOfWeek = 65
        $wrongRepetition = New-TestMarketWatchdogTask
        $wrongRepetition.Triggers[0].Repetition.Interval = 'PT10M'
        $wrongDuration = New-TestMarketWatchdogTask
        $wrongDuration.Triggers[0].Repetition.Duration = 'PT1H'
        $doesNotStop = New-TestMarketWatchdogTask
        $doesNotStop.Triggers[0].Repetition.StopAtDurationEnd = $false

        $cases = @(
            @{Expected='settings_disabled'; Task=$disabled},
            @{Expected='settings_multiple_instances_mismatch'; Task=$parallel},
            @{Expected='settings_execution_limit_mismatch'; Task=$wrongLimit},
            @{Expected='settings_battery_policy_mismatch'; Task=$batteryBound},
            @{Expected='settings_wake_to_run_disabled'; Task=$notWakeable},
            @{Expected='settings_start_when_available_enabled'; Task=$replaysMissedRuns},
            @{Expected='settings_restart_policy_mismatch'; Task=$retries},
            @{Expected='trigger_count_or_type_mismatch'; Task=$missingTrigger},
            @{Expected='trigger_weekly_shape_mismatch'; Task=$wrongStart},
            @{Expected='trigger_weekly_shape_mismatch'; Task=$wrongDays},
            @{Expected='trigger_weekly_shape_mismatch'; Task=$wrongRepetition},
            @{Expected='trigger_weekly_shape_mismatch'; Task=$wrongDuration},
            @{Expected='trigger_weekly_shape_mismatch'; Task=$doesNotStop}
        )
        foreach ($case in $cases) {
            $result = Test-MarketWatchdogTaskContract `
                -Task $case.Task `
                -ExpectedTaskName $WatchdogTaskName `
                -ExpectedTaskPath $WatchdogTaskPath `
                -ExpectedProjectRoot $ProjectRoot `
                -ExpectedPrincipalSid 'S-1-5-18' `
                -ExpectedPowerShellExe (Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe')

            $result.Valid | Should Be $false
            $result.RepairableLimitedPrincipal | Should Be $false
            (@($result.Reasons) -contains $case.Expected) | Should Be $true
        }
    }

    It 'reports a disabled triggerless parallel watchdog pending without mutation' {
        $tampered = New-TestMarketWatchdogTask
        $tampered.Settings.Enabled = $false
        $tampered.Settings.MultipleInstances = 'Parallel'
        $tampered.Triggers = @()
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask { $tampered }
        Mock Set-ScheduledTask {}
        Mock New-ScheduledTaskPrincipal { [pscustomobject]@{} }

        $result = Repair-MarketWatchdogRecoveryAuthority

        $result.Status | Should Be 'watchdog_authority_pending'
        $result.Error | Should Match 'settings_disabled'
        $result.Error | Should Match 'settings_multiple_instances_mismatch'
        $result.Error | Should Match 'trigger_count_or_type_mismatch'
        Assert-MockCalled Set-ScheduledTask -Times 0
        Assert-MockCalled New-ScheduledTaskPrincipal -Times 0
    }

    It 'does not mutate a tampered task even when the launcher is elevated' {
        $tampered = New-TestMarketWatchdogTask -Arguments "-NoProfile -File `"$(Join-Path $ProjectRoot 'start_market_day.ps1')`" -EnableRutCanary -SkipClockSync"
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask { $tampered }
        Mock Set-ScheduledTask {}
        Mock New-ScheduledTaskPrincipal { [pscustomobject]@{} }

        $result = Repair-MarketWatchdogRecoveryAuthority

        $result.Status | Should Be 'watchdog_authority_pending'
        $result.Error | Should Match '^task_contract_mismatch:action_arguments_mismatch$'
        Assert-MockCalled Set-ScheduledTask -Times 0
        Assert-MockCalled New-ScheduledTaskPrincipal -Times 0
    }

    It 'is idempotent for an already aligned exact task' {
        $aligned = New-TestMarketWatchdogTask -RunLevel 'Highest'
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask { $aligned }
        Mock Set-ScheduledTask {}
        Mock New-ScheduledTaskPrincipal { [pscustomobject]@{} }

        $result = Repair-MarketWatchdogRecoveryAuthority

        $result.Status | Should Be 'watchdog_authority_ready'
        Assert-MockCalled Set-ScheduledTask -Times 0
        Assert-MockCalled New-ScheduledTaskPrincipal -Times 0
    }

    It 'repairs only a Limited exact LocalSystem watchdog principal to Highest' {
        $limited = New-TestMarketWatchdogTask -RunLevel 'Limited'
        $highest = New-TestMarketWatchdogTask -RunLevel 'Highest'
        $limitedContract = Test-MarketWatchdogTaskContract `
            -Task $limited `
            -ExpectedTaskName $WatchdogTaskName `
            -ExpectedTaskPath $WatchdogTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedPrincipalSid 'S-1-5-18' `
            -ExpectedPowerShellExe (Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe')
        $limitedContract.Valid | Should Be $false
        $limitedContract.RepairableLimitedPrincipal | Should Be $true
        $script:watchdogGetCount = 0
        Mock Test-MarketLauncherIsElevated { $true }
        Mock Get-ScheduledTask {
            $script:watchdogGetCount += 1
            if ($script:watchdogGetCount -eq 1) { return $limited }
            return $highest
        }
        Mock Set-ScheduledTask {}
        Mock New-ScheduledTaskPrincipal {
            New-CimInstance `
                -ClassName MSFT_TaskPrincipal `
                -Namespace 'root/Microsoft/Windows/TaskScheduler' `
                -ClientOnly `
                -Property @{UserId='S-1-5-18';LogonType=5;RunLevel=1}
        }

        $result = Repair-MarketWatchdogRecoveryAuthority

        $result.Error | Should Be $null
        $result.Status | Should Be 'watchdog_authority_repaired'
        Assert-MockCalled Set-ScheduledTask -Times 1
    }
}

Describe 'Launcher integration is component-scoped and ownership-aware' {
    BeforeAll {
        $ensureScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'ensure_market_app.ps1') -Raw
        $startScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_databento_app.ps1') -Raw
        $startClosingScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_closing_tape.ps1') -Raw
        $registerScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'register_market_watchdog.ps1') -Raw
        $supervisorModule = Get-Content -LiteralPath (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Raw
    }

    It 'requires an expected PID for each explicit component restart' {
        $ensureScript | Should Match '-RestartBackend requires -ExpectedBackendPid'
        $ensureScript | Should Match '-RestartDashboard requires -ExpectedDashboardPid'
        $ensureScript | Should Match '-Port 8000\s+`\s*\r?\n\s*-ExpectedPid \$ExpectedBackendPid'
        $ensureScript | Should Match '-Port 8501\s+`\s*\r?\n\s*-ExpectedPid \$ExpectedDashboardPid'
    }

    It 'uses the shared mutex in both mutating launch paths' {
        $ensureScript | Should Match 'Enter-MarketAppSupervisorLock'
        $startScript | Should Match 'Enter-MarketAppSupervisorLock'
    }

    It 'captures launch handles and verifies listener ancestry plus exact endpoint contracts before success' {
        $ensureScript | Should Match 'Start-Process[\s\S]+-PassThru'
        $ensureScript | Should Match 'Wait-MarketAppOwnedEndpoint'
        $ensureScript | Should Match "ExpectedEndpointContract 'BackendHealth'"
        $ensureScript | Should Match "ExpectedEndpointContract 'StreamlitHealth'"
        $startScript | Should Match 'Start-Process[\s\S]+-PassThru'
        $startScript | Should Match 'Wait-MarketAppOwnedEndpoint'
        $startScript | Should Match "ExpectedEndpointContract 'BackendHealth'"
        $startScript | Should Match "ExpectedEndpointContract 'StreamlitHealth'"
        $supervisorModule | Should Match '\[int\]\$Response\.StatusCode -ne 200'
        $supervisorModule | Should Match '\$uri\.AbsolutePath -ceq \$expectedPath'
        $supervisorModule | Should Match "Content\.Trim\(\)\.ToLowerInvariant\(\) -ceq 'ok'"
        $supervisorModule | Should Not Match 'StatusCode -ge 200[\s\S]+StatusCode -lt 500'
    }

    It 'uses shared retained log preparation in both launchers' {
        $ensureScript | Should Match 'New-MarketAppLaunchLogPaths[\s\S]+-Component ''backend'''
        $startScript | Should Match 'New-MarketAppLaunchLogPaths[\s\S]+-Component ''backend'''
        $ensureScript | Should Match 'RetainedStandardOutputPath'
        $startScript | Should Match 'RetainedStandardOutputPath'
        $ensureScript | Should Match '-RedirectStandardOutput \$StandardOutputPath'
        $startScript | Should Match '-RedirectStandardOutput \$backendLogPaths\.StandardOutputPath'
    }

    It 'uses the same bounded universe-fallback refresh interval in both launchers' {
        $ensureScript | Should Match "DATABENTO_UNIVERSE_FALLBACK_REFRESH_SECONDS = '28800'"
        $startScript | Should Match 'DATABENTO_UNIVERSE_FALLBACK_REFRESH_SECONDS = "28800"'
    }

    It 'keeps ETF canaries out of the launch-critical opening subscription' {
        $ensureScript | Should Match 'DATABENTO_SYMBOLS = if \(\$EnableRutCanary\) \{ ''SPX,NDX,VIX,RUT'' \} else \{ ''SPX,NDX,VIX'' \}'
        $startScript | Should Match 'DATABENTO_SYMBOLS = if \(\$EnableRutCanary\) \{ "SPX,NDX,VIX,RUT" \} else \{ "SPX,NDX,VIX" \}'
    }

    It 'bounds the RUT canary and trims surplus non-primary pairs without changing the primary bound' {
        $ensureScript | Should Match 'DATABENTO_MAX_SUBSCRIPTION_CONTRACTS = if \(\$EnableRutCanary\) \{ ''3200'' \} else \{ ''3600'' \}'
        $startScript | Should Match 'DATABENTO_MAX_SUBSCRIPTION_CONTRACTS = if \(\$EnableRutCanary\) \{ "3200" \} else \{ "3600" \}'
        $ensureScript | Should Match "DATABENTO_SHADOW_MAX_STRIKE_PAIRS = '50'"
        $startScript | Should Match 'DATABENTO_SHADOW_MAX_STRIKE_PAIRS = "50"'
        $ensureScript | Should Match 'primary-expiration bound unchanged[\s\S]+trimming surplus pairs from each[\s\S]+non-primary expiration'
        $startScript | Should Match 'primary-expiration bound unchanged[\s\S]+trimming surplus pairs from each[\s\S]+non-primary expiration'
    }

    It 'fails the optional RUT canary closed when Windows reports unsynchronized time' {
        $ensureScript | Should Match 'Leap Indicator:\\s\*3'
        $startScript | Should Match 'Leap Indicator:\\s\*3'
        $ensureScript | Should Match 'requires a synchronized Windows Time status'
        $startScript | Should Match 'requires a synchronized Windows Time status'
    }

    It 'prepares and labels the universe before either controlled backend launch' {
        $ensureScript | Should Match 'Invoke-MarketAppUniverseCachePreparation[\s\S]+universe_cache_(current_day_ready|prior_session_fallback)[\s\S]+Start-VerifiedComponent'
        $startScript | Should Match 'Invoke-MarketAppUniverseCachePreparation[\s\S]+universe_cache_(current_day_ready|prior_session_fallback)[\s\S]+Start-Process'
        $ensureScript | Should Match 'PRIOR_SESSION_FALLBACK|ProvenanceLabel'
        $startScript | Should Match 'PRIOR_SESSION_FALLBACK'
    }

    It 'bounds preparation and switches to cache-only near or after the opening boundary' {
        $ensureScript | Should Match 'Get-MarketAppUniversePreparationDeadline'
        $startScript | Should Match 'Get-MarketAppUniversePreparationDeadline'
        $ensureScript | Should Match 'Test-MarketAppUniverseProviderDiscoveryAllowed[\s\S]+SkipProviderDiscovery'
        $startScript | Should Match 'Test-MarketAppUniverseProviderDiscoveryAllowed[\s\S]+SkipProviderDiscovery'
        $ensureScript | Should Match 'StartupMayContinue[\s\S]+universe_cache_preparation_blocked_startup'
        $startScript | Should Match 'StartupMayContinue[\s\S]+universe_cache_preparation_blocked_startup'
        $supervisorModule | Should Match "-ExpectedPreparationMode 'cache_only'"
        $supervisorModule | Should Not Match 'SuppressDatabentoApiKey|SetEnvironmentVariable'
    }

    It 'rolls a listening backend when health provenance is from a prior trading date' {
        $ensureScript | Should Match 'Get-BackendUniverseState -RuntimeState \$backendReadinessState'
        $ensureScript | Should Not Match 'function Get-BackendUniverseTradingDate'
        $ensureScript | Should Match 'universe_provenance\.trading_date'
        $ensureScript | Should Match 'stale_backend_trading_date'
        $ensureScript | Should Match 'observed_trading_date=.*expected_trading_date=.*action=stage_replacement_universe'
        $ensureScript | Should Match 'Invoke-BackendPreparedAutomaticListenerStop[\s\S]+-ExpectedPid \(\[int\]\$backendPids\[0\]\)[\s\S]+-RecoveryReason ''stale_backend_trading_date'''
        $ensureScript | Should Match 'stalePreparationDeadline[\s\S]+-NotAfter \$stalePreparationNow\.Date\.AddHours\(8\)\.AddMinutes\(25\)'
    }

    It 'recovers an exact verified prior-day backend when health cannot expose its trading date' {
        $ensureScript | Should Match 'Resolve-MarketAppMissingTradingDateAction'
        $ensureScript | Should Match 'prior_day_backend_health_unverified'
        $ensureScript | Should Match 'missingDateDecision\.Reason'
        $ensureScript | Should Match 'prior_day_backend_health_unverified[\s\S]+Invoke-MarketAppBoundedAutomaticListenerStop'
        $ensureScript | Should Match 'backend_trading_date_unverified[\s\S]+action=preserve_listener'
    }

    It 'recovers a running fallback universe only after a current-day cache is staged' {
        $ensureScript | Should Match 'Get-BackendUniverseState'
        $ensureScript | Should Match 'universe_fallback_recovery_requested[\s\S]+Invoke-MarketAppUniverseCachePreparation'
        $ensureScript | Should Match 'recoveryPreparation\.UsesFallback[\s\S]+universe_fallback_recovery_pending'
        $ensureScript | Should Match 'universe_fallback_recovered[\s\S]+Invoke-MarketAppBoundedAutomaticListenerStop'
        $ensureScript | Should Match 'universe_fallback_recovery_failed[\s\S]+action=preserve_verified_listener'
    }

    It 'upgrades a clock-eligible RUT canary only before the protected opening boundary' {
        $ensureScript | Should Match 'ConfiguredSymbols = @\('
        $ensureScript | Should Match '''RUT'' -notin @\(\$backendUniverseState\.ConfiguredSymbols\)'
        $ensureScript | Should Match "AddHours\(8\)\.AddMinutes\(25\)"
        $ensureScript | Should Match 'rut_canary_preopen_upgrade[\s\S]+Invoke-BackendPreparedAutomaticListenerStop'
        $ensureScript | Should Match 'rutPreparationDeadline[\s\S]+-NotAfter \$rutUpgradeDeadline'
        $ensureScript | Should Match "RecoveryReason 'rut_canary_preopen_upgrade'[\s\S]+-RequireCurrentDay"
        $ensureScript | Should Match 'rut_canary_upgrade_missed_opening_boundary[\s\S]+action=preserve_verified_listener'
    }

    It 'stages before either automatic replacement stop and reuses the validated result' {
        $helperStart = $ensureScript.IndexOf('function Invoke-BackendPreparedAutomaticListenerStop')
        $helperPreparation = $ensureScript.IndexOf(
            '$preparation = Invoke-MarketAppUniverseCachePreparation',
            $helperStart
        )
        $helperStop = $ensureScript.IndexOf(
            '$automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop',
            $helperPreparation
        )
        ($helperStart -ge 0) | Should Be $true
        ($helperPreparation -gt $helperStart) | Should Be $true
        ($helperStop -gt $helperPreparation) | Should Be $true
        $ensureScript | Should Match 'StartupMayContinue[\s\S]+action=preserve_verified_listener[\s\S]+Invoke-MarketAppBoundedAutomaticListenerStop'
        $ensureScript | Should Match '\$preparedBackendUniverse = \$preparedStop\.Preparation'
        $ensureScript | Should Match '\$universePreparation = \$preparedBackendUniverse[\s\S]+if \(\$null -eq \$universePreparation\)[\s\S]+Invoke-MarketAppUniverseCachePreparation'
        $ensureScript | Should Match 'backend_replacement_universe_preparation_reused'
        $ensureScript | Should Match 'action=launch_without_second_provider_call'
    }

    It 'wires bounded readiness recovery after fallback and RUT precedence' {
        $ensureScript | Should Match "Get-BackendReadinessState[\s\S]+/health/live"
        $ensureScript | Should Match 'Resolve-MarketAppBackendReadinessAction'
        $ensureScript | Should Match 'RecoveryReason \$readinessDecision\.Reason[\s\S]+-RuntimeState \$backendReadinessState'
        $ensureScript | Should Match 'backend_readiness_recovery_requested'
        $ensureScript | Should Match 'Test-BackendReadinessRecoveryAlreadyAttempted'
        $ensureScript | Should Match "backend_session_recovery_requested[\s\S]+backend_session_recovery_succeeded"
        $ensureScript | Should Match 'backendLaunchResult\.ListenerProcessId[\s\S]+action=latched'
        $ensureScript | Should Match 'backend_prior_day_process_current_session_verified[\s\S]+reason=current_session_contract_progressing[\s\S]+action=preserve_listener'
        $ensureScript | Should Match "verified_prior_day_backend_preopen_refresh[\s\S]+Invoke-BackendPreparedAutomaticListenerStop[\s\S]+-RequireCurrentDay"
        $ensureScript | Should Match '\$preparedBackendUniverse = \$automaticStop\.Preparation'
        $supervisorModule | Should Match "Reason = 'verified_prior_day_backend_preopen_refresh'[\s\S]+prior_day_backend_requires_current_code"
        $ensureScript | Should Match 'backend_prior_day_process_post_close_preserved[\s\S]+reason=current_session_post_close_preserved[\s\S]+action=preserve_retained_final_state'
        $postClosePreserveIndex = $ensureScript.IndexOf(
            "elseif (`$readinessDecision.Reason -eq 'current_session_post_close_preserved')"
        )
        $genericDeferredIndex = $ensureScript.IndexOf(
            'elseif (@($readinessDecision.FailureReasons).Count -gt 0)',
            $postClosePreserveIndex
        )
        ($postClosePreserveIndex -ge 0) | Should Be $true
        ($genericDeferredIndex -gt $postClosePreserveIndex) | Should Be $true
        $fallbackIndex = $ensureScript.IndexOf("-Event 'universe_fallback_recovery_requested'")
        $rutIndex = $ensureScript.IndexOf("-Event 'rut_canary_preopen_upgrade'")
        $readinessIndex = $ensureScript.IndexOf('Resolve-MarketAppBackendReadinessAction')
        ($fallbackIndex -ge 0) | Should Be $true
        ($rutIndex -ge 0) | Should Be $true
        ($readinessIndex -gt $fallbackIndex) | Should Be $true
        ($readinessIndex -gt $rutIndex) | Should Be $true
    }

    It 'requires an owned Streamlit health response before preserving a dashboard listener' {
        $ensureScript | Should Match "Test-MarketAppHttpEndpointContract[\s\S]+/_stcore/health[\s\S]+ExpectedEndpointContract 'StreamlitHealth'"
        $ensureScript | Should Match 'Resolve-MarketAppDashboardReadinessAction'
        $ensureScript | Should Match 'Test-DashboardReadinessRecoveryAlreadyAttempted'
        $ensureScript | Should Match 'dashboard_readiness_recovery_requested[\s\S]+Invoke-MarketAppBoundedAutomaticListenerStop'
        $ensureScript | Should Match 'dashboard_session_recovery_requested[\s\S]+dashboard_session_recovery_succeeded'
        $ensureScript | Should Match 'dashboardLaunchResult\.ListenerProcessId[\s\S]+action=latched'
        $requestIndex = $ensureScript.IndexOf("-Event 'dashboard_session_recovery_requested'")
        $stopIndex = $ensureScript.IndexOf('$automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop', $requestIndex)
        $launchIndex = $ensureScript.IndexOf('$dashboardLaunchResult = Start-VerifiedComponent', $stopIndex)
        $successIndex = $ensureScript.IndexOf("-Event 'dashboard_session_recovery_succeeded'", $launchIndex)
        ($requestIndex -ge 0) | Should Be $true
        ($stopIndex -gt $requestIndex) | Should Be $true
        ($launchIndex -gt $stopIndex) | Should Be $true
        ($successIndex -gt $launchIndex) | Should Be $true
        $ensureScript | Should Match 'dashboard_readiness_recovery_deferred[\s\S]+action=preserve_listener'
        $ensureScript | Should Match 'component=dashboard port=8501 listener_pids=.*ownership_verified=.*endpoint_contract=StreamlitHealth endpoint_contract_verified=.*action=noop'
    }

    It 'routes every automatic stop through the final stop-time boundary helper' {
        $automaticStops = [regex]::Matches(
            $ensureScript,
            '(?m)^\s*\$automaticStop = Invoke-MarketAppBoundedAutomaticListenerStop\s+`'
        )
        $directStops = [regex]::Matches(
            $ensureScript,
            '(?m)^\s*Stop-VerifiedComponentListener\s+`'
        )
        $automaticStops.Count | Should Be 5
        $directStops.Count | Should Be 2
        foreach ($reason in @(
            'stale_backend_trading_date',
            'universe_fallback_recovered',
            'rut_canary_preopen_upgrade',
            'prior_day_backend_health_unverified',
            'dashboard_readiness_recovery_requested'
        )) {
            $ensureScript | Should Match "RecoveryReason '$reason'"
        }
        $ensureScript | Should Match 'RecoveryReason \$readinessDecision.Reason'
        $supervisorModule | Should Match 'stopCheckTime = Get-Date[\s\S]+Stop-Process -Id \$listenerPid'
        $supervisorModule | Should Match 'stopCheckTime -ge \$recoveryDeadline[\s\S]+action=preserve_listener'
    }

    It 'records a startup profile receipt with cache source and verified process ownership' {
        $startScript | Should Match 'marketpin-startup-profile\.v1'
        $startScript | Should Match 'requested_symbols[\s\S]+cache_file[\s\S]+cache_source_sha256[\s\S]+selected_universe_sha256'
        $startScript | Should Match 'source_fingerprint_sha256[\s\S]+backend[\s\S]+listener_pid[\s\S]+ownership_verified'
        $startScript | Should Match "Event 'startup_profile_receipt_recorded'"
    }

    It 'logs incomplete post-close evidence instead of raising a runtime failure' {
        $ensureScript | Should Match 'Resolve-MarketAppPostCloseFinalizeResult'
        $ensureScript | Should Match 'closing_tape_finalize_degraded'
        $ensureScript | Should Match 'ExpectedIncomplete'
    }

    It 'returns closing-tape status without terminating the parent supervisor runspace' {
        $startClosingScript | Should Not Match '(?m)^\s*exit\b'
        $startClosingScript | Should Match "Status = 'not_yet_due'[\s\S]+\r?\n\s*return"
        $startClosingScript | Should Match "Status = 'recovery_replay_blocked'[\s\S]+\r?\n\s*return"
        $startClosingScript | Should Match "Status = 'outside_start_window'[\s\S]+\r?\n\s*return"
        $startClosingScript | Should Match "Status = 'already_running'[\s\S]+\r?\n\s*return"
    }

    It 'defers the broad closing-tape stream until the opening ORB is complete' {
        $ensureScript | Should Match 'closing_tape_deferred_for_opening_capture'
        $ensureScript | Should Match "resultStatus -eq 'not_yet_due'"
        $ensureScript | Should Match 'protect_opening_gamma_and_orb'
        $ensureScript | Should Match 'closing_tape_deferred_for_primary_capture'
        $ensureScript | Should Match "resultStatus -eq 'primary_capture_not_ready'"
        $startClosingScript | Should Match '--require-primary-ready'
    }

    It 'preserves prior tape evidence instead of auto-replaying it during live hours' {
        $ensureScript | Should Match "resultStatus -eq 'recovery_replay_blocked'"
        $ensureScript | Should Match 'closing_tape_recovery_replay_blocked'
        $ensureScript | Should Match 'action=preserve_primary_backend'
        $ensureScript | Should Match "resultStatus -eq 'outside_start_window'[\s\S]+Resolve-MarketAppPostCloseFinalizeResult"
    }

    It 'limits explicit restart execution to the requested component' {
        $ensureScript | Should Match '\$manageBackend = -not \$explicitComponentAction -or \[bool\]\$RestartBackend'
        $ensureScript | Should Match '\$manageDashboard = \('
        $ensureScript | Should Match '\[bool\]\$StartDashboardIfMissing'
        $ensureScript | Should Match 'if \(\$manageBackend\)'
        $ensureScript | Should Match 'if \(\$manageDashboard\)'
        $ensureScript | Should Match '\$manageRecorder = -not \$explicitComponentAction'
        $ensureScript | Should Match 'Get-MarketAppVerifiedRecorderProcessId'
        $ensureScript | Should Match 'start_closing_tape\.ps1'
    }

    It 'supports start-only recovery for a missing dashboard without weakening PID restart guards' {
        $ensureScript | Should Match '\[switch\]\$StartDashboardIfMissing'
        $ensureScript | Should Match '-RestartDashboard and -StartDashboardIfMissing are mutually exclusive'
        $ensureScript | Should Match '-RestartDashboard requires -ExpectedDashboardPid'
    }

    It 'labels scheduled-task invocations explicitly' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $startMarketDayScript | Should Match '-Caller ScheduledTask'
        $registerScript | Should Match '-MultipleInstances IgnoreNew'
    }

    It 'separates auditable pre-open starts from non-replaying keep-alive checks' {
        $registerScript | Should Match "-At '07:00'"
        $registerScript | Should Match "-At '07:15'"
        $registerScript | Should Match "-At '07:45'"
        $registerScript | Should Match "-At '08:15'"
        $registerScript | Should Match 'New-ScheduledTaskTrigger -AtStartup'
        $registerScript | Should Match 'New-ScheduledTaskTrigger -AtLogOn'
        $registerScript | Should Match 'MarketPinPredictor_Watchdog'
        $registerScript | Should Match 'MSFT_TaskRepetitionPattern'
        $registerScript | Should Match "Interval='PT5M'"
        $registerScript | Should Match "Duration='PT10H10M'"
        $registerScript | Should Match 'StartupSettings[\s\S]+StartWhenAvailable'
        $watchdogSettingsBlock = [regex]::Match(
            $registerScript,
            '(?s)\$WatchdogSettings\s*=.*?(?=\r?\n\r?\nfunction Resolve-MarketTaskIdentitySid)'
        ).Value
        $watchdogSettingsBlock | Should Not Match 'StartWhenAvailable'
        $registerScript | Should Match 'WakeToRun'
        $registerScript | Should Match "\[Environment\]::SystemDirectory[\s\S]+WindowsPowerShell\\v1\.0\\powershell\.exe"
        $registerScript | Should Match 'New-ScheduledTaskPrincipal[\s\S]+-UserId \$TaskPrincipalSid[\s\S]+-LogonType ServiceAccount[\s\S]+-RunLevel Highest'
        $registerScript | Should Match '(?s)MarketPinPredictor keep-alive task.*?-Principal \$TaskPrincipal'
        $registerScript | Should Match '(?s)MarketPinPredictor pre-open task.*?-Principal \$TaskPrincipal'
        $registerScript | Should Not Match '(?m)^\s*-User \$LogonRecoveryUser\s*`'
        $registerScript | Should Match 'New-ScheduledTaskTrigger -AtLogOn -User \$LogonRecoveryUser'
        $registerScript | Should Match "MSFT_TaskBootTrigger'[\s\S]+Startup trigger count or type mismatch"
        $registerScript | Should Match "Startup boot trigger mismatch"
        $registerScript.IndexOf("ShouldProcess(`$WatchdogTaskName") | Should BeLessThan $registerScript.IndexOf("ShouldProcess(`$TaskName")
        $registerScript | Should Match 'Assert-MarketTaskReadback[\s\S]+RegisteredTaskName \$WatchdogTaskName[\s\S]+RegisteredTaskName \$TaskName'
        $registerScript | Should Match '-SkipClockSync'
        $registerScript | Should Match '-EnableRutCanary'
        $registerScript | Should Match 'if \(\$WhatIfPreference\)'
        $registerScript | Should Match 'Preflights Windows Time and the current-day universe at 07:00 CT'
        $registerScript | Should Match 'retries universe pre-stage at 07:15 CT'
        $registerScript | Should Match '\$triggers.Count -ne 6 -or \$weekly.Count -ne 4'
        $registerScript | Should Match '07:00,07:15,07:45,08:15'
    }

    It 'aligns watchdog authority with the elevated listeners it may recover' {
        $registerScript | Should Not Match '-RunLevel Limited'
        $registerScript | Should Match 'LocalSystem makes both tasks noninteractive and independent of user logon'
        $ensureScript | Should Match 'Test-MarketAppVerifiedProcess'
        $ensureScript | Should Match 'Stop-VerifiedComponentListener'
        $ensureScript | Should Match 'Assert-MarketAppExpectedListenerPid'
        $ensureScript | Should Match 'Enter-MarketAppSupervisorLock'
    }

    It 'waits for Windows Time rediscovery before the explicit market-day starts' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $startMarketDayScript | Should Match 'Sync-MarketSystemClock'
        $startMarketDayScript | Should Match 'Start-Service -Name W32Time'
        $startMarketDayScript | Should Match 'w32tm /resync /rediscover'
        $startMarketDayScript | Should Not Match '(?m)^\s*& w32tm /resync /nowait'
        $startMarketDayScript | Should Match 'UtcNow\.AddSeconds\(30\)[\s\S]+w32tm /query /status[\s\S]+Start-Sleep -Milliseconds 500'
        $startMarketDayScript | Should Match "Status='synchronized'"
        $startMarketDayScript | Should Match 'clock_sync\.log'
        $startMarketDayScript | Should Match 'PROCESSING_CLOCK_NOT_SYNCHRONIZED|fails closed'
        $startMarketDayScript | Should Match 'All-index ORB launch failed closed because RUT clock eligibility was not proven'
        $startMarketDayScript | Should Match 'No SPX/NDX/VIX-only downgrade will be launched'
        $startMarketDayScript | Should Match '(?s)if \(-not \$rutCanaryEligible\) \{.*?Write-Error.*?exit 1\s*\}'
        $startMarketDayScript | Should Not Match 'core SPX/NDX/VIX launch will continue'
    }

    It 'routes only 07:00 through 07:39:59 to bounded cache pre-stage' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $startMarketDayScript | Should Match '\$universePrestageStart = \$Date\.Date\.AddHours\(7\)'
        $startMarketDayScript | Should Match '\$universePrestageCutoff = \$Date\.Date\.AddHours\(7\)\.AddMinutes\(40\)'
        $startMarketDayScript | Should Match '\$launchWindowEndExclusive = \$Date\.Date\.AddHours\(18\)\.AddMinutes\(1\)'
        $startMarketDayScript | Should Match '\$Date -ge \$universePrestageStart -and[\s\S]+\$Date -lt \$universePrestageCutoff'
        $startMarketDayScript | Should Match '\$Date -ge \$universePrestageCutoff -and[\s\S]+\$Date -lt \$launchWindowEndExclusive'
        $startMarketDayScript | Should Match '\$clockPreflightOnly = \$action -ceq ''clock_and_universe_prestage'''
        $startMarketDayScript | Should Match "'clock_and_universe_prestage'"
        $startMarketDayScript | Should Match 'if \(\$clockPreflightOnly\)[\s\S]+-PrepareUniverseOnly[\s\S]+exit \$LASTEXITCODE'
        $startMarketDayScript | Should Match '07:00 and 07:15 triggers repair/verify time and stage only the[\s\S]+07:40-through-18:00 guarded launch window[\s\S]+pre-stage stops[\s\S]+by 07:40'
        $startMarketDayScript | Should Match "'ScheduledTaskUniversePrestage'"
    }

    It 'classifies every opening boundary and holidays under Windows PowerShell' {
        $powerShellExe = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
        $startMarketDayPath = Join-Path $ProjectRoot 'start_market_day.ps1'
        $openDayCases = @(
            @{Date='2026-09-09T00:00:00-05:00'; Expected='would_repair_scheduled_tasks_only'},
            @{Date='2026-09-09T02:00:00-05:00'; Expected='would_repair_scheduled_tasks_only'},
            @{Date='2026-09-09T06:59:59-05:00'; Expected='would_repair_scheduled_tasks_only'},
            @{Date='2026-09-09T07:00:00-05:00'; Expected='would_clock_and_universe_prestage'},
            @{Date='2026-09-09T07:39:59-05:00'; Expected='would_clock_and_universe_prestage'},
            @{Date='2026-09-09T07:40:00-05:00'; Expected='would_launch'},
            @{Date='2026-09-09T18:00:00-05:00'; Expected='would_launch'},
            @{Date='2026-09-09T18:00:00.585-05:00'; Expected='would_launch'},
            @{Date='2026-09-09T18:00:59-05:00'; Expected='would_launch'},
            @{Date='2026-09-09T18:01:00-05:00'; Expected='would_repair_scheduled_tasks_only'},
            @{Date='2026-09-09T23:59:00-05:00'; Expected='would_repair_scheduled_tasks_only'}
        )
        foreach ($case in $openDayCases) {
            $output = @(& $powerShellExe -NoProfile -NonInteractive -ExecutionPolicy Bypass `
                -File $startMarketDayPath -Date $case.Date -CheckOnly -EnableRutCanary 2>&1) -join "`n"
            $LASTEXITCODE | Should Be 0
            $output | Should Match "Action\s+:\s+$($case.Expected)"
        }

        foreach ($holidayTime in @('02:00:00','07:00:00','07:40:00','18:00:59','18:01:00','23:59:00')) {
            $output = @(& $powerShellExe -NoProfile -NonInteractive -ExecutionPolicy Bypass `
                -File $startMarketDayPath -Date "2026-09-07T$holidayTime-05:00" -CheckOnly -EnableRutCanary 2>&1) -join "`n"
            $LASTEXITCODE | Should Be 0
            $output | Should Match 'MarketOpen\s+:\s+False'
            $output | Should Match 'Action\s+:\s+abstain'
        }
    }

    It 'exits repair-only after scheduler repair and before every market or application operation' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $mainStart = $startMarketDayScript.IndexOf('$marketCalendarStatus = Get-UsCashEquityMarketCalendarStatus')
        $startupRepairIndex = $startMarketDayScript.IndexOf('$startupBootRecovery = Repair-MarketStartupBootRecovery', $mainStart)
        $watchdogRepairIndex = $startMarketDayScript.IndexOf('$watchdogAuthority = Repair-MarketWatchdogRecoveryAuthority', $startupRepairIndex)
        $repairOnlyExitIndex = $startMarketDayScript.IndexOf('if ($repairScheduledTasksOnly) { exit 0 }', $watchdogRepairIndex)
        $ensureGuardIndex = $startMarketDayScript.IndexOf('Test-Path -LiteralPath $EnsureScript', $repairOnlyExitIndex)
        $clockIndex = $startMarketDayScript.IndexOf('$clockResult = $null', $repairOnlyExitIndex)
        $rutIndex = $startMarketDayScript.IndexOf('$rutClock = Get-RutCanaryClockEligibility', $repairOnlyExitIndex)
        $prestageInvokeIndex = $startMarketDayScript.IndexOf('& powershell.exe @prestageArguments', $repairOnlyExitIndex)
        $launchInvokeIndex = $startMarketDayScript.IndexOf('& powershell.exe -NoProfile', $repairOnlyExitIndex)

        ($mainStart -ge 0) | Should Be $true
        ($startupRepairIndex -gt $mainStart) | Should Be $true
        ($watchdogRepairIndex -gt $startupRepairIndex) | Should Be $true
        ($repairOnlyExitIndex -gt $watchdogRepairIndex) | Should Be $true
        ($ensureGuardIndex -gt $repairOnlyExitIndex) | Should Be $true
        ($clockIndex -gt $repairOnlyExitIndex) | Should Be $true
        ($rutIndex -gt $repairOnlyExitIndex) | Should Be $true
        ($prestageInvokeIndex -gt $repairOnlyExitIndex) | Should Be $true
        ($launchInvokeIndex -gt $repairOnlyExitIndex) | Should Be $true
        $startMarketDayScript | Should Match 'if \(\$repairScheduledTasksOnly\) \{\s*exit 0\s*\}'
        $startMarketDayScript | Should Match 'outside 07:00 through the final 18:00[\s\S]+scheduled minute may repair only[\s\S]+never reach clock sync,[\s\S]+universe/provider discovery,[\s\S]+application lifecycle'
    }

    It 'isolates pre-stage from opening preflight and every component lifecycle path' {
        $ensureScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'ensure_market_app.ps1') -Raw
        $lockIndex = $ensureScript.IndexOf("Write-WatchdogLog -Event 'supervisor_lock_acquired'")
        $prestageIndex = $ensureScript.IndexOf('if ($PrepareUniverseOnly) {', $lockIndex)
        $normalPathIndex = $ensureScript.IndexOf('$backendWasStopped = $false', $prestageIndex)
        ($lockIndex -ge 0) | Should Be $true
        ($prestageIndex -gt $lockIndex) | Should Be $true
        ($normalPathIndex -gt $prestageIndex) | Should Be $true
        $prestageBranch = $ensureScript.Substring($prestageIndex, $normalPathIndex - $prestageIndex)
        $prestageBranch | Should Match 'Invoke-MarketAppCurrentDayUniversePrestage'
        $prestageBranch | Should Match 'universe_current_day_prestage_(failed|ready)'
        $prestageBranch | Should Not Match 'Stop-VerifiedComponentListener|Start-VerifiedComponent|Get-MarketAppVerifiedRecorderProcessId|start_closing_tape'
        $ensureScript | Should Match '\$prestageNotAfter = \$prestageNow\.Date\.AddHours\(7\)\.AddMinutes\(40\)'
        $ensureScript | Should Match '-TradingDate \$expectedTradingDate'
        $ensureScript | Should Match 'CURRENT_DAY_CACHE[\s\S]+current_day_cache_ready[\s\S]+current_day_cache[\s\S]+selected_universe_sha256'
    }

    It 'self-heals only the exact MarketPin watchdog task authority' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $startMarketDayScript | Should Match "\`$WatchdogTaskName = 'MarketPinPredictor_Watchdog'"
        $startMarketDayScript.Contains('$WatchdogTaskPath = ''\''') | Should Be $true
        $startMarketDayScript | Should Match 'Get-ScheduledTask[\s\S]+-TaskName \$TaskName[\s\S]+-TaskPath \$TaskPath'
        $startMarketDayScript | Should Not Match 'Register-ScheduledTask'
    }

    It 'repairs only a sole missing AutoStart boot trigger or exact legacy 07:15 omission before the holiday exit' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $checkOnlyIndex = $startMarketDayScript.IndexOf('if ($CheckOnly) { exit 0 }')
        $startupRepairIndex = $startMarketDayScript.IndexOf('$startupBootRecovery = Repair-MarketStartupBootRecovery')
        $holidayExitIndex = $startMarketDayScript.IndexOf('if (-not $isOpen) { exit 0 }')
        ($checkOnlyIndex -ge 0) | Should Be $true
        ($startupRepairIndex -gt $checkOnlyIndex) | Should Be $true
        ($holidayExitIndex -gt $startupRepairIndex) | Should Be $true
        $startMarketDayScript | Should Match 'RepairableMissingBootTrigger'
        $startMarketDayScript | Should Match 'RepairableMissingUniversePrestageRetry'
        $startMarketDayScript | Should Match 'boot_trigger_missing'
        $startMarketDayScript | Should Match "-At '07:15'"
        $startMarketDayScript | Should Match 'Set-ScheduledTask[\s\S]+-TaskName \$TaskName[\s\S]+-TaskPath \$TaskPath[\s\S]+-Trigger'
        $startMarketDayScript | Should Match 'existing[\s\S]+action, principal, settings, and every proven trigger remain intact'
        $startMarketDayScript | Should Match 'AutoStart trigger recovery remains pending; core launch behavior is unchanged'
    }

    It 'gates watchdog authority repair on an elevated token without blocking core launch' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $startMarketDayScript | Should Match 'Test-MarketLauncherIsElevated'
        $startMarketDayScript | Should Match "if \(-not \(Test-MarketLauncherIsElevated\)\)[\s\S]+Status = 'watchdog_authority_pending'[\s\S]+Error = 'elevation_required'"
        $startMarketDayScript | Should Match "watchdog_authority_pending'[\s\S]+core launch behavior is unchanged"
    }

    It 'updates only the watchdog principal and is idempotent once authority is aligned' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $startMarketDayScript | Should Match "Principal\.RunLevel -eq 'Highest'[\s\S]+Status = 'watchdog_authority_ready'"
        $startMarketDayScript | Should Match "New-ScheduledTaskPrincipal[\s\S]+-UserId 'S-1-5-18'[\s\S]+-LogonType ServiceAccount[\s\S]+-RunLevel Highest"
        $startMarketDayScript | Should Match 'Set-ScheduledTask[\s\S]+-TaskName \$TaskName[\s\S]+-TaskPath \$TaskPath[\s\S]+-Principal \$alignedPrincipal'
        $startMarketDayScript | Should Match 'action, triggers, and settings remain[\s\S]+untouched'
        $startMarketDayScript | Should Match "Principal\.RunLevel -ne 'Highest'[\s\S]+scoped update"
    }

    It 'repairs watchdog authority before the holiday exit while keeping CheckOnly read-only' {
        $startMarketDayScript = Get-Content -LiteralPath (Join-Path $ProjectRoot 'start_market_day.ps1') -Raw
        $checkOnlyIndex = $startMarketDayScript.IndexOf('if ($CheckOnly) { exit 0 }')
        $repairIndex = $startMarketDayScript.IndexOf('$watchdogAuthority = Repair-MarketWatchdogRecoveryAuthority')
        $holidayExitIndex = $startMarketDayScript.IndexOf('if (-not $isOpen) { exit 0 }')
        ($checkOnlyIndex -ge 0) | Should Be $true
        ($repairIndex -gt $checkOnlyIndex) | Should Be $true
        ($holidayExitIndex -gt $repairIndex) | Should Be $true
        $startMarketDayScript | Should Match 'An elevated 07:00[\s\S]+AutoStart[\s\S]+next open[\s\S]+on a holiday'
    }
}
