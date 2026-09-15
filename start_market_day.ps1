[CmdletBinding()]
param(
    [datetime]$Date = (Get-Date),
    [switch]$CheckOnly,
    [switch]$SkipClockSync,
    [switch]$EnableRutCanary
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$EnsureScript = Join-Path $ProjectRoot 'ensure_market_app.ps1'
$RuntimeLogDir = Join-Path $ProjectRoot 'logs\runtime'
$ClockLogPath = Join-Path $RuntimeLogDir 'clock_sync.log'
$StartupTaskName = 'MarketPinPredictor_AutoStart'
$StartupTaskPath = '\'
$WatchdogTaskName = 'MarketPinPredictor_Watchdog'
$WatchdogTaskPath = '\'
$MarketCalendarModule = Join-Path $ProjectRoot 'market_calendar.psm1'
$BootstrapMutexTimeoutMilliseconds = 45000

if (-not (Test-Path -LiteralPath $MarketCalendarModule -PathType Leaf)) {
    throw "Market calendar module was not found: $MarketCalendarModule"
}
Import-Module -Name $MarketCalendarModule -Force -ErrorAction Stop

function Sync-MarketSystemClock {
    try {
        $service = Get-Service -Name W32Time -ErrorAction Stop
        if ($service.StartType -ne 'Automatic') {
            Set-Service -Name W32Time -StartupType Automatic -ErrorAction Stop
        }
        if ($service.Status -ne 'Running') {
            Start-Service -Name W32Time -ErrorAction Stop
        }
        # Rediscover the configured source and wait for the request to finish.
        # `/nowait` returned before Windows applied Friday's correction, which
        # left the opening calculations five seconds behind provider time.
        & w32tm /resync /rediscover *> $null
        if ($LASTEXITCODE -ne 0) {
            throw "w32tm /resync /rediscover returned exit code $LASTEXITCODE"
        }

        $deadline = [DateTime]::UtcNow.AddSeconds(30)
        do {
            $timeStatus = @(& w32tm /query /status 2>&1)
            $queryExitCode = $LASTEXITCODE
            $statusText = $timeStatus -join "`n"
            if ($queryExitCode -eq 0 -and $statusText -notmatch 'Leap Indicator:\s*3') {
                return [pscustomobject]@{Status='synchronized';Service='running';Error=$null}
            }
            Start-Sleep -Milliseconds 500
        } while ([DateTime]::UtcNow -lt $deadline)

        throw 'Windows Time did not report synchronized status within 30 seconds'
    }
    catch {
        # Clock repair is best-effort. The live formula still fails closed when
        # provider receive timestamps prove the processing clock is unsafe.
        return [pscustomobject]@{
            Status='warning'
            Service=(Get-Service -Name W32Time -ErrorAction SilentlyContinue).Status
            Error=$_.Exception.Message
        }
    }
}

function Get-MarketDayBootstrapMutexName {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $canonicalRoot = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    ).ToUpperInvariant()
    $sha256 = [Security.Cryptography.SHA256]::Create()
    try {
        $digest = $sha256.ComputeHash([Text.Encoding]::UTF8.GetBytes($canonicalRoot))
    }
    finally {
        $sha256.Dispose()
    }
    $suffix = ([BitConverter]::ToString($digest)).Replace('-', '').ToLowerInvariant().Substring(0, 24)
    return "Global\MarketPinPredictor.MarketDayBootstrap.$suffix"
}

function Enter-MarketDayBootstrapLock {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [ValidateRange(0, 60000)][int]$TimeoutMilliseconds = 45000
    )

    $mutexName = Get-MarketDayBootstrapMutexName -ProjectRoot $ProjectRoot
    $mutex = [Threading.Mutex]::new($false, $mutexName)
    $acquired = $false
    try {
        try {
            $acquired = $mutex.WaitOne($TimeoutMilliseconds)
        }
        catch [Threading.AbandonedMutexException] {
            $acquired = $true
        }
        return [pscustomobject]@{
            Name = $mutexName
            Mutex = $mutex
            Acquired = [bool]$acquired
        }
    }
    catch {
        $mutex.Dispose()
        throw
    }
}

function Exit-MarketDayBootstrapLock {
    param($LockHandle)

    if ($null -eq $LockHandle) { return }
    try {
        if ([bool]$LockHandle.Acquired) {
            $LockHandle.Mutex.ReleaseMutex()
        }
    }
    finally {
        if ($LockHandle.Mutex) {
            $LockHandle.Mutex.Dispose()
        }
    }
}

function Write-MarketClockLog {
    param(
        [Parameter(Mandatory = $true)]$Result,
        [ValidateRange(1, 20)][int]$MaxAttempts = 5,
        [ValidateRange(1, 1000)][int]$RetryDelayMilliseconds = 100
    )

    $safeError = ([string]$Result.Error -replace '[\r\n]+', ' ' -replace '\s+', '_')
    $line = '{0} powershell_pid={1} status={2} service={3} error={4}' -f `
        (Get-Date).ToString('yyyy-MM-dd HH:mm:ss zzz'), `
        $PID, `
        $Result.Status, `
        $Result.Service, `
        $(if ($safeError) { $safeError } else { 'none' })

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            New-Item -ItemType Directory -Path $RuntimeLogDir -Force | Out-Null
            Add-Content -LiteralPath $ClockLogPath -Value $line -Encoding UTF8 -ErrorAction Stop
            return
        }
        catch {
            if ($attempt -lt $MaxAttempts) {
                Start-Sleep -Milliseconds $RetryDelayMilliseconds
                continue
            }
            $safeWriteError = ([string]$_.Exception.Message -replace '[\r\n]+', ' ' -replace '\s+', '_')
            Write-Warning "Market clock evidence append failed after $MaxAttempts attempts; launch will continue. error=$safeWriteError"
            return
        }
    }
}

function Test-MarketLauncherIsElevated {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function ConvertTo-MarketTaskArgumentTokens {
    param([Parameter(Mandatory = $true)][string]$Arguments)

    $tokens = [System.Collections.Generic.List[string]]::new()
    $token = [System.Text.StringBuilder]::new()
    $insideQuotes = $false
    $tokenStarted = $false
    foreach ($character in $Arguments.ToCharArray()) {
        if ($character -eq '"') {
            $insideQuotes = -not $insideQuotes
            $tokenStarted = $true
            continue
        }
        if ([char]::IsWhiteSpace($character) -and -not $insideQuotes) {
            if ($tokenStarted) {
                $tokens.Add($token.ToString())
                [void]$token.Clear()
                $tokenStarted = $false
            }
            continue
        }
        [void]$token.Append($character)
        $tokenStarted = $true
    }
    if ($insideQuotes) {
        return $null
    }
    if ($tokenStarted) {
        $tokens.Add($token.ToString())
    }
    return $tokens.ToArray()
}

function Resolve-MarketWindowsIdentitySid {
    param([Parameter(Mandatory = $true)][string]$Identity)

    try {
        if ($Identity -match '^S-\d-(?:\d+-)+\d+$') {
            return ([Security.Principal.SecurityIdentifier]::new($Identity)).Value
        }
        $account = [Security.Principal.NTAccount]::new($Identity)
        return $account.Translate([Security.Principal.SecurityIdentifier]).Value
    }
    catch {
        if ($Identity -notmatch '\\' -and $env:USERDOMAIN) {
            try {
                $qualified = [Security.Principal.NTAccount]::new("$env:USERDOMAIN\$Identity")
                return $qualified.Translate([Security.Principal.SecurityIdentifier]).Value
            }
            catch {
                return $null
            }
        }
        return $null
    }
}

function Test-MarketWatchdogTaskContract {
    param(
        [Parameter(Mandatory = $true)][psobject]$Task,
        [Parameter(Mandatory = $true)][string]$ExpectedTaskName,
        [Parameter(Mandatory = $true)][string]$ExpectedTaskPath,
        [Parameter(Mandatory = $true)][string]$ExpectedProjectRoot,
        [string]$ExpectedPrincipalSid = 'S-1-5-18',
        [Parameter(Mandatory = $true)][string]$ExpectedPowerShellExe
    )

    $reasons = [System.Collections.Generic.List[string]]::new()
    if ([string]$Task.TaskName -cne $ExpectedTaskName) {
        $reasons.Add('task_name_mismatch')
    }
    if ([string]$Task.TaskPath -cne $ExpectedTaskPath) {
        $reasons.Add('task_path_mismatch')
    }
    $taskSid = Resolve-MarketWindowsIdentitySid -Identity ([string]$Task.Principal.UserId)
    if (-not $taskSid -or $taskSid -cne $ExpectedPrincipalSid) {
        $reasons.Add('principal_user_sid_mismatch')
    }
    if ([string]$Task.Principal.LogonType -cne 'ServiceAccount') {
        $reasons.Add('principal_logon_type_mismatch')
    }
    $principalRunLevel = [string]$Task.Principal.RunLevel
    if ($principalRunLevel -cne 'Highest') {
        $reasons.Add('principal_run_level_mismatch')
    }

    $actions = @($Task.Actions)
    if ($actions.Count -ne 1) {
        $reasons.Add('action_count_mismatch')
    }
    else {
        $action = $actions[0]
        try {
            $actualExecute = [IO.Path]::GetFullPath([string]$action.Execute)
            $expectedExecute = [IO.Path]::GetFullPath($ExpectedPowerShellExe)
            if (-not $actualExecute.Equals($expectedExecute, [StringComparison]::OrdinalIgnoreCase)) {
                $reasons.Add('action_executable_mismatch')
            }
        }
        catch {
            $reasons.Add('action_executable_invalid')
        }
        try {
            $actualWorkingDirectory = [IO.Path]::GetFullPath([string]$action.WorkingDirectory).TrimEnd('\')
            $expectedWorkingDirectory = [IO.Path]::GetFullPath($ExpectedProjectRoot).TrimEnd('\')
            if (-not $actualWorkingDirectory.Equals($expectedWorkingDirectory, [StringComparison]::OrdinalIgnoreCase)) {
                $reasons.Add('action_working_directory_mismatch')
            }
        }
        catch {
            $reasons.Add('action_working_directory_invalid')
        }

        $expectedLaunchScript = Join-Path ([IO.Path]::GetFullPath($ExpectedProjectRoot)) 'start_market_day.ps1'
        $expectedTokens = @(
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-File',
            $expectedLaunchScript,
            '-EnableRutCanary',
            '-SkipClockSync'
        )
        $actualTokens = @(ConvertTo-MarketTaskArgumentTokens -Arguments ([string]$action.Arguments))
        $argumentsMatch = $actualTokens.Count -eq $expectedTokens.Count
        if ($argumentsMatch) {
            for ($index = 0; $index -lt $expectedTokens.Count; $index++) {
                if (-not ([string]$actualTokens[$index]).Equals(
                    [string]$expectedTokens[$index],
                    [StringComparison]::OrdinalIgnoreCase
                )) {
                    $argumentsMatch = $false
                    break
                }
            }
        }
        if (-not $argumentsMatch) {
            $reasons.Add('action_arguments_mismatch')
        }
    }

    $settings = $Task.Settings
    if (-not [bool]$settings.Enabled) { $reasons.Add('settings_disabled') }
    if ([string]$settings.MultipleInstances -cne 'IgnoreNew') { $reasons.Add('settings_multiple_instances_mismatch') }
    if ([string]$settings.ExecutionTimeLimit -cne 'PT15M') { $reasons.Add('settings_execution_limit_mismatch') }
    if ([bool]$settings.DisallowStartIfOnBatteries -or [bool]$settings.StopIfGoingOnBatteries) {
        $reasons.Add('settings_battery_policy_mismatch')
    }
    if (-not [bool]$settings.WakeToRun) { $reasons.Add('settings_wake_to_run_disabled') }
    if ([bool]$settings.StartWhenAvailable) { $reasons.Add('settings_start_when_available_enabled') }
    if (
        [int]$settings.RestartCount -ne 0 -or
        -not [string]::IsNullOrEmpty([string]$settings.RestartInterval)
    ) { $reasons.Add('settings_restart_policy_mismatch') }

    $triggers = @($Task.Triggers)
    if (
        $triggers.Count -ne 1 -or
        $triggers[0].CimClass.CimClassName -cne 'MSFT_TaskWeeklyTrigger'
    ) {
        $reasons.Add('trigger_count_or_type_mismatch')
    }
    else {
        $trigger = $triggers[0]
        $startTime = $null
        try { $startTime = ([datetime]$trigger.StartBoundary).ToString('HH:mm') }
        catch { $startTime = $null }
        if (
            -not $trigger.Enabled -or
            $startTime -cne '07:50' -or
            [int]$trigger.DaysOfWeek -ne 62 -or
            [int]$trigger.WeeksInterval -ne 1 -or
            [string]$trigger.EndBoundary -ne '' -or
            [string]$trigger.RandomDelay -ne '' -or
            [string]$trigger.Repetition.Interval -cne 'PT5M' -or
            [string]$trigger.Repetition.Duration -cne 'PT10H10M' -or
            -not [bool]$trigger.Repetition.StopAtDurationEnd
        ) { $reasons.Add('trigger_weekly_shape_mismatch') }
    }

    $repairableLimitedPrincipal = (
        $principalRunLevel -ceq 'Limited' -and
        $reasons.Count -eq 1 -and
        $reasons[0] -ceq 'principal_run_level_mismatch'
    )

    return [pscustomobject]@{
        Valid = $reasons.Count -eq 0
        RepairableLimitedPrincipal = $repairableLimitedPrincipal
        Reasons = @($reasons)
        PrincipalSid = $taskSid
    }
}

function Test-MarketStartupTaskContract {
    param(
        [Parameter(Mandatory = $true)][psobject]$Task,
        [Parameter(Mandatory = $true)][string]$ExpectedTaskName,
        [Parameter(Mandatory = $true)][string]$ExpectedTaskPath,
        [Parameter(Mandatory = $true)][string]$ExpectedProjectRoot,
        [Parameter(Mandatory = $true)][string]$ExpectedLogonRecoverySid,
        [string]$ExpectedPrincipalSid = 'S-1-5-18',
        [Parameter(Mandatory = $true)][string]$ExpectedPowerShellExe
    )

    $reasons = [System.Collections.Generic.List[string]]::new()
    if ([string]$Task.TaskName -cne $ExpectedTaskName) { $reasons.Add('task_name_mismatch') }
    if ([string]$Task.TaskPath -cne $ExpectedTaskPath) { $reasons.Add('task_path_mismatch') }
    $taskSid = Resolve-MarketWindowsIdentitySid -Identity ([string]$Task.Principal.UserId)
    if (-not $taskSid -or $taskSid -cne $ExpectedPrincipalSid) { $reasons.Add('principal_user_sid_mismatch') }
    if ([string]$Task.Principal.LogonType -cne 'ServiceAccount') { $reasons.Add('principal_logon_type_mismatch') }
    if ([string]$Task.Principal.RunLevel -cne 'Highest') { $reasons.Add('principal_run_level_mismatch') }

    $actions = @($Task.Actions)
    if ($actions.Count -ne 1) {
        $reasons.Add('action_count_mismatch')
    }
    else {
        $action = $actions[0]
        try {
            if (-not ([IO.Path]::GetFullPath([string]$action.Execute)).Equals(
                [IO.Path]::GetFullPath($ExpectedPowerShellExe),
                [StringComparison]::OrdinalIgnoreCase
            )) { $reasons.Add('action_executable_mismatch') }
        }
        catch { $reasons.Add('action_executable_invalid') }
        try {
            $actualRoot = [IO.Path]::GetFullPath([string]$action.WorkingDirectory).TrimEnd('\')
            $expectedRoot = [IO.Path]::GetFullPath($ExpectedProjectRoot).TrimEnd('\')
            if (-not $actualRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase)) {
                $reasons.Add('action_working_directory_mismatch')
            }
        }
        catch { $reasons.Add('action_working_directory_invalid') }

        $expectedTokens = @(
            '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File',
            (Join-Path ([IO.Path]::GetFullPath($ExpectedProjectRoot)) 'start_market_day.ps1'),
            '-EnableRutCanary'
        )
        $actualTokens = @(ConvertTo-MarketTaskArgumentTokens -Arguments ([string]$action.Arguments))
        $argumentsMatch = $actualTokens.Count -eq $expectedTokens.Count
        if ($argumentsMatch) {
            for ($index = 0; $index -lt $expectedTokens.Count; $index++) {
                if (-not ([string]$actualTokens[$index]).Equals(
                    [string]$expectedTokens[$index],
                    [StringComparison]::OrdinalIgnoreCase
                )) {
                    $argumentsMatch = $false
                    break
                }
            }
        }
        if (-not $argumentsMatch) { $reasons.Add('action_arguments_mismatch') }
    }

    $settings = $Task.Settings
    if (-not [bool]$settings.Enabled) { $reasons.Add('settings_disabled') }
    if ([string]$settings.MultipleInstances -cne 'IgnoreNew') { $reasons.Add('settings_multiple_instances_mismatch') }
    if ([string]$settings.ExecutionTimeLimit -cne 'PT15M') { $reasons.Add('settings_execution_limit_mismatch') }
    if ([bool]$settings.DisallowStartIfOnBatteries -or [bool]$settings.StopIfGoingOnBatteries) { $reasons.Add('settings_battery_policy_mismatch') }
    if (-not [bool]$settings.WakeToRun) { $reasons.Add('settings_wake_to_run_disabled') }
    if (-not [bool]$settings.StartWhenAvailable) { $reasons.Add('settings_start_when_available_disabled') }
    if ([int]$settings.RestartCount -ne 3 -or [string]$settings.RestartInterval -cne 'PT1M') { $reasons.Add('settings_restart_policy_mismatch') }

    $triggers = @($Task.Triggers)
    $weekly = @($triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskWeeklyTrigger' })
    $boot = @($triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskBootTrigger' })
    $logon = @($triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger' })
    $knownTriggerCount = $weekly.Count + $boot.Count + $logon.Count
    if ($knownTriggerCount -ne $triggers.Count) { $reasons.Add('trigger_unexpected_type') }

    $times = [System.Collections.Generic.List[string]]::new()
    $weeklyShapeValid = $true
    foreach ($trigger in $weekly) {
        try { $times.Add(([datetime]$trigger.StartBoundary).ToString('HH:mm')) }
        catch { $weeklyShapeValid = $false }
        if (
            -not $trigger.Enabled -or
            [int]$trigger.DaysOfWeek -ne 62 -or
            [int]$trigger.WeeksInterval -ne 1 -or
            [string]$trigger.EndBoundary -ne '' -or
            [string]$trigger.RandomDelay -ne '' -or
            [string]$trigger.Repetition.Interval -ne '' -or
            [string]$trigger.Repetition.Duration -ne ''
        ) { $weeklyShapeValid = $false }
    }
    $weeklyTimes = ($times | Sort-Object) -join ','
    $legacyWeeklyContract = (
        $weekly.Count -eq 3 -and
        $weeklyShapeValid -and
        $weeklyTimes -ceq '07:00,07:45,08:15'
    )
    if ($weekly.Count -ne 4) {
        $reasons.Add('trigger_weekly_count_mismatch')
    }
    if (-not $weeklyShapeValid -or $weeklyTimes -cne '07:00,07:15,07:45,08:15') {
        $reasons.Add('trigger_weekly_shape_mismatch')
    }

    if ($logon.Count -ne 1) {
        $reasons.Add('trigger_logon_count_mismatch')
    }
    else {
        $logonSid = Resolve-MarketWindowsIdentitySid -Identity ([string]$logon[0].UserId)
        if (
            -not $logon[0].Enabled -or
            -not $logonSid -or
            $logonSid -cne $ExpectedLogonRecoverySid -or
            [string]$logon[0].Delay -ne '' -or
            [string]$logon[0].Repetition.Interval -ne '' -or
            [string]$logon[0].Repetition.Duration -ne ''
        ) { $reasons.Add('trigger_logon_shape_mismatch') }
    }

    if ($boot.Count -eq 0) {
        $reasons.Add('boot_trigger_missing')
    }
    elseif ($boot.Count -ne 1) {
        $reasons.Add('boot_trigger_count_mismatch')
    }
    elseif (
        -not $boot[0].Enabled -or
        [string]$boot[0].StartBoundary -ne '' -or
        [string]$boot[0].EndBoundary -ne '' -or
        [string]$boot[0].Delay -ne '' -or
        [string]$boot[0].Repetition.Interval -ne '' -or
        [string]$boot[0].Repetition.Duration -ne ''
    ) { $reasons.Add('boot_trigger_shape_mismatch') }

    return [pscustomobject]@{
        Valid = $reasons.Count -eq 0
        RepairableMissingBootTrigger = (
            $reasons.Count -eq 1 -and $reasons[0] -ceq 'boot_trigger_missing'
        )
        RepairableMissingUniversePrestageRetry = (
            $legacyWeeklyContract -and
            @($reasons | Where-Object {
                $_ -cne 'trigger_weekly_count_mismatch' -and
                $_ -cne 'trigger_weekly_shape_mismatch'
            }).Count -eq 0
        )
        Reasons = @($reasons)
        PrincipalSid = $taskSid
    }
}

function Repair-MarketStartupBootRecovery {
    param(
        [string]$TaskName = $StartupTaskName,
        [string]$TaskPath = $StartupTaskPath,
        [string]$ExpectedProjectRoot = $ProjectRoot,
        [string]$ExpectedLogonRecoverySid
    )

    if (-not (Test-MarketLauncherIsElevated)) {
        return [pscustomobject]@{Status='startup_boot_recovery_pending';Service='TaskScheduler';Error='elevation_required'}
    }
    try {
        if (-not $ExpectedLogonRecoverySid) {
            $projectOwner = (Get-Acl -LiteralPath $ExpectedProjectRoot -ErrorAction Stop).Owner
            $ExpectedLogonRecoverySid = Resolve-MarketWindowsIdentitySid -Identity ([string]$projectOwner)
        }
        if (-not $ExpectedLogonRecoverySid) { throw 'project_owner_sid_unavailable' }
        $expectedPowerShellExe = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
        $task = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -ErrorAction Stop
        $contract = Test-MarketStartupTaskContract `
            -Task $task `
            -ExpectedTaskName $StartupTaskName `
            -ExpectedTaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ExpectedProjectRoot `
            -ExpectedLogonRecoverySid $ExpectedLogonRecoverySid `
            -ExpectedPrincipalSid 'S-1-5-18' `
            -ExpectedPowerShellExe $expectedPowerShellExe
        if ($contract.Valid) {
            return [pscustomobject]@{Status='startup_boot_recovery_ready';Service='TaskScheduler';Error=$null}
        }
        $repairTrigger = $null
        $repairedStatus = $null
        if ($contract.RepairableMissingBootTrigger) {
            $repairTrigger = New-ScheduledTaskTrigger -AtStartup
            $repairedStatus = 'startup_boot_recovery_repaired'
        }
        elseif ($contract.RepairableMissingUniversePrestageRetry) {
            $repairTrigger = New-ScheduledTaskTrigger `
                -Weekly `
                -WeeksInterval 1 `
                -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
                -At '07:15'
            $repairedStatus = 'startup_universe_prestage_retry_repaired'
        }
        else {
            return [pscustomobject]@{
                Status='startup_boot_recovery_pending'
                Service='TaskScheduler'
                Error="task_contract_mismatch:$(@($contract.Reasons) -join ',')"
            }
        }

        # Change only the Trigger property on the exact root task. The existing
        # action, principal, settings, and every proven trigger remain intact.
        Set-ScheduledTask `
            -TaskName $TaskName `
            -TaskPath $TaskPath `
            -Trigger @(@($task.Triggers) + $repairTrigger) `
            -ErrorAction Stop | Out-Null
        $verified = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath -ErrorAction Stop
        $verifiedContract = Test-MarketStartupTaskContract `
            -Task $verified `
            -ExpectedTaskName $StartupTaskName `
            -ExpectedTaskPath $StartupTaskPath `
            -ExpectedProjectRoot $ExpectedProjectRoot `
            -ExpectedLogonRecoverySid $ExpectedLogonRecoverySid `
            -ExpectedPrincipalSid 'S-1-5-18' `
            -ExpectedPowerShellExe $expectedPowerShellExe
        if (-not $verifiedContract.Valid) {
            throw "AutoStart task did not verify after trigger-only repair: $(@($verifiedContract.Reasons) -join ',')"
        }
        return [pscustomobject]@{Status=$repairedStatus;Service='TaskScheduler';Error=$null}
    }
    catch {
        return [pscustomobject]@{Status='startup_boot_recovery_pending';Service='TaskScheduler';Error=$_.Exception.Message}
    }
}

function Repair-MarketWatchdogRecoveryAuthority {
    param(
        [string]$TaskName = $WatchdogTaskName,
        [string]$TaskPath = $WatchdogTaskPath
    )

    if (-not (Test-MarketLauncherIsElevated)) {
        return [pscustomobject]@{
            Status = 'watchdog_authority_pending'
            Service = 'TaskScheduler'
            Error = 'elevation_required'
        }
    }
    try {
        $expectedPowerShellExe = Join-Path `
            ([Environment]::SystemDirectory) `
            'WindowsPowerShell\v1.0\powershell.exe'
        $task = Get-ScheduledTask `
            -TaskName $TaskName `
            -TaskPath $TaskPath `
            -ErrorAction Stop
        $contract = Test-MarketWatchdogTaskContract `
            -Task $task `
            -ExpectedTaskName $WatchdogTaskName `
            -ExpectedTaskPath $WatchdogTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedPrincipalSid 'S-1-5-18' `
            -ExpectedPowerShellExe $expectedPowerShellExe
        if (-not $contract.Valid -and -not $contract.RepairableLimitedPrincipal) {
            return [pscustomobject]@{
                Status = 'watchdog_authority_pending'
                Service = 'TaskScheduler'
                Error = "task_contract_mismatch:$(@($contract.Reasons) -join ',')"
            }
        }
        if ([string]$task.Principal.RunLevel -eq 'Highest') {
            return [pscustomobject]@{
                Status = 'watchdog_authority_ready'
                Service = 'TaskScheduler'
                Error = $null
            }
        }

        # Set-ScheduledTask receives the exact task identity plus only a
        # replacement principal. Its action, triggers, and settings remain
        # untouched.
        $alignedPrincipal = New-ScheduledTaskPrincipal `
            -UserId 'S-1-5-18' `
            -LogonType ServiceAccount `
            -RunLevel Highest
        Set-ScheduledTask `
            -TaskName $TaskName `
            -TaskPath $TaskPath `
            -Principal $alignedPrincipal `
            -ErrorAction Stop | Out-Null
        $verified = Get-ScheduledTask `
            -TaskName $TaskName `
            -TaskPath $TaskPath `
            -ErrorAction Stop
        $verifiedContract = Test-MarketWatchdogTaskContract `
            -Task $verified `
            -ExpectedTaskName $WatchdogTaskName `
            -ExpectedTaskPath $WatchdogTaskPath `
            -ExpectedProjectRoot $ProjectRoot `
            -ExpectedPrincipalSid 'S-1-5-18' `
            -ExpectedPowerShellExe $expectedPowerShellExe
        if (-not $verifiedContract.Valid -or [string]$verified.Principal.RunLevel -ne 'Highest') {
            throw "Watchdog task did not verify at Highest with its exact contract after the scoped update: $(@($verifiedContract.Reasons) -join ',')"
        }
        return [pscustomobject]@{
            Status = 'watchdog_authority_repaired'
            Service = 'TaskScheduler'
            Error = $null
        }
    }
    catch {
        return [pscustomobject]@{
            Status = 'watchdog_authority_pending'
            Service = 'TaskScheduler'
            Error = $_.Exception.Message
        }
    }
}

function Get-RutCanaryClockEligibility {
    try {
        $service = Get-Service -Name W32Time -ErrorAction Stop
        $timeStatus = @(& w32tm /query /status 2>&1)
        $statusText = $timeStatus -join "`n"
        if ($LASTEXITCODE -ne 0) {
            throw "w32tm /query /status returned exit code $LASTEXITCODE"
        }
        if ($service.Status -ne 'Running') {
            throw 'Windows Time service is not running'
        }
        if ($statusText -match 'Leap Indicator:\s*3') {
            throw 'Windows reports Leap Indicator 3 (not synchronized)'
        }
        return [pscustomobject]@{Eligible=$true;Status='rut_canary_eligible';Service=$service.Status;Error=$null}
    }
    catch {
        return [pscustomobject]@{
            Eligible=$false
            Status='rut_canary_blocked'
            Service=(Get-Service -Name W32Time -ErrorAction SilentlyContinue).Status
            Error=$_.Exception.Message
        }
    }
}

function Get-MarketDayInvocationAction {
    param(
        [Parameter(Mandatory = $true)][datetime]$Date,
        [Parameter(Mandatory = $true)][bool]$MarketOpen,
        [switch]$CheckOnly
    )

    if (-not $MarketOpen) { return 'abstain' }

    $universePrestageStart = $Date.Date.AddHours(7)
    $universePrestageCutoff = $Date.Date.AddHours(7).AddMinutes(40)
    # Task Scheduler can dispatch the final 18:00 watchdog with subsecond lag.
    # Preserve only that scheduled minute; 18:01 and later is repair-only.
    $launchWindowEndExclusive = $Date.Date.AddHours(18).AddMinutes(1)
    $withinUniversePrestageWindow = (
        $Date -ge $universePrestageStart -and
        $Date -lt $universePrestageCutoff
    )
    $withinLaunchWindow = (
        $Date -ge $universePrestageCutoff -and
        $Date -lt $launchWindowEndExclusive
    )

    if ($CheckOnly) {
        if ($withinUniversePrestageWindow) { return 'would_clock_and_universe_prestage' }
        if ($withinLaunchWindow) { return 'would_launch' }
        return 'would_repair_scheduled_tasks_only'
    }
    if ($withinUniversePrestageWindow) { return 'clock_and_universe_prestage' }
    if ($withinLaunchWindow) { return 'launch' }
    return 'repair_scheduled_tasks_only'
}

$marketCalendarStatus = Get-UsCashEquityMarketCalendarStatus -Candidate $Date
$isOpen = [bool]$marketCalendarStatus.MarketOpen
$action = Get-MarketDayInvocationAction `
    -Date $Date `
    -MarketOpen $isOpen `
    -CheckOnly:$CheckOnly
$clockPreflightOnly = $action -ceq 'clock_and_universe_prestage'
$repairScheduledTasksOnly = $action -ceq 'repair_scheduled_tasks_only'
[pscustomobject]@{
    Date = $Date.ToString('yyyy-MM-dd')
    DayOfWeek = $Date.DayOfWeek.ToString()
    CalendarSupported = [bool]$marketCalendarStatus.Supported
    CalendarReason = [string]$marketCalendarStatus.Reason
    MarketOpen = $isOpen
    Action = $action
} | Format-List
if ($CheckOnly) { exit 0 }
$bootstrapLock = Enter-MarketDayBootstrapLock `
    -ProjectRoot $ProjectRoot `
    -TimeoutMilliseconds $BootstrapMutexTimeoutMilliseconds
if (-not $bootstrapLock.Acquired) {
    $bootstrapLock.Mutex.Dispose()
    Write-Warning "Another market-day wrapper still owns $($bootstrapLock.Name); this redundant invocation will exit without entering the guarded launcher."
    exit 0
}

$prestageArguments = $null
$rutCanaryEligible = $false
try {
    # These repairs intentionally precede the holiday exit. An elevated 07:00
    # AutoStart can close the boot-recovery gap, add the exact 07:15 universe
    # pre-stage retry to the legacy schedule, and align the watchdog before the
    # next open without launching the application on a holiday.
    $startupBootRecovery = Repair-MarketStartupBootRecovery
    $startupBootRecovery | Format-List
    Write-MarketClockLog -Result $startupBootRecovery
    if ($startupBootRecovery.Status -eq 'startup_boot_recovery_pending') {
        Write-Warning "AutoStart trigger recovery remains pending; core launch behavior is unchanged. $($startupBootRecovery.Error)"
    }
    $watchdogAuthority = Repair-MarketWatchdogRecoveryAuthority
    $watchdogAuthority | Format-List
    Write-MarketClockLog -Result $watchdogAuthority
    if ($watchdogAuthority.Status -eq 'watchdog_authority_pending') {
        Write-Warning "Watchdog recovery authority is pending; core launch behavior is unchanged. $($watchdogAuthority.Error)"
    }
    if (-not $marketCalendarStatus.Supported) {
        Write-Error "Market calendar is unavailable for $($Date.ToString('yyyy-MM-dd')); launch failed closed. reason=$($marketCalendarStatus.Reason)"
        exit 1
    }
    if (-not $isOpen) { exit 0 }
    # Boot, logon, or manual invocations outside 07:00 through the final 18:00
    # scheduled minute may repair only the exact scheduler contracts above.
    # They must never reach clock sync, universe/provider discovery, RUT
    # eligibility, or application lifecycle.
    if ($repairScheduledTasksOnly) { exit 0 }
    if (-not (Test-Path -LiteralPath $EnsureScript -PathType Leaf)) { throw "Guarded app launcher was not found: $EnsureScript" }
    $clockResult = $null
    if (-not $SkipClockSync) {
        $clockResult = Sync-MarketSystemClock
        $clockResult | Format-List
        Write-MarketClockLog -Result $clockResult
    }
    if ($EnableRutCanary) {
        $rutClock = Get-RutCanaryClockEligibility
        Write-MarketClockLog -Result $rutClock
        $rutCanaryEligible = [bool]$rutClock.Eligible
        if (-not $rutCanaryEligible) {
            Write-Error "All-index ORB launch failed closed because RUT clock eligibility was not proven. No SPX/NDX/VIX-only downgrade will be launched. $($rutClock.Error)"
            exit 1
        }
    }
    if ($clockPreflightOnly) {
        # The 07:00 and 07:15 triggers repair/verify time and stage only the
        # bounded current-day universe cache. The application remains owned by
        # the 07:40-through-18:00 guarded launch window, including the
        # 07:45/08:15 triggers and in-window logon recovery. The pre-stage stops
        # by 07:40.
        if ($clockResult -and $clockResult.Status -ne 'synchronized') {
            exit 1
        }

        $prestageArguments = @(
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy',
            'Bypass',
            '-File',
            $EnsureScript,
            '-Caller',
            'ScheduledTaskUniversePrestage',
            '-PrepareUniverseOnly'
        )
        if ($rutCanaryEligible) {
            $prestageArguments += '-EnableRutCanary'
        }
    }
}
finally {
    # The downstream launcher owns its own process-lifecycle mutex. Never hold
    # this wrapper lock while invoking it or the nested guard would deadlock.
    Exit-MarketDayBootstrapLock -LockHandle $bootstrapLock
}

if ($null -ne $prestageArguments) {
    & powershell.exe @prestageArguments
    exit $LASTEXITCODE
}
if ($rutCanaryEligible) {
    & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $EnsureScript -Caller ScheduledTask -EnableRutCanary
}
else {
    & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $EnsureScript -Caller ScheduledTask
}
exit $LASTEXITCODE
