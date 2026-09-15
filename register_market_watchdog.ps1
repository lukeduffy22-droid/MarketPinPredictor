[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$TaskName = 'MarketPinPredictor_AutoStart',
    [string]$WatchdogTaskName = 'MarketPinPredictor_Watchdog',
    [string]$LogonRecoveryUser = "$env:USERDOMAIN\$env:USERNAME"
)

$ErrorActionPreference = 'Stop'
if ($TaskName -ceq $WatchdogTaskName) {
    throw 'The startup and watchdog task names must be distinct.'
}
$ProjectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$LaunchScript = Join-Path $ProjectRoot 'start_market_day.ps1'
if (-not (Test-Path -LiteralPath $LaunchScript -PathType Leaf)) {
    throw "Market-day launcher was not found: $LaunchScript"
}
$timeZone = Get-TimeZone
if ($timeZone.Id -ne 'Central Standard Time') {
    throw "Registration requires Windows Central time; current zone is '$($timeZone.Id)'."
}

$PowerShellExe = Join-Path `
    ([Environment]::SystemDirectory) `
    'WindowsPowerShell\v1.0\powershell.exe'
if (-not (Test-Path -LiteralPath $PowerShellExe -PathType Leaf)) {
    throw "Canonical Windows PowerShell executable was not found: $PowerShellExe"
}
$TaskPrincipalSid = 'S-1-5-18'
$TaskPrincipal = New-ScheduledTaskPrincipal `
    -UserId $TaskPrincipalSid `
    -LogonType ServiceAccount `
    -RunLevel Highest
$StartupArguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$LaunchScript`" -EnableRutCanary"
$WatchdogArguments = "$StartupArguments -SkipClockSync"
$StartupAction = New-ScheduledTaskAction `
    -Execute $PowerShellExe `
    -Argument $StartupArguments `
    -WorkingDirectory $ProjectRoot
$WatchdogAction = New-ScheduledTaskAction `
    -Execute $PowerShellExe `
    -Argument $WatchdogArguments `
    -WorkingDirectory $ProjectRoot

# The elevated 07:00 trigger gives Windows Time and the current-day universe a
# dedicated preflight. A separate 07:15 retry gives transient provider/cache
# failures one bounded recovery opportunity before the 07:40 pre-stage cutoff.
# The launcher exits after pre-stage before 07:40, so neither trigger starts the
# application outside the existing watch window. Two explicit app-start attempts
# remain easy to audit. At-startup closes the no-logon gap after an unexpected
# reboot, while at-logon provides a separate interactive recovery opportunity.
$StartupTriggers = @(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:00'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:15'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:45'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '08:15'),
    (New-ScheduledTaskTrigger -AtLogOn -User $LogonRecoveryUser)
)

# Keep-alive checks are intentionally separate and never replay missed runs.
# The guarded launcher is idempotent and its own 07:45-18:00 CT window remains
# the final safety boundary.
$WatchdogTrigger = New-ScheduledTaskTrigger `
    -Weekly `
    -WeeksInterval 1 `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
    -At '07:50'
$WatchdogRepetition = New-CimInstance `
    -ClassName MSFT_TaskRepetitionPattern `
    -Namespace 'root/Microsoft/Windows/TaskScheduler' `
    -ClientOnly `
    -Property @{Interval='PT5M';Duration='PT10H10M';StopAtDurationEnd=$true}
$WatchdogTrigger.Repetition = $WatchdogRepetition

$StartupSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)
$WatchdogSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15)

function Resolve-MarketTaskIdentitySid {
    param([Parameter(Mandatory = $true)][string]$Identity)
    try {
        if ($Identity -match '^S-\d-(?:\d+-)+\d+$') {
            return ([Security.Principal.SecurityIdentifier]::new($Identity)).Value
        }
        return ([Security.Principal.NTAccount]::new($Identity)).Translate(
            [Security.Principal.SecurityIdentifier]
        ).Value
    }
    catch {
        return $null
    }
}

function Assert-MarketTaskReadback {
    param(
        [Parameter(Mandatory = $true)][string]$RegisteredTaskName,
        [Parameter(Mandatory = $true)][ValidateSet('Startup','Watchdog')][string]$Kind,
        [Parameter(Mandatory = $true)]$ExpectedAction
    )

    $matches = @(Get-ScheduledTask -TaskName $RegisteredTaskName -ErrorAction SilentlyContinue)
    $rootMatches = @($matches | Where-Object { [string]$_.TaskPath -ceq '\' })
    if ($matches.Count -ne 1 -or $rootMatches.Count -ne 1) {
        throw "Scheduled task identity did not read back uniquely at the root: $RegisteredTaskName"
    }
    $task = $rootMatches[0]
    $taskSid = Resolve-MarketTaskIdentitySid -Identity ([string]$task.Principal.UserId)
    if ($taskSid -cne $TaskPrincipalSid) { throw "Scheduled task principal SID mismatch: $RegisteredTaskName" }
    if ([string]$task.Principal.LogonType -cne 'ServiceAccount') { throw "Scheduled task logon type mismatch: $RegisteredTaskName" }
    if ([string]$task.Principal.RunLevel -cne 'Highest') { throw "Scheduled task run level mismatch: $RegisteredTaskName" }
    if (-not [bool]$task.Settings.Enabled) { throw "Scheduled task is disabled: $RegisteredTaskName" }

    $actions = @($task.Actions)
    if ($actions.Count -ne 1) { throw "Scheduled task action count mismatch: $RegisteredTaskName" }
    $actualAction = $actions[0]
    if (-not ([IO.Path]::GetFullPath([string]$actualAction.Execute) -ieq [IO.Path]::GetFullPath([string]$ExpectedAction.Execute))) {
        throw "Scheduled task executable mismatch: $RegisteredTaskName"
    }
    if ([string]$actualAction.Arguments -cne [string]$ExpectedAction.Arguments) {
        throw "Scheduled task arguments mismatch: $RegisteredTaskName"
    }
    if (-not ([IO.Path]::GetFullPath([string]$actualAction.WorkingDirectory).TrimEnd('\') -ieq $ProjectRoot.TrimEnd('\'))) {
        throw "Scheduled task working directory mismatch: $RegisteredTaskName"
    }
    if (
        [string]$task.Settings.MultipleInstances -cne 'IgnoreNew' -or
        [string]$task.Settings.ExecutionTimeLimit -cne 'PT15M' -or
        [bool]$task.Settings.DisallowStartIfOnBatteries -or
        [bool]$task.Settings.StopIfGoingOnBatteries -or
        -not [bool]$task.Settings.WakeToRun
    ) {
        throw "Scheduled task common settings mismatch: $RegisteredTaskName"
    }

    $triggers = @($task.Triggers)
    if ($Kind -eq 'Startup') {
        $weekly = @($triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskWeeklyTrigger' })
        $boot = @($triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskBootTrigger' })
        $logon = @($triggers | Where-Object { $_.CimClass.CimClassName -eq 'MSFT_TaskLogonTrigger' })
        $times = @($weekly | ForEach-Object { ([datetime]$_.StartBoundary).ToString('HH:mm') } | Sort-Object)
        if ($triggers.Count -ne 6 -or $weekly.Count -ne 4 -or $boot.Count -ne 1 -or $logon.Count -ne 1) { throw 'Startup trigger count or type mismatch.' }
        if (($times -join ',') -cne '07:00,07:15,07:45,08:15') { throw 'Startup trigger time mismatch.' }
        if (@($weekly | Where-Object {
            -not $_.Enabled -or
            [int]$_.DaysOfWeek -ne 62 -or
            [int]$_.WeeksInterval -ne 1 -or
            [string]$_.EndBoundary -ne '' -or
            [string]$_.RandomDelay -ne '' -or
            [string]$_.Repetition.Interval -ne '' -or
            [string]$_.Repetition.Duration -ne ''
        }).Count -ne 0) { throw 'Startup weekly trigger shape mismatch.' }
        if (
            -not $boot[0].Enabled -or
            [string]$boot[0].StartBoundary -ne '' -or
            [string]$boot[0].EndBoundary -ne '' -or
            [string]$boot[0].Delay -ne '' -or
            [string]$boot[0].Repetition.Interval -ne '' -or
            [string]$boot[0].Repetition.Duration -ne ''
        ) { throw 'Startup boot trigger mismatch.' }
        if (-not $logon[0].Enabled -or [string]$logon[0].UserId -cne $LogonRecoveryUser) { throw 'Startup logon trigger mismatch.' }
        if (-not [bool]$task.Settings.StartWhenAvailable -or [int]$task.Settings.RestartCount -ne 3 -or [string]$task.Settings.RestartInterval -cne 'PT1M') { throw 'Startup recovery settings mismatch.' }
    }
    else {
        if ($triggers.Count -ne 1 -or $triggers[0].CimClass.CimClassName -ne 'MSFT_TaskWeeklyTrigger') { throw 'Watchdog trigger count or type mismatch.' }
        $trigger = $triggers[0]
        if (
            -not $trigger.Enabled -or
            ([datetime]$trigger.StartBoundary).ToString('HH:mm') -cne '07:50' -or
            [int]$trigger.DaysOfWeek -ne 62 -or
            [int]$trigger.WeeksInterval -ne 1 -or
            [string]$trigger.Repetition.Interval -cne 'PT5M' -or
            [string]$trigger.Repetition.Duration -cne 'PT10H10M' -or
            -not [bool]$trigger.Repetition.StopAtDurationEnd
        ) { throw 'Watchdog trigger shape mismatch.' }
        if ([bool]$task.Settings.StartWhenAvailable -or [int]$task.Settings.RestartCount -ne 0) { throw 'Watchdog recovery settings mismatch.' }
    }
    return $task
}

# LocalSystem makes both tasks noninteractive and independent of user logon.
# The human account remains scoped only to the optional at-logon recovery
# trigger. The watchdog is registered first so a partial registration never
# removes the existing keep-alive path before startup readback is attempted.
if ($PSCmdlet.ShouldProcess($WatchdogTaskName, 'Register noninteractive MarketPinPredictor keep-alive task')) {
    Register-ScheduledTask `
        -TaskName $WatchdogTaskName `
        -Action $WatchdogAction `
        -Trigger $WatchdogTrigger `
        -Settings $WatchdogSettings `
        -Description 'Runs the noninteractive LocalSystem, ownership-guarded MarketPinPredictor recovery check every five minutes from 07:50 through 18:00 CT without replaying missed watchdog runs.' `
        -Principal $TaskPrincipal `
        -Force | Out-Null
}
if ($PSCmdlet.ShouldProcess($TaskName, 'Register noninteractive guarded MarketPinPredictor pre-open task')) {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $StartupAction `
        -Trigger $StartupTriggers `
        -Settings $StartupSettings `
        -Description 'Preflights Windows Time and the current-day universe at 07:00 CT, retries universe pre-stage at 07:15 CT, starts MarketPinPredictor noninteractively as LocalSystem at 07:45 and 08:15 CT on open market weekdays, and recovers after boot without requiring logon. The launcher is holiday-aware, time-bounded, and ownership-guarded.' `
        -Principal $TaskPrincipal `
        -Force | Out-Null
}

if ($WhatIfPreference) { return }

[void](Assert-MarketTaskReadback -RegisteredTaskName $WatchdogTaskName -Kind Watchdog -ExpectedAction $WatchdogAction)
[void](Assert-MarketTaskReadback -RegisteredTaskName $TaskName -Kind Startup -ExpectedAction $StartupAction)

foreach ($registeredName in @($TaskName, $WatchdogTaskName)) {
    $Task = Get-ScheduledTask -TaskName $registeredName -ErrorAction Stop
    $Info = Get-ScheduledTaskInfo -TaskName $registeredName
    [pscustomobject]@{
        TaskName = $Task.TaskName
        State = $Task.State
        Principal = $Task.Principal.UserId
        LogonType = $Task.Principal.LogonType
        RunLevel = $Task.Principal.RunLevel
        NextRunTime = $Info.NextRunTime
        LastRunTime = $Info.LastRunTime
        LastTaskResult = $Info.LastTaskResult
        TriggerCount = @($Task.Triggers).Count
        StartBoundaries = @($Task.Triggers | ForEach-Object StartBoundary) -join ','
        RepetitionInterval = [string]$Task.Triggers[0].Repetition.Interval
        RepetitionDuration = [string]$Task.Triggers[0].Repetition.Duration
        StartWhenAvailable = $Task.Settings.StartWhenAvailable
        WakeToRun = $Task.Settings.WakeToRun
        Action = "$($Task.Actions[0].Execute) $($Task.Actions[0].Arguments)"
    }
}
