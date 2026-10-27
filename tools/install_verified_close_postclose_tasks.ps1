param(
    [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$runner = Join-Path $projectRoot 'tools\run_verified_close_postclose.ps1'
$selectionTask = 'MarketPin-VerifiedClose-Selection'
$settlementTask = 'MarketPin-VerifiedClose-Settlement'

if ($Uninstall) {
    foreach ($name in @($selectionTask, $settlementTask)) {
        if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $name -Confirm:$false
        }
    }
    return
}

if (-not (Test-Path -LiteralPath $runner -PathType Leaf)) {
    throw "Verified-close task runner was not found: $runner"
}

$days = @('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
$powerShell = (Get-Command powershell.exe).Source
$selectionSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20)
$settlementSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20)
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Limited

$selectionAction = New-ScheduledTaskAction `
    -Execute $powerShell `
    -Argument ("-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"{0}`" -Mode select" -f $runner)
$selectionTrigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '3:05 PM'
$selectionDefinition = New-ScheduledTask `
    -Action $selectionAction `
    -Trigger $selectionTrigger `
    -Principal $principal `
    -Settings $selectionSettings `
    -Description 'Freeze the deterministic MarketPin close-prediction selection after cash close and before official-close publication.'

$settlementAction = New-ScheduledTaskAction `
    -Execute $powerShell `
    -Argument ("-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"{0}`" -Mode catchup" -f $runner)
$settlementTriggers = @(
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '6:30 AM'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '7:45 AM'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '8:15 AM'),
    (New-ScheduledTaskTrigger -AtLogOn -User ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '5:05 PM'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '5:20 PM'),
    (New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 -DaysOfWeek $days -At '5:45 PM')
)
$settlementDefinition = New-ScheduledTask `
    -Action $settlementAction `
    -Trigger $settlementTriggers `
    -Principal $principal `
    -Settings $settlementSettings `
    -Description 'Settle at most one pending frozen selection from the last seven days; morning catch-up handles delayed official publication without rewriting prior forecasts.'

Register-ScheduledTask -TaskName $selectionTask -InputObject $selectionDefinition -Force | Out-Null
Register-ScheduledTask -TaskName $settlementTask -InputObject $settlementDefinition -Force | Out-Null

Get-ScheduledTask -TaskName $selectionTask, $settlementTask |
    Select-Object TaskName, State
