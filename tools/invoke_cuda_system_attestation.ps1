[CmdletBinding()]
param(
    [string]$ProjectRoot,
    [string]$OutputPath,
    [ValidateRange(5, 120)][int]$TimeoutSeconds = 60
)

$ErrorActionPreference = 'Stop'
if (-not $ProjectRoot) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}
$root = [System.IO.Path]::GetFullPath($ProjectRoot)
$python = Join-Path $root '.venv\Scripts\python.exe'
$probe = Join-Path $root 'tools\cuda_runtime_probe.py'

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
$isAdministrator = $principal.IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)
if (-not $isAdministrator) {
    throw 'Administrator rights are required to create the disposable SYSTEM attestation task.'
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Canonical Python interpreter was not found: $python"
}
if (-not (Test-Path -LiteralPath $probe -PathType Leaf)) {
    throw "CUDA runtime probe was not found: $probe"
}

if (-not $OutputPath) {
    $dateDirectory = (Get-Date).ToString('yyyy-MM-dd')
    $stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmss.fffffffZ')
    $OutputPath = Join-Path $root (
        "exports\runtime_diagnostics\$dateDirectory\cuda_system_attestation.$stamp.json"
    )
}
$output = [System.IO.Path]::GetFullPath($OutputPath)
if (Test-Path -LiteralPath $output) {
    throw "Refusing to overwrite an existing CUDA attestation: $output"
}
New-Item -ItemType Directory -Path (Split-Path -Parent $output) -Force | Out-Null

$taskName = 'MarketPinPredictor_CudaProbe_' + [Guid]::NewGuid().ToString('N')
if ($taskName -notmatch '^MarketPinPredictor_CudaProbe_[0-9a-f]{32}$') {
    throw 'Failed to construct a bounded temporary task identity.'
}

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument ('"{0}" --expect-python "{1}" --output "{2}"' -f $probe, $python, $output) `
    -WorkingDirectory $root
$systemPrincipal = New-ScheduledTaskPrincipal `
    -UserId 'SYSTEM' `
    -LogonType ServiceAccount `
    -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::FromMinutes(2)) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

try {
    Register-ScheduledTask `
        -TaskName $taskName `
        -Action $action `
        -Principal $systemPrincipal `
        -Settings $settings `
        -Description 'Disposable MarketPin CUDA attestation; no service or provider startup.' | Out-Null
    Start-ScheduledTask -TaskName $taskName

    $deadlineUtc = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        Start-Sleep -Milliseconds 250
        $task = Get-ScheduledTask -TaskName $taskName
        $info = Get-ScheduledTaskInfo -TaskName $taskName
        $ran = (
            $info.LastRunTime -gt [datetime]'2000-01-01' -and
            $task.State -notin @('Queued', 'Running')
        )
    } while (-not $ran -and [DateTime]::UtcNow -lt $deadlineUtc)

    if (-not $ran) {
        throw "SYSTEM CUDA attestation exceeded the $TimeoutSeconds-second deadline."
    }
    if (-not (Test-Path -LiteralPath $output -PathType Leaf)) {
        throw "SYSTEM CUDA attestation returned task result $($info.LastTaskResult) without an output document."
    }

    $payload = Get-Content -LiteralPath $output -Raw | ConvertFrom-Json
    $attestedAccount = [string]$payload.account
    $attestedSessionId = $payload.windows_session_id
    if ($attestedAccount -notin @('SYSTEM', 'NT AUTHORITY\SYSTEM')) {
        throw "CUDA attestation did not run as SYSTEM (account=$attestedAccount)."
    }
    if ($null -eq $attestedSessionId -or [int]$attestedSessionId -ne 0) {
        throw "CUDA attestation did not run in Windows session 0 (session=$attestedSessionId)."
    }
    [pscustomobject]@{
        TaskResult = $info.LastTaskResult
        OutputPath = $output
        Status = $payload.status
        Account = $attestedAccount
        WindowsSessionId = [int]$attestedSessionId
        PythonExecutable = $payload.python_executable
        TorchVersion = $payload.torch_version
        CudaBuild = $payload.torch_cuda_build
        CudaAvailable = $payload.cuda_available
        KernelMatched = $payload.kernel_probe.matches_expected
    } | Format-List

    if ($info.LastTaskResult -ne 0 -or $payload.status -ne 'pass') {
        throw "SYSTEM CUDA attestation failed; evidence retained at $output"
    }
}
finally {
    # Query the exact randomized identity even if registration returned
    # ambiguously before `$registered` could be set.  Never leave a privileged
    # diagnostic task behind.
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($null -ne $task -and $task.TaskName -eq $taskName) {
        if ($task.State -in @('Queued', 'Running')) {
            Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        }
        Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    }

    $cleanupDeadlineUtc = [DateTime]::UtcNow.AddSeconds(5)
    do {
        $remainingTask = Get-ScheduledTask `
            -TaskName $taskName `
            -ErrorAction SilentlyContinue
        if ($null -eq $remainingTask) {
            break
        }
        Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $cleanupDeadlineUtc)
    if ($null -ne $remainingTask) {
        throw "Disposable SYSTEM CUDA attestation task was not removed: $taskName"
    }
}
