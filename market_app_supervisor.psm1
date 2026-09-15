Set-StrictMode -Version Latest

function Get-MarketAppSupervisorMutexName {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot
    )

    $normalizedRoot = [System.IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\', '/').ToUpperInvariant()
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($normalizedRoot))
    }
    finally {
        $sha256.Dispose()
    }
    $token = -join ($hash[0..11] | ForEach-Object { $_.ToString('x2') })
    return "Global\MarketPinPredictor.Supervisor.$token"
}

function Enter-MarketAppSupervisorLock {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [ValidateRange(0, 300000)][int]$TimeoutMilliseconds = 0
    )

    $name = Get-MarketAppSupervisorMutexName -ProjectRoot $ProjectRoot
    $mutex = [System.Threading.Mutex]::new($false, $name)
    $acquired = $false
    try {
        try {
            $acquired = $mutex.WaitOne($TimeoutMilliseconds)
        }
        catch [System.Threading.AbandonedMutexException] {
            # The previous owner exited without releasing the mutex. Ownership
            # transfers to this caller, so recovery can continue safely.
            $acquired = $true
        }

        return [pscustomobject]@{
            Name = $name
            Mutex = $mutex
            Acquired = [bool]$acquired
        }
    }
    catch {
        $mutex.Dispose()
        throw
    }
}

function Exit-MarketAppSupervisorLock {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][object]$LockHandle
    )

    try {
        if ($LockHandle.Acquired) {
            $LockHandle.Mutex.ReleaseMutex()
            $LockHandle.Acquired = $false
        }
    }
    finally {
        $LockHandle.Mutex.Dispose()
    }
}

function ConvertTo-MarketAppLogToken {
    param([AllowNull()][string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return 'unspecified'
    }
    return ($Value -replace '[^A-Za-z0-9_.:@-]', '_')
}

function Write-MarketAppSupervisorLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$InvocationId,
        [Parameter(Mandatory = $true)][string]$Caller,
        [Parameter(Mandatory = $true)][string]$Event,
        [Parameter(Mandatory = $true)][string]$Message
    )

    $runtimeLogDir = Join-Path ([System.IO.Path]::GetFullPath($ProjectRoot)) 'logs\runtime'
    New-Item -ItemType Directory -Path $runtimeLogDir -Force | Out-Null
    $watchdogLog = Join-Path $runtimeLogDir 'watchdog.log'
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz'
    $safeInvocation = ConvertTo-MarketAppLogToken $InvocationId
    $safeCaller = ConvertTo-MarketAppLogToken $Caller
    $safeEvent = ConvertTo-MarketAppLogToken $Event
    Add-Content -LiteralPath $watchdogLog -Value (
        "$timestamp invocation=$safeInvocation caller=$safeCaller powershell_pid=$PID event=$safeEvent $Message"
    )
}

function Get-MarketAppSessionRecoveryJournalState {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)]
        [ValidateSet('backend', 'dashboard')]
        [string]$Component,
        [Parameter(Mandatory = $true)]
        [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
        [string]$TradingDate
    )

    $journalPath = Join-Path ([System.IO.Path]::GetFullPath($ProjectRoot)) 'logs\runtime\watchdog.log'
    if (-not (Test-Path -LiteralPath $journalPath -PathType Leaf)) {
        return [pscustomobject]@{
            Readable = $true
            Requested = $false
            Succeeded = $false
        }
    }

    $requested = $false
    $succeeded = $false
    try {
        # Scan lazily from the append-only journal so an unusually noisy day
        # cannot evict the success latch from a fixed tail window.
        foreach ($line in [System.IO.File]::ReadLines($journalPath)) {
            $text = [string]$line
            if (-not $text.Contains("trading_date=$TradingDate")) {
                continue
            }
            if ($text.Contains("event=${Component}_session_recovery_requested")) {
                $requested = $true
            }
            if ($text.Contains("event=${Component}_session_recovery_succeeded")) {
                $succeeded = $true
            }
        }
    }
    catch {
        # An unreadable recovery journal cannot prove that another automatic
        # restart is safe. Fail closed as already succeeded, not as pending.
        return [pscustomobject]@{
            Readable = $false
            Requested = $false
            Succeeded = $true
        }
    }
    return [pscustomobject]@{
        Readable = $true
        Requested = [bool]$requested
        Succeeded = [bool]$succeeded
    }
}

function New-MarketAppLaunchLogPaths {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z0-9_.-]+$')][string]$Component,
        [Parameter(Mandatory = $true)][string]$InvocationId,
        [datetime]$NowUtc = [DateTime]::UtcNow
    )

    $normalizedRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
    $runtimeLogDir = Join-Path $normalizedRoot 'logs\runtime'
    $launchDay = $NowUtc.ToUniversalTime().ToString('yyyy-MM-dd')
    $launchLogDir = Join-Path $runtimeLogDir ("launches\$launchDay")
    New-Item -ItemType Directory -Path $launchLogDir -Force | Out-Null

    $safeComponent = ConvertTo-MarketAppLogToken $Component
    $safeInvocation = ConvertTo-MarketAppLogToken $InvocationId
    $launchStamp = $NowUtc.ToUniversalTime().ToString('yyyyMMddTHHmmss.fffffffZ')
    $launchBase = "$safeComponent.$launchStamp.$safeInvocation"
    $launchStdout = Join-Path $launchLogDir "$launchBase.stdout.log"
    $launchStderr = Join-Path $launchLogDir "$launchBase.stderr.log"
    $stableStdout = Join-Path $runtimeLogDir "$safeComponent.stdout.log"
    $stableStderr = Join-Path $runtimeLogDir "$safeComponent.stderr.log"

    # Start-Process truncates redirect targets. Each launch therefore writes to
    # a new timestamped file. Stable hard links keep the existing operator and
    # inspection paths useful without sacrificing prior-session evidence.
    New-Item -ItemType File -Path $launchStdout -ErrorAction Stop | Out-Null
    New-Item -ItemType File -Path $launchStderr -ErrorAction Stop | Out-Null

    $rollovers = @()
    $createdStableLinks = @()
    $lockedStreams = @{}
    try {
        foreach ($entry in @(
            @{ Stable = $stableStdout; Stream = 'stdout' },
            @{ Stable = $stableStderr; Stream = 'stderr' }
        )) {
            if (Test-Path -LiteralPath $entry.Stable -PathType Leaf) {
                $rolloverPath = Join-Path $launchLogDir (
                    "$safeComponent.rollover.$launchStamp.$safeInvocation.$($entry.Stream).log"
                )
                try {
                    Move-Item -LiteralPath $entry.Stable -Destination $rolloverPath -ErrorAction Stop
                }
                catch {
                    if ($_.Exception -isnot [System.IO.IOException]) {
                        throw
                    }
                    # A recently stopped Windows process can retain a log handle
                    # briefly. Keep that stable link untouched and redirect this
                    # launch straight to its immutable retained file.
                    $lockedStreams[$entry.Stream] = $true
                    continue
                }
                $rollovers += [pscustomobject]@{
                    Stable = $entry.Stable
                    Rollover = $rolloverPath
                }
            }
        }

        if (-not $lockedStreams.ContainsKey('stdout')) {
            New-Item -ItemType HardLink -Path $stableStdout -Target $launchStdout -ErrorAction Stop | Out-Null
            $createdStableLinks += $stableStdout
        }
        if (-not $lockedStreams.ContainsKey('stderr')) {
            New-Item -ItemType HardLink -Path $stableStderr -Target $launchStderr -ErrorAction Stop | Out-Null
            $createdStableLinks += $stableStderr
        }
    }
    catch {
        foreach ($stableLink in $createdStableLinks) {
            Remove-Item -LiteralPath $stableLink -Force -ErrorAction SilentlyContinue
        }
        foreach ($rollover in $rollovers) {
            if (Test-Path -LiteralPath $rollover.Rollover -PathType Leaf) {
                Move-Item -LiteralPath $rollover.Rollover -Destination $rollover.Stable -ErrorAction SilentlyContinue
            }
        }
        Remove-Item -LiteralPath $launchStdout -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $launchStderr -Force -ErrorAction SilentlyContinue
        throw
    }

    return [pscustomobject]@{
        Component = $safeComponent
        LaunchStampUtc = $NowUtc.ToUniversalTime().ToString('o')
        StandardOutputPath = if ($lockedStreams.ContainsKey('stdout')) { $launchStdout } else { $stableStdout }
        StandardErrorPath = if ($lockedStreams.ContainsKey('stderr')) { $launchStderr } else { $stableStderr }
        RetainedStandardOutputPath = $launchStdout
        RetainedStandardErrorPath = $launchStderr
        StableStdoutAvailable = -not $lockedStreams.ContainsKey('stdout')
        StableStderrAvailable = -not $lockedStreams.ContainsKey('stderr')
    }
}

function Resolve-MarketAppPostCloseFinalizeResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Output,
        [Parameter(Mandatory = $true)][int]$ExitCode
    )

    $text = ($Output | ForEach-Object { [string]$_ }) -join [Environment]::NewLine
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "Post-close tape finalizer returned no output (exit code $ExitCode)."
    }
    try {
        $result = $text | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "Post-close tape finalizer returned unreadable output (exit code $ExitCode)."
    }
    if ($null -eq $result) {
        throw "Post-close tape finalizer returned no result (exit code $ExitCode)."
    }
    $action = [string]$result.action
    if ([string]::IsNullOrWhiteSpace($action)) {
        throw "Post-close tape finalizer omitted its action (exit code $ExitCode)."
    }

    $expectedIncomplete = $ExitCode -eq 2 -and $action -eq 'incomplete'
    if ($ExitCode -ne 0 -and -not $expectedIncomplete) {
        throw "Post-close tape finalizer failed with exit code $ExitCode (action=$action)."
    }
    return [pscustomobject]@{
        Result = $result
        ExitCode = $ExitCode
        ExpectedIncomplete = [bool]$expectedIncomplete
    }
}

function Resolve-MarketAppUniverseCachePreparationResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Output,
        [Parameter(Mandatory = $true)][int]$ExitCode,
        [Parameter(Mandatory = $true)]
        [ValidateSet('provider_allowed', 'cache_only')]
        [string]$ExpectedPreparationMode
    )

    $text = ($Output | ForEach-Object { [string]$_ }) -join [Environment]::NewLine
    try {
        $result = $text | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "Databento universe preparation returned unreadable output (exit code $ExitCode)."
    }
    if ($ExitCode -ne 0 -or [string]$result.status -eq 'failed') {
        $failure = if ([string]::IsNullOrWhiteSpace([string]$result.error)) {
            'no launch-safe universe was produced'
        }
        else {
            [string]$result.error
        }
        throw "Databento universe preparation failed (exit code $ExitCode): $failure"
    }

    $modeProperty = $result.PSObject.Properties['preparation_mode']
    $preparationMode = if ($null -eq $modeProperty) { '' } else { [string]$modeProperty.Value }
    if ($preparationMode -ne $ExpectedPreparationMode) {
        throw (
            "Databento universe preparation mode mismatch: expected " +
            "$ExpectedPreparationMode, observed $preparationMode."
        )
    }

    $label = [string]$result.provenance_label
    $isFallback = [bool]$result.provenance.is_fallback
    if ([bool]$result.current_day_cache_ready) {
        if ($label -ne 'CURRENT_DAY_CACHE' -or $isFallback) {
            throw 'Databento universe preparation returned contradictory current-day provenance.'
        }
    }
    elseif ($label -ne 'PRIOR_SESSION_FALLBACK' -or -not $isFallback -or
        [string]$result.provenance.mode -ne 'prior_cache_filtered') {
        throw 'Databento universe preparation did not explicitly label its prior-session fallback.'
    }

    return [pscustomobject]@{
        Result = $result
        UsesFallback = [bool]$isFallback
        ProvenanceLabel = $label
        PreparationMode = $preparationMode
    }
}

function Get-MarketAppUniversePreparationDeadline {
    [CmdletBinding()]
    param(
        [datetime]$Now = (Get-Date),
        [ValidateRange(15, 300)][int]$MaximumDurationSeconds = 285,
        [datetime]$NotAfter
    )

    $deadline = $Now.AddSeconds($MaximumDurationSeconds)
    if ($PSBoundParameters.ContainsKey('NotAfter') -and $NotAfter -lt $deadline) {
        $deadline = $NotAfter
    }
    return $deadline
}

function Test-MarketAppUniverseProviderDiscoveryAllowed {
    [CmdletBinding()]
    param(
        [datetime]$Now = (Get-Date),
        [datetime]$OpeningProtectionBoundary = $Now.Date.AddHours(8).AddMinutes(25),
        [ValidateRange(1, 240)][int]$ProviderTimeoutSeconds = 240,
        [ValidateRange(1, 60)][int]$CacheValidationTimeoutSeconds = 30,
        [ValidateRange(30, 180)][int]$BackendLaunchReserveSeconds = 90
    )

    if ($Now -ge $OpeningProtectionBoundary) {
        return $false
    }
    # Reserve the complete preparation cap, including child startup overhead.
    # Explicit larger child budgets may require more; smaller overrides cannot
    # silently spend the launch reserve left by the shared preparation deadline.
    $fullPreparationSeconds = (
        (Get-MarketAppUniversePreparationDeadline -Now $Now) - $Now
    ).TotalSeconds
    $preparationBudgetSeconds = [Math]::Max(
        ($ProviderTimeoutSeconds + $CacheValidationTimeoutSeconds + 6),
        $fullPreparationSeconds
    )
    $requiredSeconds = $preparationBudgetSeconds + $BackendLaunchReserveSeconds
    return (($OpeningProtectionBoundary - $Now).TotalSeconds -ge $requiredSeconds)
}

function Get-MarketAppUniverseCacheStateToken {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $cacheDir = Join-Path ([System.IO.Path]::GetFullPath($ProjectRoot)) 'data\databento_cache'
    if (-not (Test-Path -LiteralPath $cacheDir -PathType Container)) {
        return 'CACHE_DIRECTORY_ABSENT'
    }
    $states = foreach ($file in @(
        Get-ChildItem -LiteralPath $cacheDir -File -Filter 'opra_universe_*' |
            Sort-Object FullName
    )) {
        $before = Get-Item -LiteralPath $file.FullName -ErrorAction Stop
        $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256 -ErrorAction Stop).Hash
        $after = Get-Item -LiteralPath $file.FullName -ErrorAction Stop
        if ($before.Length -ne $after.Length -or
            $before.LastWriteTimeUtc.Ticks -ne $after.LastWriteTimeUtc.Ticks) {
            throw "Universe cache changed while hashing: $($file.FullName)"
        }
        "$($after.FullName)|$($after.Length)|$($after.LastWriteTimeUtc.Ticks)|$hash"
    }
    return (@($states) -join [Environment]::NewLine)
}

function ConvertTo-MarketAppUniverseProcessCreationUtc {
    param([Parameter(Mandatory = $true)][object]$CreationDate)

    try {
        if ($CreationDate -is [datetime]) {
            return ([datetime]$CreationDate).ToUniversalTime()
        }
        return [System.Management.ManagementDateTimeConverter]::ToDateTime(
            [string]$CreationDate
        ).ToUniversalTime()
    }
    catch {
        return $null
    }
}

function New-MarketAppUniverseProcessIdentity {
    param([Parameter(Mandatory = $true)][psobject]$Record)

    $creationTimeUtc = ConvertTo-MarketAppUniverseProcessCreationUtc $Record.CreationDate
    if ($null -eq $creationTimeUtc) {
        return $null
    }
    return [pscustomobject]@{
        ProcessId = [int]$Record.ProcessId
        ParentProcessId = [int]$Record.ParentProcessId
        CreationTimeUtc = [datetime]$creationTimeUtc
        ExecutablePath = [string]$Record.ExecutablePath
        CommandLine = [string]$Record.CommandLine
    }
}

function Test-MarketAppUniverseProcessIdentity {
    param(
        [Parameter(Mandatory = $true)][psobject]$Record,
        [Parameter(Mandatory = $true)][psobject]$Identity
    )

    $current = New-MarketAppUniverseProcessIdentity -Record $Record
    if ($null -eq $current) {
        return $false
    }
    return [bool](
        [int]$current.ProcessId -eq [int]$Identity.ProcessId -and
        [int]$current.ParentProcessId -eq [int]$Identity.ParentProcessId -and
        [datetime]$current.CreationTimeUtc -eq [datetime]$Identity.CreationTimeUtc -and
        [string]::Equals(
            [string]$current.ExecutablePath,
            [string]$Identity.ExecutablePath,
            [StringComparison]::OrdinalIgnoreCase
        ) -and
        [string]$current.CommandLine -ceq [string]$Identity.CommandLine
    )
}

function Get-MarketAppUniversePreparationTreeSnapshot {
    param([Parameter(Mandatory = $true)][int]$RootProcessId)

    $allProcesses = @(Get-CimInstance Win32_Process -ErrorAction Stop)
    $rootRecords = @($allProcesses | Where-Object { [int]$_.ProcessId -eq $RootProcessId })
    if ($rootRecords.Count -ne 1) {
        throw "Universe preparation root PID $RootProcessId is absent or ambiguous."
    }

    $queue = New-Object 'System.Collections.Generic.Queue[int]'
    $queue.Enqueue($RootProcessId)
    $seen = @{}
    $identities = @()
    while ($queue.Count -gt 0) {
        $queuedProcessId = [int]$queue.Dequeue()
        $records = @($allProcesses | Where-Object {
            [int]$_.ProcessId -eq $queuedProcessId
        })
        if ($records.Count -ne 1) {
            throw "Universe preparation tree PID $queuedProcessId is absent or ambiguous."
        }
        foreach ($record in @($records)) {
            $processId = [int]$record.ProcessId
            if ($seen.ContainsKey($processId)) {
                continue
            }
            $identity = New-MarketAppUniverseProcessIdentity -Record $record
            if ($null -eq $identity) {
                throw "Universe preparation PID $processId has no verifiable creation identity."
            }
            $seen[$processId] = $true
            $identities += $identity
            foreach ($child in @($allProcesses | Where-Object {
                [int]$_.ParentProcessId -eq $processId
            })) {
                if (-not $seen.ContainsKey([int]$child.ProcessId)) {
                    $queue.Enqueue([int]$child.ProcessId)
                }
            }
        }
    }
    return @($identities)
}

function Stop-MarketAppUniversePreparationTree {
    param(
        [Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process,
        [Parameter(Mandatory = $true)][string]$ExpectedExecutablePath,
        [Parameter(Mandatory = $true)][string[]]$RequiredCommandMarkers,
        [Parameter(Mandatory = $true)][datetime]$DeadlineUtc
    )

    try {
        $Process.Refresh()
        if ($Process.HasExited -or [DateTime]::UtcNow -ge $DeadlineUtc) {
            return $false
        }
        $captured = @(Get-MarketAppUniversePreparationTreeSnapshot `
            -RootProcessId ([int]$Process.Id))
        $rootIdentities = @($captured | Where-Object {
            [int]$_.ProcessId -eq [int]$Process.Id
        })
        if ($rootIdentities.Count -ne 1) {
            return $false
        }
        $rootIdentity = $rootIdentities[0]
        $handleStartUtc = $Process.StartTime.ToUniversalTime()
        if ([Math]::Abs((([datetime]$rootIdentity.CreationTimeUtc) - $handleStartUtc).TotalSeconds) -gt 2 -or
            -not [string]::Equals(
                [string]$rootIdentity.ExecutablePath,
                [string]$ExpectedExecutablePath,
                [StringComparison]::OrdinalIgnoreCase
            )) {
            return $false
        }
        foreach ($marker in @($RequiredCommandMarkers)) {
            if ([string]::IsNullOrWhiteSpace($marker) -or
                ([string]$rootIdentity.CommandLine).IndexOf(
                    $marker,
                    [StringComparison]::OrdinalIgnoreCase
                ) -lt 0) {
                return $false
            }
        }

        # Revalidate the exact root immediately before the only tree-wide stop.
        $currentRoot = Get-CimInstance Win32_Process `
            -Filter "ProcessId = $([int]$Process.Id)" `
            -ErrorAction Stop
        if ($null -eq $currentRoot -or -not (
            Test-MarketAppUniverseProcessIdentity -Record $currentRoot -Identity $rootIdentity
        )) {
            return $false
        }
        $Process.Refresh()
        if ($Process.HasExited -or [DateTime]::UtcNow -ge $DeadlineUtc) {
            return $false
        }

        $taskkillPath = Join-Path $env:SystemRoot 'System32\taskkill.exe'
        if (-not (Test-Path -LiteralPath $taskkillPath -PathType Leaf)) {
            return $false
        }
        $killer = Start-Process `
            -FilePath $taskkillPath `
            -ArgumentList @('/PID', [string]$Process.Id, '/T', '/F') `
            -WindowStyle Hidden `
            -PassThru
        $killerBudget = [Math]::Min(
            2000,
            [Math]::Max(1, [Math]::Floor(($DeadlineUtc - [DateTime]::UtcNow).TotalMilliseconds))
        )
        if (-not $killer.WaitForExit([int]$killerBudget)) {
            try { $killer.Kill() } catch {}
            [void]$killer.WaitForExit(500)
            return $false
        }

        $rootBudget = [Math]::Min(
            2000,
            [Math]::Max(1, [Math]::Floor(($DeadlineUtc - [DateTime]::UtcNow).TotalMilliseconds))
        )
        if (-not $Process.WaitForExit([int]$rootBudget)) {
            return $false
        }
        $Process.Refresh()
        if (-not $Process.HasExited) {
            return $false
        }

        # A reused PID is never stopped. Only an original captured identity
        # still present here is a surviving member of the preparer tree.
        foreach ($identity in @($captured)) {
            $current = Get-CimInstance Win32_Process `
                -Filter "ProcessId = $([int]$identity.ProcessId)" `
                -ErrorAction Stop
            if ($null -ne $current -and (
                Test-MarketAppUniverseProcessIdentity -Record $current -Identity $identity
            )) {
                return $false
            }
        }
        return [DateTime]::UtcNow -lt $DeadlineUtc
    }
    catch {
        return $false
    }
}

function Test-MarketAppUniversePreparationQuiescent {
    param(
        [Parameter(Mandatory = $true)][System.Diagnostics.Process]$Process,
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][datetime]$DeadlineUtc,
        [ValidateRange(25, 2000)][int]$ObservationMilliseconds = 250
    )

    try {
        $Process.Refresh()
        if (-not $Process.HasExited -or [DateTime]::UtcNow -ge $DeadlineUtc) {
            return $false
        }
        # A CIM failure is not evidence of an empty descendant set.  Enumerate
        # with Stop semantics here even though the generic cleanup helper is
        # intentionally best-effort for other callers.
        $descendantExitDeadline = [DateTime]::UtcNow.AddSeconds(1)
        if ($DeadlineUtc -lt $descendantExitDeadline) {
            $descendantExitDeadline = $DeadlineUtc
        }
        do {
            $allProcesses = @(Get-CimInstance Win32_Process -ErrorAction Stop)
            $pending = New-Object 'System.Collections.Generic.Queue[int]'
            $pending.Enqueue([int]$Process.Id)
            $seen = @{}
            while ($pending.Count -gt 0) {
                $parentId = [int]$pending.Dequeue()
                foreach ($child in @($allProcesses | Where-Object { [int]$_.ParentProcessId -eq $parentId })) {
                    $childId = [int]$child.ProcessId
                    if (-not $seen.ContainsKey($childId)) {
                        $seen[$childId] = $true
                        $pending.Enqueue($childId)
                    }
                }
            }
            if ($seen.Count -eq 0) {
                break
            }
            Start-Sleep -Milliseconds 50
        } while ([DateTime]::UtcNow -lt $descendantExitDeadline)
        if ($seen.Count -ne 0 -or [DateTime]::UtcNow -ge $DeadlineUtc) {
            return $false
        }
        $before = Get-MarketAppUniverseCacheStateToken -ProjectRoot $ProjectRoot
        if ([DateTime]::UtcNow.AddMilliseconds($ObservationMilliseconds) -ge $DeadlineUtc) {
            return $false
        }
        Start-Sleep -Milliseconds $ObservationMilliseconds
        $Process.Refresh()
        if (-not $Process.HasExited -or [DateTime]::UtcNow -ge $DeadlineUtc) {
            return $false
        }
        $after = Get-MarketAppUniverseCacheStateToken -ProjectRoot $ProjectRoot
        return $before -ceq $after -and [DateTime]::UtcNow -lt $DeadlineUtc
    }
    catch {
        Write-Verbose (
            "Universe preparation quiescence verification failed closed: " +
            $_.Exception.GetType().Name + ': ' + $_.Exception.Message
        )
        return $false
    }
}

function Invoke-MarketAppBoundedChildProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$InvocationId,
        [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z0-9_.-]+$')][string]$LogComponent,
        [Parameter(Mandatory = $true)][ValidateRange(1, 300)][int]$TimeoutSeconds
    )

    $root = [System.IO.Path]::GetFullPath($ProjectRoot)
    $executable = if (Test-Path -LiteralPath $FilePath -PathType Leaf) {
        [System.IO.Path]::GetFullPath($FilePath)
    }
    else {
        [string](Get-Command -Name $FilePath -CommandType Application -ErrorAction Stop).Source
    }
    $logs = New-MarketAppLaunchLogPaths `
        -ProjectRoot $root `
        -Component $LogComponent `
        -InvocationId $InvocationId
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $exitCode = $null
    try {
        $process = Start-Process `
            -FilePath $executable `
            -ArgumentList $ArgumentList `
            -WorkingDirectory $root `
            -RedirectStandardOutput $logs.StandardOutputPath `
            -RedirectStandardError $logs.StandardErrorPath `
            -WindowStyle Hidden `
            -PassThru

        # Windows PowerShell 5.1 opens this handle lazily. Force it while the
        # child is alive so ExitCode remains available after a fast exit.
        $null = $process.Handle

        $completed = $process.WaitForExit($TimeoutSeconds * 1000)
        $timedOut = -not $completed
        $terminationAttempted = $false
        $terminationConfirmed = [bool]$completed
        if ($timedOut) {
            $terminationAttempted = $true
            try {
                $process.Refresh()
                if (-not $process.HasExited) {
                    # Kill through the exact Process handle returned by
                    # Start-Process. Never enumerate or terminate another PID.
                    $process.Kill()
                }
                $terminationConfirmed = $process.WaitForExit(5000)
                if (-not $terminationConfirmed) {
                    $process.Refresh()
                    $terminationConfirmed = [bool]$process.HasExited
                }
            }
            catch {
                $terminationConfirmed = $false
            }
        }
        elseif ($completed) {
            # The parameterless wait flushes redirected output before it is
            # read from the retained launch files.
            $process.WaitForExit()
            $exitCode = $process.ExitCode
            $process.Refresh()
        }
    }
    finally {
        $stopwatch.Stop()
    }

    return [pscustomobject]@{
        Completed = [bool]$completed
        TimedOut = [bool]$timedOut
        TerminationAttempted = [bool]$terminationAttempted
        TerminationConfirmed = [bool]$terminationConfirmed
        ProcessId = [int]$process.Id
        # Preserve null instead of silently converting unavailable evidence to
        # exit code 0. Callers must fail closed when no exit code was captured.
        ExitCode = $exitCode
        ElapsedMilliseconds = [int64]$stopwatch.ElapsedMilliseconds
        Output = @(
            if ($terminationConfirmed -and (Test-Path -LiteralPath $logs.RetainedStandardOutputPath)) {
                Get-Content -LiteralPath $logs.RetainedStandardOutputPath -ErrorAction SilentlyContinue
            }
        )
        StandardError = @(
            if ($terminationConfirmed -and (Test-Path -LiteralPath $logs.RetainedStandardErrorPath)) {
                Get-Content -LiteralPath $logs.RetainedStandardErrorPath -ErrorAction SilentlyContinue
            }
        )
        RetainedStandardOutputPath = $logs.RetainedStandardOutputPath
        RetainedStandardErrorPath = $logs.RetainedStandardErrorPath
    }
}

function Invoke-MarketAppUniversePreparationChild {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][string]$ToolPath,
        [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z0-9_,]+$')][string]$Symbols,
        [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
        [ValidateScript({
            try {
                $null = [datetime]::ParseExact(
                    $_,
                    'yyyy-MM-dd',
                    [System.Globalization.CultureInfo]::InvariantCulture
                )
                return $true
            }
            catch {
                return $false
            }
        })]
        [string]$TradingDate,
        [Parameter(Mandatory = $true)][string]$InvocationId,
        [Parameter(Mandatory = $true)][ValidateRange(1, 300000)][int]$TimeoutMilliseconds,
        [Parameter(Mandatory = $true)][datetime]$DeadlineUtc,
        [Parameter(Mandatory = $true)][string]$LogComponent,
        [switch]$CacheOnly
    )

    $root = [System.IO.Path]::GetFullPath($ProjectRoot)
    $python = if (Test-Path -LiteralPath $PythonExe -PathType Leaf) {
        [System.IO.Path]::GetFullPath($PythonExe)
    }
    else {
        [string](Get-Command -Name $PythonExe -CommandType Application -ErrorAction Stop).Source
    }
    $logs = New-MarketAppLaunchLogPaths `
        -ProjectRoot $root `
        -Component $LogComponent `
        -InvocationId $InvocationId
    $arguments = @('"' + $ToolPath + '"', '--symbols', $Symbols)
    if ($PSBoundParameters.ContainsKey('TradingDate')) {
        $arguments += @('--trading-date', $TradingDate)
    }
    if ($CacheOnly) {
        $arguments += '--cache-only'
    }
    $process = Start-Process `
        -FilePath $python `
        -ArgumentList $arguments `
        -WorkingDirectory $root `
        -RedirectStandardOutput $logs.StandardOutputPath `
        -RedirectStandardError $logs.StandardErrorPath `
        -WindowStyle Hidden `
        -PassThru

    $timedOut = -not $process.WaitForExit($TimeoutMilliseconds)
    $terminationObserved = $true
    if ($timedOut) {
        $requiredMarkers = @($ToolPath, '--symbols', $Symbols)
        if ($PSBoundParameters.ContainsKey('TradingDate')) {
            $requiredMarkers += @('--trading-date', $TradingDate)
        }
        if ($CacheOnly) {
            $requiredMarkers += '--cache-only'
        }
        $terminationObserved = Stop-MarketAppUniversePreparationTree `
            -Process $process `
            -ExpectedExecutablePath $python `
            -RequiredCommandMarkers $requiredMarkers `
            -DeadlineUtc $DeadlineUtc
    }
    if ($terminationObserved) {
        $process.WaitForExit()
        $process.Refresh()
    }
    $quiescent = $terminationObserved -and (
        Test-MarketAppUniversePreparationQuiescent `
            -Process $process `
            -ProjectRoot $root `
            -DeadlineUtc $DeadlineUtc
    )
    return [pscustomobject]@{
        Completed = -not $timedOut
        TimedOut = $timedOut
        ExitedAndQuiescent = [bool]$quiescent
        ProcessId = [int]$process.Id
        ExitCode = if (-not $timedOut -and $terminationObserved) { [int]$process.ExitCode } else { $null }
        Output = @(
            if ($terminationObserved -and (Test-Path -LiteralPath $logs.RetainedStandardOutputPath)) {
                Get-Content -LiteralPath $logs.RetainedStandardOutputPath
            }
        )
        StandardError = @(
            if ($terminationObserved -and (Test-Path -LiteralPath $logs.RetainedStandardErrorPath)) {
                Get-Content -LiteralPath $logs.RetainedStandardErrorPath
            }
        )
        RetainedStandardOutputPath = $logs.RetainedStandardOutputPath
        RetainedStandardErrorPath = $logs.RetainedStandardErrorPath
    }
}

function Invoke-MarketAppUniverseCachePreparation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][ValidatePattern('^[A-Za-z0-9_,]+$')][string]$Symbols,
        [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
        [ValidateScript({
            try {
                $null = [datetime]::ParseExact(
                    $_,
                    'yyyy-MM-dd',
                    [System.Globalization.CultureInfo]::InvariantCulture
                )
                return $true
            }
            catch {
                return $false
            }
        })]
        [string]$TradingDate,
        [ValidateRange(1, 240)][int]$ProviderTimeoutSeconds = 240,
        [ValidateRange(1, 60)][int]$CacheValidationTimeoutSeconds = 30,
        [Parameter(Mandatory = $true)][datetime]$Deadline,
        [string]$InvocationId = ([guid]::NewGuid().ToString('N')),
        [switch]$SkipProviderDiscovery
    )

    $toolPath = Join-Path ([System.IO.Path]::GetFullPath($ProjectRoot)) `
        'tools\prepare_databento_universe_cache.py'
    if (-not (Test-Path -LiteralPath $toolPath -PathType Leaf)) {
        throw "Databento universe preparation tool was not found: $toolPath"
    }
    $deadlineUtc = $Deadline.ToUniversalTime()
    $remainingMilliseconds = [Math]::Floor(
        ($deadlineUtc - [DateTime]::UtcNow).TotalMilliseconds
    )
    # Reserve cache-only validation plus bounded termination/quiescence for
    # both children.  Provider discovery is skipped when that proof budget is
    # no longer available.
    $providerBudget = [Math]::Min(
        $ProviderTimeoutSeconds * 1000,
        [Math]::Max(0, $remainingMilliseconds - (($CacheValidationTimeoutSeconds + 6) * 1000))
    )
    $primary = $null
    if (-not $SkipProviderDiscovery -and $providerBudget -ge 1) {
        $primaryParameters = @{
            ProjectRoot = $ProjectRoot
            PythonExe = $PythonExe
            ToolPath = $toolPath
            Symbols = $Symbols
            InvocationId = $InvocationId
            TimeoutMilliseconds = [int]$providerBudget
            DeadlineUtc = $deadlineUtc
            LogComponent = 'universe-cache-preparation'
        }
        if ($PSBoundParameters.ContainsKey('TradingDate')) {
            $primaryParameters['TradingDate'] = $TradingDate
        }
        $primary = Invoke-MarketAppUniversePreparationChild @primaryParameters
        if (-not $primary.ExitedAndQuiescent) {
            return [pscustomobject]@{
                Outcome = 'provider_child_exit_or_quiescence_unverified'
                TimedOut = [bool]$primary.TimedOut
                StartupMayContinue = $false
                Result = $null
                UsesFallback = $false
                ProvenanceLabel = 'NO_LAUNCH_SAFE_UNIVERSE'
                Primary = $primary
                CacheValidation = $null
            }
        }
    }

    $remainingMilliseconds = [Math]::Floor(
        ($deadlineUtc - [DateTime]::UtcNow).TotalMilliseconds
    )
    $validationBudget = [Math]::Min(
        $CacheValidationTimeoutSeconds * 1000,
        [Math]::Max(0, $remainingMilliseconds - 3000)
    )
    if ($validationBudget -lt 1) {
        return [pscustomobject]@{
            Outcome = 'deadline_elapsed_before_cache_only_validation'
            TimedOut = $true
            StartupMayContinue = $false
            Result = $null
            UsesFallback = $false
            ProvenanceLabel = 'NO_LAUNCH_SAFE_UNIVERSE'
            Primary = $primary
            CacheValidation = $null
        }
    }

    $validationParameters = @{
        ProjectRoot = $ProjectRoot
        PythonExe = $PythonExe
        ToolPath = $toolPath
        Symbols = $Symbols
        InvocationId = "$InvocationId-cache-only"
        TimeoutMilliseconds = [int]$validationBudget
        DeadlineUtc = $deadlineUtc
        LogComponent = 'universe-cache-validation'
        CacheOnly = $true
    }
    if ($PSBoundParameters.ContainsKey('TradingDate')) {
        $validationParameters['TradingDate'] = $TradingDate
    }
    $validation = Invoke-MarketAppUniversePreparationChild @validationParameters
    if (-not $validation.Completed -or -not $validation.ExitedAndQuiescent) {
        return [pscustomobject]@{
            Outcome = 'cache_only_child_exit_or_quiescence_unverified'
            TimedOut = [bool]$validation.TimedOut
            StartupMayContinue = $false
            Result = $null
            UsesFallback = $false
            ProvenanceLabel = 'NO_LAUNCH_SAFE_UNIVERSE'
            Primary = $primary
            CacheValidation = $validation
        }
    }
    try {
        $resolved = Resolve-MarketAppUniverseCachePreparationResult `
            -Output $validation.Output `
            -ExitCode $validation.ExitCode `
            -ExpectedPreparationMode 'cache_only'
    }
    catch {
        return [pscustomobject]@{
            Outcome = 'cache_only_result_invalid'
            TimedOut = [bool]($primary -and $primary.TimedOut)
            StartupMayContinue = $false
            Result = $null
            UsesFallback = $false
            ProvenanceLabel = 'NO_LAUNCH_SAFE_UNIVERSE'
            Primary = $primary
            CacheValidation = $validation
        }
    }
    if (
        $PSBoundParameters.ContainsKey('TradingDate') -and
        [string]$resolved.Result.provenance.trading_date -cne $TradingDate
    ) {
        $observedTradingDate = [string]$resolved.Result.provenance.trading_date
        return [pscustomobject]@{
            Outcome = 'cache_only_trading_date_mismatch'
            TimedOut = [bool]($primary -and $primary.TimedOut)
            StartupMayContinue = $false
            Result = $null
            UsesFallback = $false
            ProvenanceLabel = 'NO_LAUNCH_SAFE_UNIVERSE'
            ExpectedTradingDate = $TradingDate
            ObservedTradingDate = $observedTradingDate
            Primary = $primary
            CacheValidation = $validation
        }
    }
    if ([DateTime]::UtcNow -ge $deadlineUtc) {
        return [pscustomobject]@{
            Outcome = 'deadline_elapsed_after_cache_only_validation'
            TimedOut = [bool]($primary -and $primary.TimedOut)
            StartupMayContinue = $false
            Result = $null
            UsesFallback = $false
            ProvenanceLabel = 'NO_LAUNCH_SAFE_UNIVERSE'
            Primary = $primary
            CacheValidation = $validation
        }
    }
    return [pscustomobject]@{
        Outcome = if ($primary -and $primary.TimedOut) {
            'provider_timed_out_cache_only_validated'
        }
        elseif ($null -eq $primary) {
            'provider_skipped_cache_only_validated'
        }
        else {
            'provider_completed_cache_only_validated'
        }
        TimedOut = [bool]($primary -and $primary.TimedOut)
        StartupMayContinue = $true
        Result = $resolved.Result
        UsesFallback = $resolved.UsesFallback
        ProvenanceLabel = $resolved.ProvenanceLabel
        Primary = $primary
        CacheValidation = $validation
    }
}

function Resolve-MarketAppMissingTradingDateAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][datetime]$Now,
        [Parameter(Mandatory = $true)][ValidateRange(0, [int]::MaxValue)][int]$ListenerCount,
        [Parameter(Mandatory = $true)][bool]$OwnershipVerified,
        [datetime]$ListenerStartTime = [datetime]::MinValue,
        [bool]$RecoveryAlreadySucceeded = $false,
        [datetime]$RecoveryDeadline = $Now.Date.AddHours(8).AddMinutes(25)
    )

    if ($ListenerCount -ne 1) {
        return [pscustomobject]@{Action='preserve';Reason='listener_count_not_exactly_one'}
    }
    if (-not $OwnershipVerified) {
        return [pscustomobject]@{Action='preserve';Reason='listener_ownership_unverified'}
    }
    if ($ListenerStartTime -eq [datetime]::MinValue) {
        return [pscustomobject]@{Action='preserve';Reason='listener_start_time_unverified'}
    }
    if ($ListenerStartTime.Date -ge $Now.Date) {
        return [pscustomobject]@{Action='preserve';Reason='listener_started_current_day'}
    }
    if ($RecoveryAlreadySucceeded) {
        return [pscustomobject]@{Action='preserve';Reason='session_recovery_already_succeeded'}
    }
    if ($Now -ge $RecoveryDeadline) {
        # A prior-day listener with no authoritative trading-date contract is
        # not useful opening protection. Permit exactly one ownership-guarded
        # salvage instead of preserving stale code for the entire session.
        return [pscustomobject]@{Action='restart';Reason='verified_prior_day_listener_late_salvage'}
    }

    return [pscustomobject]@{Action='restart';Reason='verified_prior_day_listener_preopen'}
}

function Resolve-MarketAppVerifiedCashClose {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][psobject]$RuntimeState,
        [Parameter(Mandatory = $true)][datetime]$Now
    )

    $unverified = {
        param([string]$Reason)
        return [pscustomobject]@{
            Verified = $false
            Reason = $Reason
            CashCloseCt = $null
        }
    }

    if (-not [bool]$RuntimeState.HealthReachable -or
        -not [bool]$RuntimeState.LiveHealthReachable) {
        return & $unverified 'health_surface_unreachable'
    }
    $healthWindow = $RuntimeState.HealthEvidence.subscription_window
    $liveWindow = $RuntimeState.LiveEvidence.subscription_window
    if ($null -eq $healthWindow -or $null -eq $liveWindow) {
        return & $unverified 'subscription_window_missing'
    }

    $expectedTradingDate = $Now.ToString('yyyy-MM-dd')
    if ([string]$healthWindow.trading_date -cne $expectedTradingDate -or
        [string]$liveWindow.trading_date -cne $expectedTradingDate) {
        return & $unverified 'subscription_window_trading_date_mismatch'
    }

    $healthValue = $healthWindow.cash_close_utc
    $liveValue = $liveWindow.cash_close_utc
    if ($healthValue -isnot [string] -or $liveValue -isnot [string] -or
        [string]::IsNullOrWhiteSpace([string]$healthValue) -or
        [string]::IsNullOrWhiteSpace([string]$liveValue) -or
        [string]$healthValue -cnotmatch '(?:Z|[+-]\d{2}:\d{2})$' -or
        [string]$liveValue -cnotmatch '(?:Z|[+-]\d{2}:\d{2})$') {
        return & $unverified 'cash_close_utc_invalid'
    }

    try {
        $healthClose = [DateTimeOffset]::Parse(
            [string]$healthValue,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::RoundtripKind
        )
        $liveClose = [DateTimeOffset]::Parse(
            [string]$liveValue,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::RoundtripKind
        )
    }
    catch {
        return & $unverified 'cash_close_utc_invalid'
    }
    if ($healthClose.UtcDateTime.Ticks -ne $liveClose.UtcDateTime.Ticks) {
        return & $unverified 'cash_close_utc_mismatch'
    }

    try {
        $centralZone = [TimeZoneInfo]::FindSystemTimeZoneById('Central Standard Time')
        $cashCloseCt = [TimeZoneInfo]::ConvertTime($healthClose, $centralZone).DateTime
    }
    catch {
        return & $unverified 'cash_close_timezone_conversion_failed'
    }
    $cashOpenCt = $Now.Date.AddHours(8).AddMinutes(30)
    $latestPermittedCloseCt = $Now.Date.AddHours(15)
    if ($cashCloseCt.Date -ne $Now.Date -or
        $cashCloseCt -le $cashOpenCt -or
        $cashCloseCt -gt $latestPermittedCloseCt) {
        return & $unverified 'cash_close_out_of_bounds'
    }

    return [pscustomobject]@{
        Verified = $true
        Reason = 'health_live_cash_close_verified'
        CashCloseCt = $cashCloseCt
    }
}

function Test-MarketAppBackendCurrentSessionUsable {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][psobject]$RuntimeState,
        [Parameter(Mandatory = $true)]
        [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
        [string]$ExpectedTradingDate
    )

    # A process start date is not a subscription identity. A backend that
    # crossed midnight may be retained only when both health surfaces prove the
    # current trading date, one canonical process epoch/generation, an active
    # handoff, progressing transport, and usable required-index calculations.
    $healthEpoch = [string]$RuntimeState.HealthSubscriptionEpochId
    $liveEpoch = [string]$RuntimeState.LiveSubscriptionEpochId
    $healthGeneration = 0
    $liveGeneration = 0
    try { $healthGeneration = [int]$RuntimeState.HealthActiveGeneration } catch { $healthGeneration = 0 }
    try { $liveGeneration = [int]$RuntimeState.LiveActiveGeneration } catch { $liveGeneration = 0 }
    $configured = @(
        $RuntimeState.ConfiguredSymbols |
            ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
            Where-Object { $_ }
    )
    $requiredFailures = @(
        @($RuntimeState.RequiredMissingSymbols) +
        @($RuntimeState.RequiredInvalidSymbols) +
        @($RuntimeState.RequiredEpochMismatchSymbols) +
        @($RuntimeState.RequiredGenerationMismatchSymbols) +
        @($RuntimeState.RequiredZeroFreshQuoteSymbols) +
        @($RuntimeState.RequiredStaleSymbols) |
            Where-Object { $_ }
    )

    return [bool](
        [bool]$RuntimeState.HealthReachable -and
        [bool]$RuntimeState.LiveHealthReachable -and
        ([string]$RuntimeState.HealthProvider).ToLowerInvariant() -eq 'databento' -and
        ([string]$RuntimeState.LiveProvider).ToLowerInvariant() -eq 'databento' -and
        [string]$RuntimeState.TradingDate -ceq $ExpectedTradingDate -and
        [bool]$RuntimeState.StreamingActive -and
        [bool]$RuntimeState.SubscriptionAllowed -and
        ([string]$RuntimeState.SubscriptionSessionState).ToLowerInvariant() -in @('preopen', 'regular_session') -and
        [bool]$RuntimeState.OrbSamplerAlive -and
        [int]$RuntimeState.OrbSamplerIntervalSeconds -eq 5 -and
        [bool]$RuntimeState.StreamConnected -and
        [bool]$RuntimeState.StreamProgressing -and
        [bool]$RuntimeState.CollectionReady -and
        [bool]$RuntimeState.CalculationReady -and
        [bool]$RuntimeState.PredictionPipelineOk -and
        $healthEpoch -cmatch '^[0-9a-f]{64}$' -and
        $liveEpoch -ceq $healthEpoch -and
        $healthGeneration -gt 0 -and
        $liveGeneration -eq $healthGeneration -and
        ([string]$RuntimeState.HealthHandoffStatus).ToLowerInvariant() -eq 'active' -and
        ([string]$RuntimeState.LiveHandoffStatus).ToLowerInvariant() -eq 'active' -and
        'SPX' -in $configured -and
        'NDX' -in $configured -and
        $requiredFailures.Count -eq 0
    )
}

function Test-MarketAppRecoveryPositiveInteger {
    param($Value)
    return [bool](($Value -is [int] -or $Value -is [long]) -and $Value -gt 0)
}

function Test-MarketAppDeadHandoffRecoveryEligible {
    [CmdletBinding()]
    param(
        [datetime]$Now,
        [int]$ListenerCount,
        [bool]$OwnershipVerified,
        [datetime]$ListenerStartTime,
        [psobject]$RuntimeState,
        [bool]$RecoveryAlreadyAttempted = $false
    )
    try {
    $cashCloseResolution = Resolve-MarketAppVerifiedCashClose `
        -RuntimeState $RuntimeState `
        -Now $Now
    if (-not [bool]$cashCloseResolution.Verified) {
        return $false
    }
    $cashClose = [datetime]$cashCloseResolution.CashCloseCt
    if ($RecoveryAlreadyAttempted -or $ListenerCount -ne 1 -or -not $OwnershipVerified -or
        $ListenerStartTime.Date -ne $Now.Date -or ($Now - $ListenerStartTime).TotalSeconds -le 180 -or
        $Now.DayOfWeek -in @([DayOfWeek]::Saturday, [DayOfWeek]::Sunday) -or
        $Now -lt $Now.Date.AddHours(8).AddMinutes(30) -or $Now -ge $cashClose) {
        return $false
    }
    $health = $RuntimeState.HealthEvidence
    $live = $RuntimeState.LiveEvidence
    if (-not $RuntimeState.HealthReachable -or -not $RuntimeState.LiveHealthReachable -or
        $null -eq $health -or $null -eq $live -or
        [string]$health.provider -cne 'databento' -or [string]$live.provider -cne 'databento' -or
        [string]$health.universe_provenance.trading_date -cne $Now.ToString('yyyy-MM-dd') -or
        [string]$health.handoff_reason -cne 'Fresh post-refresh quotes did not arrive before timeout' -or
        [string]$health.handoff_status -cne 'degraded' -or [string]$live.handoff_status -cne 'degraded' -or
        [string]$health.subscription_session_state -cne 'regular_session' -or
        [string]$live.subscription_session_state -cne 'regular_session') {
        return $false
    }
    foreach ($value in @($health.streaming_active, $health.subscription_allowed, $live.subscription_allowed,
        $health.stream_progressing, $live.stream_progressing, $live.stream_connected,
        $health.orb_reference_sampler.thread_alive)) {
        if ($value -isnot [bool] -or $value -ne $true) { return $false }
    }
    foreach ($value in @($health.collection_ready, $live.collection_ready,
        $health.calculation_ready, $live.calculation_ready)) {
        if ($value -isnot [bool] -or $value -ne $false) { return $false }
    }
    if ($health.orb_reference_sampler.interval_seconds -ne 5 -or
        $health.subscription_epoch_id -isnot [string] -or
        $health.subscription_epoch_id -cnotmatch '^[0-9a-f]{64}$' -or
        $health.subscription_epoch_id -cne $live.subscription_epoch_id) { return $false }
    foreach ($value in @($health.active_generation, $live.active_generation, $live.subscription_generation,
        $health.messages_received, $live.messages_received, $health.symbols_subscribed,
        $health.subscription_metadata.selected_contract_count)) {
        if (-not (Test-MarketAppRecoveryPositiveInteger $value)) { return $false }
    }
    if ($health.active_generation -ne $live.active_generation -or
        $health.active_generation -ne $live.subscription_generation -or
        $health.symbols_subscribed -ne $health.subscription_metadata.selected_contract_count -or
        $health.subscription_metadata.selected_universe_sha256 -isnot [string] -or
        $health.subscription_metadata.selected_universe_sha256 -cnotmatch '^[0-9a-f]{64}$') { return $false }
    foreach ($symbol in @('SPX', 'NDX')) {
        if ($symbol -cnotin @($health.symbols_requested) -or
            -not (Test-MarketAppRecoveryPositiveInteger $health.fresh_quote_counts.$symbol) -or
            -not (Test-MarketAppRecoveryPositiveInteger $live.symbol_status.$symbol.fresh_quote_count)) { return $false }
    }
    return $true
    }
    catch { return $false }
}

function Test-MarketAppDeadHandoffCacheProof {
    [CmdletBinding()]
    param(
        [psobject]$BeforeState,
        [psobject]$AfterState,
        [psobject]$Preparation,
        [string[]]$ExpectedSymbols,
        [int]$ExpectedContractCap
    )
    try {
    $before = $BeforeState.HealthEvidence
    $after = $AfterState.HealthEvidence
    $result = $Preparation.Result
    if ($Preparation.StartupMayContinue -isnot [bool] -or $Preparation.StartupMayContinue -ne $true -or
        $Preparation.TimedOut -isnot [bool] -or $Preparation.TimedOut -ne $false -or
        $null -eq $result -or [string]$result.preparation_mode -cne 'cache_only' -or
        [string]$result.provenance.trading_date -cne [string]$before.universe_provenance.trading_date -or
        $before.subscription_epoch_id -cne $after.subscription_epoch_id -or
        $before.active_generation -ne $after.active_generation -or
        $after.messages_received -le $before.messages_received -or
        [string]$before.subscription_profile -cne 'near-term-shadow' -or
        [string]$after.subscription_profile -cne [string]$before.subscription_profile) { return $false }
    $expected = (@($ExpectedSymbols | Sort-Object -Unique) -join ',')
    foreach ($health in @($before, $after)) {
        if ((@($health.symbols_requested | Sort-Object -Unique) -join ',') -cne $expected -or
            $health.subscription_bounds.max_subscription_contracts -ne $ExpectedContractCap -or
            $health.subscription_bounds.shadow_max_strike_pairs -ne 50 -or
            $health.subscription_metadata.selected_universe_sha256 -cne $result.selected_universe_sha256 -or
            $health.symbols_subscribed -ne $result.selected_contract_count -or
            [string]$health.universe_provenance.source_sha256 -cne [string]$result.provenance.source_sha256) { return $false }
    }
    return [bool](
        (Test-MarketAppRecoveryPositiveInteger $result.selected_contract_count) -and
        $ExpectedContractCap -gt 0 -and $result.selected_contract_count -le $ExpectedContractCap -and
        $result.selected_universe_sha256 -is [string] -and
        $result.selected_universe_sha256 -cmatch '^[0-9a-f]{64}$' -and
        $result.provenance.source_sha256 -is [string] -and
        $result.provenance.source_sha256 -cmatch '^[0-9a-f]{64}$'
    )
    }
    catch { return $false }
}

function Resolve-MarketAppBackendReadinessAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][datetime]$Now,
        [Parameter(Mandatory = $true)][ValidateRange(0, [int]::MaxValue)][int]$ListenerCount,
        [Parameter(Mandatory = $true)][bool]$OwnershipVerified,
        [datetime]$ListenerStartTime = [datetime]::MinValue,
        [Parameter(Mandatory = $true)][psobject]$RuntimeState,
        [bool]$RecoveryAlreadyAttempted = $false,
        [bool]$DeadHandoffRecoveryAlreadyAttempted = $false,
        [datetime]$RecoveryDeadline = $Now.Date.AddHours(8).AddMinutes(25),
        [timespan]$StartGrace = ([timespan]::FromMinutes(3))
    )

    # This helper is deliberately pure. The caller owns endpoint reads,
    # journaling, PID verification, and any eventual stop/start operation.
    $failures = @()
    if (-not [bool]$RuntimeState.HealthReachable) {
        $failures += 'health_unreachable'
    }
    if (-not [bool]$RuntimeState.LiveHealthReachable) {
        $failures += 'live_health_unreachable'
    }
    if (([string]$RuntimeState.HealthProvider).ToLowerInvariant() -ne 'databento') {
        $failures += 'health_provider_not_databento'
    }
    if (([string]$RuntimeState.LiveProvider).ToLowerInvariant() -ne 'databento') {
        $failures += 'live_provider_not_databento'
    }
    if (-not [bool]$RuntimeState.StreamingActive) {
        $failures += 'provider_lifecycle_inactive'
    }
    if (-not [bool]$RuntimeState.OrbSamplerAlive) {
        $failures += 'orb_sampler_not_alive'
    }
    if ([int]$RuntimeState.OrbSamplerIntervalSeconds -ne 5) {
        $failures += 'orb_sampler_cadence_invalid'
    }
    $healthEpoch = [string]$RuntimeState.HealthSubscriptionEpochId
    $liveEpoch = [string]$RuntimeState.LiveSubscriptionEpochId
    if ($healthEpoch -cnotmatch '^[0-9a-f]{64}$') {
        $failures += 'health_subscription_epoch_invalid'
    }
    if ($liveEpoch -cnotmatch '^[0-9a-f]{64}$') {
        $failures += 'live_subscription_epoch_invalid'
    }
    if (
        $healthEpoch -cmatch '^[0-9a-f]{64}$' -and
        $liveEpoch -cmatch '^[0-9a-f]{64}$' -and
        $healthEpoch -cne $liveEpoch
    ) {
        $failures += 'subscription_epoch_mismatch'
    }
    $healthGeneration = 0
    $liveGeneration = 0
    try { $healthGeneration = [int]$RuntimeState.HealthActiveGeneration } catch { $healthGeneration = 0 }
    try { $liveGeneration = [int]$RuntimeState.LiveActiveGeneration } catch { $liveGeneration = 0 }
    if ($healthGeneration -le 0) {
        $failures += 'health_subscription_generation_invalid'
    }
    if ($liveGeneration -le 0) {
        $failures += 'live_subscription_generation_invalid'
    }
    if ($healthGeneration -gt 0 -and $liveGeneration -gt 0 -and $healthGeneration -ne $liveGeneration) {
        $failures += 'subscription_generation_mismatch'
    }
    if (([string]$RuntimeState.HealthHandoffStatus).ToLowerInvariant() -ne 'active') {
        $failures += 'health_handoff_not_active'
    }
    if (([string]$RuntimeState.LiveHandoffStatus).ToLowerInvariant() -ne 'active') {
        $failures += 'live_handoff_not_active'
    }

    $cashCloseResolution = Resolve-MarketAppVerifiedCashClose `
        -RuntimeState $RuntimeState `
        -Now $Now
    if (-not [bool]$cashCloseResolution.Verified) {
        $failures += 'subscription_cash_close_unverified'
    }
    $sessionStart = $Now.Date.AddHours(7).AddMinutes(45)
    $cashOpen = $Now.Date.AddHours(8).AddMinutes(30)
    $cashClose = if ([bool]$cashCloseResolution.Verified) {
        [datetime]$cashCloseResolution.CashCloseCt
    }
    else {
        # Before the earliest scheduled U.S. cash early close, retain the
        # ordinary-session clock so opening recovery remains available. At or
        # after noon CT, unverified close metadata blocks destructive recovery.
        $Now.Date.AddHours(15)
    }
    $insideSubscriptionWindow = $Now -ge $sessionStart -and $Now -lt $cashClose
    if ($insideSubscriptionWindow) {
        if (-not [bool]$RuntimeState.SubscriptionAllowed) {
            $failures += 'subscription_not_allowed'
        }
        if (([string]$RuntimeState.SubscriptionSessionState).ToLowerInvariant() -notin @('preopen', 'regular_session')) {
            $failures += 'subscription_session_state_invalid'
        }
    }

    $configured = @(
        $RuntimeState.ConfiguredSymbols |
            ForEach-Object { ([string]$_).Trim().ToUpperInvariant() } |
            Where-Object { $_ }
    )
    foreach ($core in @('SPX', 'NDX')) {
        if ($core -notin $configured) {
            $failures += "core_symbol_not_configured:$core"
        }
    }

    if ($Now -ge $cashOpen -and $Now -lt $cashClose) {
        foreach ($field in @(
            @{Name='StreamConnected';Reason='live_stream_not_connected'},
            @{Name='StreamProgressing';Reason='live_stream_not_progressing'},
            @{Name='CollectionReady';Reason='live_collection_not_ready'},
            @{Name='CalculationReady';Reason='live_calculation_not_ready'},
            @{Name='PredictionPipelineOk';Reason='live_prediction_pipeline_not_ready'}
        )) {
            if (-not [bool]$RuntimeState.($field.Name)) {
                $failures += $field.Reason
            }
        }
        foreach ($field in @(
            @{Name='RequiredMissingSymbols';Reason='required_symbols_missing'},
            @{Name='RequiredInvalidSymbols';Reason='required_symbols_invalid'},
            @{Name='RequiredEpochMismatchSymbols';Reason='required_epoch_mismatch'},
            @{Name='RequiredGenerationMismatchSymbols';Reason='required_generation_mismatch'},
            @{Name='RequiredZeroFreshQuoteSymbols';Reason='required_zero_fresh_quotes'},
            @{Name='RequiredStaleSymbols';Reason='required_symbols_stale'}
        )) {
            if (@($RuntimeState.($field.Name) | Where-Object { $_ }).Count -gt 0) {
                $failures += $field.Reason
            }
        }
    }

    $failures = @($failures | Select-Object -Unique)
    $exactOwnedPriorDayListener = [bool](
        $ListenerCount -eq 1 -and
        $OwnershipVerified -and
        $ListenerStartTime -ne [datetime]::MinValue -and
        $ListenerStartTime.Date -lt $Now.Date
    )
    $runtimeTradingDate = [string]$RuntimeState.TradingDate
    $currentTradingDate = $Now.ToString('yyyy-MM-dd')
    if (
        $Now -ge $cashClose -and
        $exactOwnedPriorDayListener -and
        [bool]$RuntimeState.HealthReachable -and
        $runtimeTradingDate -ceq $currentTradingDate
    ) {
        # The live subscription deliberately becomes off-hours at the cash
        # close. Preserve the exact owned process that already carries today's
        # retained final state instead of replacing it with an empty process
        # that cannot subscribe until the next session.
        return [pscustomobject]@{
            Action = 'preserve'
            Reason = 'current_session_post_close_preserved'
            FailureReasons = $failures
            StartAgeSeconds = [math]::Max(0.0, ($Now - $ListenerStartTime).TotalSeconds)
        }
    }
    if (
        $Now -ge $sessionStart -and
        $Now -lt $RecoveryDeadline -and
        $exactOwnedPriorDayListener
    ) {
        # A healthy payload can roll its trading date forward without reloading
        # the Python modules that were imported by yesterday's process. During
        # the bounded pre-open replacement window, force one ownership-guarded
        # refresh so today's source and current-day universe are both active
        # before the opening capture begins.
        return [pscustomobject]@{
            Action = 'restart'
            Reason = 'verified_prior_day_backend_preopen_refresh'
            FailureReasons = @(
                @($failures) + 'prior_day_backend_requires_current_code' |
                    Select-Object -Unique
            )
            StartAgeSeconds = [math]::Max(0.0, ($Now - $ListenerStartTime).TotalSeconds)
        }
    }
    if ($failures.Count -eq 0) {
        if (
            $Now -ge $RecoveryDeadline -and
            $Now -lt $cashClose -and
            $exactOwnedPriorDayListener
        ) {
            if (Test-MarketAppBackendCurrentSessionUsable `
                -RuntimeState $RuntimeState `
                -ExpectedTradingDate $currentTradingDate) {
                return [pscustomobject]@{
                    Action = 'preserve'
                    Reason = 'current_session_contract_progressing'
                    FailureReasons = @()
                    StartAgeSeconds = [math]::Max(0.0, ($Now - $ListenerStartTime).TotalSeconds)
                }
            }
            # Pre-open generic checks intentionally omit regular-session
            # transport and calculation gates. At the late-salvage boundary a
            # prior-day process must nevertheless prove the complete current
            # session contract or take the one ownership-guarded restart.
            $failures = @('current_session_contract_unproven')
        }
        else {
            return [pscustomobject]@{
                Action = 'preserve'
                Reason = 'healthy'
                FailureReasons = @()
                StartAgeSeconds = if ($ListenerStartTime -eq [datetime]::MinValue) { $null } else { [math]::Max(0.0, ($Now - $ListenerStartTime).TotalSeconds) }
            }
        }
    }
    if ($ListenerCount -ne 1) {
        return [pscustomobject]@{Action='preserve';Reason='listener_count_not_exactly_one';FailureReasons=$failures;StartAgeSeconds=$null}
    }
    if (-not $OwnershipVerified) {
        return [pscustomobject]@{Action='preserve';Reason='listener_ownership_unverified';FailureReasons=$failures;StartAgeSeconds=$null}
    }
    if ($ListenerStartTime -eq [datetime]::MinValue) {
        return [pscustomobject]@{Action='preserve';Reason='listener_start_time_unverified';FailureReasons=$failures;StartAgeSeconds=$null}
    }

    $startAgeSeconds = [math]::Max(0.0, ($Now - $ListenerStartTime).TotalSeconds)
    if (-not [bool]$cashCloseResolution.Verified -and
        $Now -ge $Now.Date.AddHours(12)) {
        return [pscustomobject]@{
            Action = 'preserve'
            Reason = 'cash_close_boundary_unverified'
            FailureReasons = $failures
            StartAgeSeconds = $startAgeSeconds
        }
    }
    if (Test-MarketAppDeadHandoffRecoveryEligible -Now $Now -ListenerCount $ListenerCount `
        -OwnershipVerified $OwnershipVerified -ListenerStartTime $ListenerStartTime -RuntimeState $RuntimeState `
        -RecoveryAlreadyAttempted:($RecoveryAlreadyAttempted -or $DeadHandoffRecoveryAlreadyAttempted)) {
        return [pscustomobject]@{
            Action = 'restart'
            Reason = 'verified_current_day_dead_handoff_late_salvage'
            FailureReasons = $failures
            StartAgeSeconds = $startAgeSeconds
        }
    }
    if ($Now -ge $RecoveryDeadline) {
        if ($ListenerStartTime.Date -lt $Now.Date) {
            if ($RecoveryAlreadyAttempted) {
                return [pscustomobject]@{Action='preserve';Reason='session_recovery_already_succeeded';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
            }
            return [pscustomobject]@{
                Action = 'restart'
                Reason = 'verified_prior_day_backend_late_salvage'
                FailureReasons = $failures
                StartAgeSeconds = $startAgeSeconds
            }
        }
        return [pscustomobject]@{Action='preserve';Reason='protected_opening_boundary_reached';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
    }
    if ($RecoveryAlreadyAttempted) {
        return [pscustomobject]@{Action='preserve';Reason='session_recovery_already_attempted';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
    }
    if ($startAgeSeconds -lt $StartGrace.TotalSeconds) {
        return [pscustomobject]@{Action='preserve';Reason='current_day_start_grace';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
    }

    return [pscustomobject]@{
        Action = 'restart'
        Reason = 'verified_aged_backend_readiness_failure'
        FailureReasons = $failures
        StartAgeSeconds = $startAgeSeconds
    }
}

function Resolve-MarketAppDashboardReadinessAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][datetime]$Now,
        [Parameter(Mandatory = $true)][ValidateRange(0, [int]::MaxValue)][int]$ListenerCount,
        [Parameter(Mandatory = $true)][bool]$OwnershipVerified,
        [datetime]$ListenerStartTime = [datetime]::MinValue,
        [Parameter(Mandatory = $true)][bool]$EndpointReady,
        [bool]$RecoveryAlreadyAttempted = $false,
        [datetime]$RecoveryDeadline = $Now.Date.AddHours(8).AddMinutes(25),
        [timespan]$StartGrace = ([timespan]::FromMinutes(3))
    )

    # Keep the dashboard recovery decision deterministic. The caller performs
    # the read-only health request, proves the sole listener's ownership, and
    # records a request before stopping and a success latch only after the
    # replacement listener owns the exact endpoint contract.
    $failures = if ($EndpointReady) { @() } else { @('dashboard_health_unreachable_or_invalid') }
    if ($ListenerCount -ne 1) {
        return [pscustomobject]@{Action='preserve';Reason='listener_count_not_exactly_one';FailureReasons=$failures;StartAgeSeconds=$null}
    }
    if (-not $OwnershipVerified) {
        return [pscustomobject]@{Action='preserve';Reason='listener_ownership_unverified';FailureReasons=$failures;StartAgeSeconds=$null}
    }
    if ($ListenerStartTime -eq [datetime]::MinValue) {
        return [pscustomobject]@{Action='preserve';Reason='listener_start_time_unverified';FailureReasons=$failures;StartAgeSeconds=$null}
    }

    $startAgeSeconds = [math]::Max(0.0, ($Now - $ListenerStartTime).TotalSeconds)
    if ($ListenerStartTime.Date -lt $Now.Date) {
        $failures = @($failures + 'dashboard_started_prior_day' | Select-Object -Unique)
        if ($RecoveryAlreadyAttempted) {
            return [pscustomobject]@{Action='preserve';Reason='session_recovery_already_succeeded';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
        }
        if ($Now -ge $RecoveryDeadline) {
            return [pscustomobject]@{
                Action = 'restart'
                Reason = 'verified_prior_day_dashboard_late_salvage'
                FailureReasons = $failures
                StartAgeSeconds = $startAgeSeconds
            }
        }
        return [pscustomobject]@{
            Action = 'restart'
            Reason = 'verified_prior_day_dashboard_preopen'
            FailureReasons = $failures
            StartAgeSeconds = $startAgeSeconds
        }
    }
    if ($EndpointReady) {
        return [pscustomobject]@{
            Action = 'preserve'
            Reason = 'healthy'
            FailureReasons = @()
            StartAgeSeconds = $startAgeSeconds
        }
    }
    if ($Now -ge $RecoveryDeadline) {
        return [pscustomobject]@{Action='preserve';Reason='protected_opening_boundary_reached';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
    }
    if ($RecoveryAlreadyAttempted) {
        return [pscustomobject]@{Action='preserve';Reason='session_recovery_already_attempted';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
    }
    if ($startAgeSeconds -lt $StartGrace.TotalSeconds) {
        return [pscustomobject]@{Action='preserve';Reason='current_day_start_grace';FailureReasons=$failures;StartAgeSeconds=$startAgeSeconds}
    }

    return [pscustomobject]@{
        Action = 'restart'
        Reason = 'verified_aged_dashboard_readiness_failure'
        FailureReasons = $failures
        StartAgeSeconds = $startAgeSeconds
    }
}

function Get-MarketAppListenerProcessIds {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port
    )

    # netstat is substantially faster and less prone to multi-minute CIM
    # stalls than Get-NetTCPConnection on this workstation. This is read-only.
    $netstatExe = Join-Path ([Environment]::SystemDirectory) 'netstat.exe'
    $lines = & $netstatExe -ano -p tcp 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "netstat failed while checking listener ownership for port $Port."
    }

    $pattern = '^\s*TCP\s+\S+:' + [regex]::Escape([string]$Port) + '\s+\S+\s+LISTENING\s+(\d+)\s*$'
    $processIds = foreach ($line in $lines) {
        if ([string]$line -match $pattern) {
            [int]$Matches[1]
        }
    }
    return @($processIds | Sort-Object -Unique)
}

function Test-MarketAppPortListener {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port
    )

    return @(Get-MarketAppListenerProcessIds -Port $Port).Count -gt 0
}

function Assert-MarketAppExpectedListenerPid {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$ExpectedPid,
        [int[]]$ListenerProcessIds
    )

    $currentPids = @(
        if ($PSBoundParameters.ContainsKey('ListenerProcessIds')) {
            $ListenerProcessIds | Sort-Object -Unique
        }
        else {
            Get-MarketAppListenerProcessIds -Port $Port
        }
    )

    if ($currentPids.Count -ne 1 -or [int]$currentPids[0] -ne $ExpectedPid) {
        $actual = if ($currentPids.Count) { $currentPids -join ',' } else { 'none' }
        throw "Listener PID mismatch on port ${Port}: expected $ExpectedPid, actual $actual. No process was changed."
    }
    return $ExpectedPid
}

function Get-MarketAppProcessRecord {
    param([Parameter(Mandatory = $true)][int]$ProcessId)

    return Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
}

function Get-MarketAppRemoteTcpOwnerProcessIdsFromNetstatLines {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 65535)]
        [int]$RemotePort,

        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [string[]]$Lines
    )

    $pattern = '^\s*TCP\s+\S+\s+\S+:' +
        [regex]::Escape([string]$RemotePort) + '\s+\S+\s+(\d+)\s*$'
    $processIds = foreach ($line in $Lines) {
        if ([string]$line -match $pattern) {
            $ownerProcessId = [int]$Matches[1]
            # PID 0 is an unowned kernel/TIME_WAIT row, not a process that can
            # be revalidated or stopped. Exact prior-owner quiescence is
            # enforced separately for every captured positive PID.
            if ($ownerProcessId -gt 0) {
                $ownerProcessId
            }
        }
    }
    return @($processIds | Sort-Object -Unique)
}

function Get-MarketAppRemoteTcpOwnerProcessIds {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, 65535)]
        [int]$RemotePort
    )

    $netstatExe = Join-Path ([Environment]::SystemDirectory) 'netstat.exe'
    $lines = & $netstatExe -ano -p tcp 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "netstat failed while checking remote TCP port $RemotePort ownership."
    }
    return @(Get-MarketAppRemoteTcpOwnerProcessIdsFromNetstatLines `
        -RemotePort $RemotePort `
        -Lines @($lines | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string]$_)
        }))
}

function Get-MarketAppProcessIdentityToken {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][psobject]$ProcessRecord)

    try {
        $processId = [int]$ProcessRecord.ProcessId
        $createdAt = [datetime]$ProcessRecord.CreationDate
        if ($processId -le 0 -or $createdAt -eq [datetime]::MinValue) {
            return $null
        }
        return "$processId|$($createdAt.ToUniversalTime().Ticks)"
    }
    catch {
        return $null
    }
}

function Get-MarketAppDirectBackendProcessIdentities {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string[]]$RequiredCommandMarkers,
        [psobject[]]$ProcessRecords
    )

    $records = if ($PSBoundParameters.ContainsKey('ProcessRecords')) {
        @($ProcessRecords)
    }
    else {
        @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    }
    $normalizedRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
    $identities = foreach ($record in $records) {
        $verificationText = "$([string]$record.CommandLine)`n$([string]$record.ExecutablePath)"
        $hasProjectRoot = (
            $verificationText.IndexOf(
                $normalizedRoot,
                [StringComparison]::OrdinalIgnoreCase
            ) -ge 0
        )
        $hasBackendMarker = [bool]($RequiredCommandMarkers | Where-Object {
            $verificationText.IndexOf($_, [StringComparison]::OrdinalIgnoreCase) -ge 0
        })
        if (-not $hasProjectRoot -or -not $hasBackendMarker) {
            continue
        }
        $identityToken = Get-MarketAppProcessIdentityToken -ProcessRecord $record
        if (-not $identityToken) {
            continue
        }
        [pscustomobject]@{
            ProcessId = [int]$record.ProcessId
            ParentProcessId = [int]$record.ParentProcessId
            IdentityToken = $identityToken
        }
    }
    return @($identities | Sort-Object ProcessId -Unique)
}

function Invoke-MarketAppVerifiedOrphanBackendCleanup {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [ValidateRange(1, 65535)][int]$Port = 8000,
        [ValidateRange(1, 65535)][int]$ProviderRemotePort = 13000,
        [string[]]$RequiredCommandMarkers = @('server.py', 'backend.app:app'),
        [Parameter(Mandatory = $true)][string]$InvocationId,
        [Parameter(Mandatory = $true)][string]$Caller
    )

    $listenerProcessIds = @(Get-MarketAppListenerProcessIds -Port $Port)
    if ($listenerProcessIds.Count -ne 0) {
        throw "A listener appeared on port $Port before orphan-backend cleanup; no process was changed."
    }

    $directIdentities = @(Get-MarketAppDirectBackendProcessIdentities `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers $RequiredCommandMarkers)
    $providerOwnerIds = @(Get-MarketAppRemoteTcpOwnerProcessIds `
        -RemotePort $ProviderRemotePort)
    if ($directIdentities.Count -eq 0 -and $providerOwnerIds.Count -eq 0) {
        return [pscustomobject]@{
            Cleaned = $false
            ProcessIds = @()
            ProviderOwnerProcessIds = @()
        }
    }

    # Do not use collection member enumeration here. Under Windows PowerShell
    # strict mode, reading .ProcessId from an empty Object[] throws instead of
    # yielding an empty collection.
    $directIds = @(
        $directIdentities |
            ForEach-Object { [int]$_.ProcessId } |
            Sort-Object -Unique
    )
    $rootIds = foreach ($directId in $directIds) {
        $nested = $false
        foreach ($otherId in $directIds) {
            if ($otherId -eq $directId) {
                continue
            }
            if (Test-MarketAppProcessDescendsFrom `
                -ProcessId $directId `
                -AncestorProcessId $otherId) {
                $nested = $true
                break
            }
        }
        if (-not $nested) {
            [int]$directId
        }
    }
    $rootIds = @($rootIds | Sort-Object -Unique)

    foreach ($providerOwnerId in $providerOwnerIds) {
        $verifiedOwner = [bool]($rootIds | Where-Object {
            Test-MarketAppProcessDescendsFrom `
                -ProcessId ([int]$providerOwnerId) `
                -AncestorProcessId ([int]$_)
        })
        if (-not $verifiedOwner) {
            throw "Remote TCP port $ProviderRemotePort has unverified owner PID $providerOwnerId; no process was changed."
        }
    }

    $stopOrder = [System.Collections.Generic.List[int]]::new()
    foreach ($rootId in $rootIds) {
        $descendants = @(Get-MarketAppDescendantProcessIds -RootProcessId $rootId)
        [array]::Reverse($descendants)
        foreach ($candidateId in @($descendants + $rootId)) {
            if (-not $stopOrder.Contains([int]$candidateId)) {
                $stopOrder.Add([int]$candidateId)
            }
        }
    }
    foreach ($providerOwnerId in $providerOwnerIds) {
        if (-not $stopOrder.Contains([int]$providerOwnerId)) {
            $stopOrder.Add([int]$providerOwnerId)
        }
    }

    $identityTokens = @{}
    foreach ($candidateId in $stopOrder) {
        $record = Get-MarketAppProcessRecord -ProcessId $candidateId
        $identityToken = if ($record) {
            Get-MarketAppProcessIdentityToken -ProcessRecord $record
        }
        else {
            $null
        }
        if (-not $identityToken -or -not (Test-MarketAppVerifiedProcess `
            -ProcessId $candidateId `
            -ProjectRoot $ProjectRoot `
            -RequiredCommandMarkers $RequiredCommandMarkers)) {
            throw "Orphan backend PID $candidateId could not be strongly revalidated; no process was changed."
        }
        $identityTokens[[int]$candidateId] = $identityToken
    }

    # Recheck the no-listener premise after all discovery and before the first
    # process mutation. A concurrent replacement belongs to another supervisor.
    if (@(Get-MarketAppListenerProcessIds -Port $Port).Count -ne 0) {
        throw "A listener appeared on port $Port during orphan-backend cleanup; no process was changed."
    }

    Write-MarketAppSupervisorLog `
        -ProjectRoot $ProjectRoot `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -Event 'orphan_backend_cleanup_prepared' `
        -Message "component=backend process_ids=$($stopOrder -join ',') provider_remote_port=$ProviderRemotePort provider_owner_pids=$($providerOwnerIds -join ',') action=stop_verified_tree"

    foreach ($candidateId in $stopOrder) {
        $record = Get-MarketAppProcessRecord -ProcessId $candidateId
        if (-not $record) {
            continue
        }
        $identityToken = Get-MarketAppProcessIdentityToken -ProcessRecord $record
        if (
            $identityToken -cne [string]$identityTokens[[int]$candidateId] -or
            -not (Test-MarketAppVerifiedProcess `
                -ProcessId $candidateId `
                -ProjectRoot $ProjectRoot `
                -RequiredCommandMarkers $RequiredCommandMarkers)
        ) {
            throw "Orphan backend PID $candidateId changed identity before stop; cleanup failed closed."
        }
        Stop-Process -Id $candidateId -Force -ErrorAction Stop
    }

    foreach ($candidateId in $stopOrder) {
        $quiescence = Wait-MarketAppProcessNetworkQuiescence `
            -ProcessId $candidateId `
            -TimeoutSeconds 30
        if (-not $quiescence.Quiescent) {
            throw "Orphan backend PID $candidateId did not reach exact-PID network quiescence: process_exists=$($quiescence.ProcessExists) tcp_connection_count=$($quiescence.TcpConnectionCount)."
        }
    }

    $remainingListeners = @(Get-MarketAppListenerProcessIds -Port $Port)
    $remainingProviderOwners = @(Get-MarketAppRemoteTcpOwnerProcessIds `
        -RemotePort $ProviderRemotePort)
    $remainingBackendProcesses = @(Get-MarketAppDirectBackendProcessIdentities `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers $RequiredCommandMarkers)
    if (
        $remainingListeners.Count -ne 0 -or
        $remainingProviderOwners.Count -ne 0 -or
        $remainingBackendProcesses.Count -ne 0
    ) {
        throw "Orphan backend cleanup postcondition failed: listener_count=$($remainingListeners.Count) provider_owner_count=$($remainingProviderOwners.Count) backend_process_count=$($remainingBackendProcesses.Count)."
    }

    Write-MarketAppSupervisorLog `
        -ProjectRoot $ProjectRoot `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -Event 'orphan_backend_cleanup_quiescent' `
        -Message "component=backend process_ids=$($stopOrder -join ',') provider_remote_port=$ProviderRemotePort process_exited=true tcp_connection_count=0 action=continue"
    return [pscustomobject]@{
        Cleaned = $true
        ProcessIds = @($stopOrder)
        ProviderOwnerProcessIds = @($providerOwnerIds)
    }
}

function Get-MarketAppProcessTcpConnectionCountFromNetstatLines {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$ProcessId,

        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [string[]]$Lines
    )

    $pattern = '^\s*TCP\s+\S+\s+\S+\s+\S+\s+' +
        [regex]::Escape([string]$ProcessId) + '\s*$'
    return [int]@($Lines | Where-Object { [string]$_ -match $pattern }).Count
}

function Get-MarketAppProcessTcpConnectionCount {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$ProcessId
    )

    # Count every TCP row owned by the exact process, not only LISTENING rows
    # on the service port. A Databento backend can own several outbound SDK
    # sockets even after its HTTP listener has disappeared.
    $netstatExe = Join-Path ([Environment]::SystemDirectory) 'netstat.exe'
    $lines = & $netstatExe -ano -p tcp 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "netstat failed while checking TCP ownership for process PID $ProcessId."
    }

    return Get-MarketAppProcessTcpConnectionCountFromNetstatLines `
        -ProcessId $ProcessId `
        -Lines @($lines | Where-Object {
            -not [string]::IsNullOrWhiteSpace([string]$_)
        })
}

function Wait-MarketAppProcessNetworkQuiescence {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateRange(1, [int]::MaxValue)]
        [int]$ProcessId,

        [ValidateRange(0, 120)]
        [double]$TimeoutSeconds = 30,

        [ValidateRange(10, 5000)]
        [int]$PollMilliseconds = 250
    )

    $startedUtc = [DateTime]::UtcNow
    $deadlineUtc = $startedUtc.AddSeconds($TimeoutSeconds)
    $processExists = $true
    $tcpConnectionCount = -1
    do {
        $processExists = $null -ne (Get-MarketAppProcessRecord -ProcessId $ProcessId)
        $tcpConnectionCount = Get-MarketAppProcessTcpConnectionCount -ProcessId $ProcessId
        if (-not $processExists -and $tcpConnectionCount -eq 0) {
            return [pscustomobject]@{
                Quiescent = $true
                ProcessId = $ProcessId
                ProcessExists = $false
                TcpConnectionCount = 0
                ElapsedMilliseconds = [int][Math]::Round(
                    ([DateTime]::UtcNow - $startedUtc).TotalMilliseconds
                )
            }
        }
        if ([DateTime]::UtcNow -ge $deadlineUtc) {
            break
        }
        Start-Sleep -Milliseconds $PollMilliseconds
    } while ($true)

    return [pscustomobject]@{
        Quiescent = $false
        ProcessId = $ProcessId
        ProcessExists = [bool]$processExists
        TcpConnectionCount = [int]$tcpConnectionCount
        ElapsedMilliseconds = [int][Math]::Round(
            ([DateTime]::UtcNow - $startedUtc).TotalMilliseconds
        )
    }
}

function Test-MarketAppProcessDescendsFrom {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$ProcessId,
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$AncestorProcessId,
        [ValidateRange(1, 32)][int]$MaximumDepth = 12
    )

    $currentId = $ProcessId
    $seen = @{}
    for ($depth = 0; $depth -lt $MaximumDepth; $depth++) {
        if ($currentId -eq $AncestorProcessId) {
            return $true
        }
        if ($seen.ContainsKey($currentId)) {
            return $false
        }
        $seen[$currentId] = $true
        $process = Get-MarketAppProcessRecord -ProcessId $currentId
        if (-not $process) {
            return $false
        }
        $currentId = [int]$process.ParentProcessId
        if ($currentId -le 0) {
            return $false
        }
    }
    return $false
}

function Test-MarketAppVerifiedProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$ProcessId,
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string[]]$RequiredCommandMarkers,
        [ValidateRange(1, 32)][int]$MaximumDepth = 12
    )

    $normalizedRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
    $currentId = $ProcessId
    $seen = @{}
    $hasProjectRoot = $false
    $hasMarker = $false

    for ($depth = 0; $depth -lt $MaximumDepth; $depth++) {
        if ($seen.ContainsKey($currentId)) {
            break
        }
        $seen[$currentId] = $true
        $process = Get-MarketAppProcessRecord -ProcessId $currentId
        if (-not $process) {
            break
        }
        $verificationText = "$([string]$process.CommandLine)`n$([string]$process.ExecutablePath)"
        if ($verificationText.IndexOf($normalizedRoot, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
            $hasProjectRoot = $true
        }
        if ($RequiredCommandMarkers | Where-Object {
            $verificationText.IndexOf($_, [StringComparison]::OrdinalIgnoreCase) -ge 0
        }) {
            $hasMarker = $true
        }
        if ($hasProjectRoot -and $hasMarker) {
            return $true
        }
        $currentId = [int]$process.ParentProcessId
        if ($currentId -le 0) {
            break
        }
    }
    return $false
}

function Invoke-MarketAppBoundedAutomaticListenerStop {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$ExpectedPid,
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string[]]$RequiredCommandMarkers,
        [Parameter(Mandatory = $true)][string]$Component,
        [Parameter(Mandatory = $true)][datetime]$DecisionTime,
        [Parameter(Mandatory = $true)][string]$InvocationId,
        [Parameter(Mandatory = $true)][string]$Caller,
        [Parameter(Mandatory = $true)][string]$RecoveryReason,
        [psobject]$RuntimeState = $null,
        [switch]$AllowLateSessionSalvage
    )

    # Re-resolve and re-verify the exact listener before consulting the final
    # stop-time boundary. This keeps every automatic stop under the same PID,
    # ancestry/command-marker, and decision-day contract.
    $listenerPid = Assert-MarketAppExpectedListenerPid `
        -Port $Port `
        -ExpectedPid $ExpectedPid
    if (-not (Test-MarketAppVerifiedProcess `
        -ProcessId $listenerPid `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers $RequiredCommandMarkers)) {
        throw "Refusing automatic $Component stop for PID $listenerPid on port $Port because it is not a verified MarketPinPredictor process."
    }

    Write-MarketAppSupervisorLog `
        -ProjectRoot $ProjectRoot `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -Event 'automatic_stop_prepared' `
        -Message "component=$Component port=$Port listener_pid=$listenerPid recovery_reason=$RecoveryReason"

    $recoveryDeadline = $DecisionTime.Date.AddHours(8).AddMinutes(25)
    # Keep Get-Date adjacent to Stop-Process. A decision made at 08:24:59 must
    # abstain if ownership verification completes at 08:25:00.
    $stopCheckTime = Get-Date
    $cashBoundedBackendLateSalvage = $RecoveryReason -cin @(
        'verified_prior_day_backend_late_salvage',
        'verified_current_day_dead_handoff_late_salvage'
    )
    $backendLateSalvageBoundaryUnverified = $false
    $backendLateSalvageOutsideCashSession = $false
    if ($cashBoundedBackendLateSalvage) {
        if ($null -eq $RuntimeState) {
            $backendLateSalvageBoundaryUnverified = $true
        }
        else {
            $cashCloseResolution = Resolve-MarketAppVerifiedCashClose `
                -RuntimeState $RuntimeState `
                -Now $stopCheckTime
            $backendLateSalvageBoundaryUnverified = -not [bool]$cashCloseResolution.Verified
            if (-not $backendLateSalvageBoundaryUnverified) {
                $backendLateSalvageOutsideCashSession = (
                    $stopCheckTime -ge [datetime]$cashCloseResolution.CashCloseCt -or
                    (
                        $RecoveryReason -ceq 'verified_current_day_dead_handoff_late_salvage' -and
                        $stopCheckTime -lt $DecisionTime.Date.AddHours(8).AddMinutes(30)
                    )
                )
            }
        }
    }
    if (
        $stopCheckTime.Date -ne $DecisionTime.Date -or
        $backendLateSalvageBoundaryUnverified -or
        $backendLateSalvageOutsideCashSession -or
        (
            -not $AllowLateSessionSalvage -and
            ($DecisionTime -ge $recoveryDeadline -or $stopCheckTime -ge $recoveryDeadline)
        )
    ) {
        $boundaryReason = if ($stopCheckTime.Date -ne $DecisionTime.Date) {
            'decision_day_changed'
        }
        elseif ($backendLateSalvageBoundaryUnverified) {
            if ($RecoveryReason -ceq 'verified_current_day_dead_handoff_late_salvage') {
                'dead_handoff_cash_close_unverified'
            }
            else {
                'prior_day_backend_cash_close_unverified'
            }
        }
        elseif ($backendLateSalvageOutsideCashSession) {
            if ($RecoveryReason -ceq 'verified_current_day_dead_handoff_late_salvage') {
                'dead_handoff_outside_cash_session'
            }
            else {
                'prior_day_backend_outside_cash_session'
            }
        }
        else {
            'protected_opening_boundary_reached'
        }
        Write-MarketAppSupervisorLog `
            -ProjectRoot $ProjectRoot `
            -InvocationId $InvocationId `
            -Caller $Caller `
            -Event 'automatic_stop_deferred' `
            -Message "component=$Component port=$Port listener_pid=$listenerPid recovery_reason=$RecoveryReason reason=$boundaryReason decision_time_ct=$($DecisionTime.ToString('o')) stop_check_time_ct=$($stopCheckTime.ToString('o')) deadline_ct=$($recoveryDeadline.ToString('o')) action=preserve_listener"
        return [pscustomobject]@{
            Stopped = $false
            Reason = $boundaryReason
            ListenerProcessId = [int]$listenerPid
            StopCheckTime = $stopCheckTime
        }
    }
    if ($AllowLateSessionSalvage -and $stopCheckTime -ge $recoveryDeadline) {
        Write-MarketAppSupervisorLog `
            -ProjectRoot $ProjectRoot `
            -InvocationId $InvocationId `
            -Caller $Caller `
            -Event 'automatic_stop_late_salvage_authorized' `
            -Message "component=$Component port=$Port listener_pid=$listenerPid recovery_reason=$RecoveryReason decision_time_ct=$($DecisionTime.ToString('o')) stop_check_time_ct=$($stopCheckTime.ToString('o')) action=stop_once"
    }
    Stop-Process -Id $listenerPid -Force -ErrorAction Stop

    $quiescence = Wait-MarketAppProcessNetworkQuiescence `
        -ProcessId $listenerPid `
        -TimeoutSeconds 30
    if (-not $quiescence.Quiescent) {
        throw "$Component PID $listenerPid did not reach exact-PID network quiescence after stop: process_exists=$($quiescence.ProcessExists) tcp_connection_count=$($quiescence.TcpConnectionCount)."
    }
    Write-MarketAppSupervisorLog `
        -ProjectRoot $ProjectRoot `
        -InvocationId $InvocationId `
        -Caller $Caller `
        -Event 'automatic_stop_verified_listener' `
        -Message "component=$Component port=$Port listener_pid=$listenerPid recovery_reason=$RecoveryReason stop_check_time_ct=$($stopCheckTime.ToString('o')) process_exited=true tcp_connection_count=0 quiescence_elapsed_ms=$($quiescence.ElapsedMilliseconds) action=stopped"
    return [pscustomobject]@{
        Stopped = $true
        Reason = if ($AllowLateSessionSalvage) { 'stopped_late_session_salvage' } else { 'stopped_before_protected_opening_boundary' }
        ListenerProcessId = [int]$listenerPid
        StopCheckTime = $stopCheckTime
    }
}

function Get-MarketAppVerifiedRecorderProcessId {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$TradingDate
    )

    $pidPath = Join-Path ([System.IO.Path]::GetFullPath($ProjectRoot)) (
        "data\closing_tape\$TradingDate\recorder.pid"
    )
    if (-not (Test-Path -LiteralPath $pidPath -PathType Leaf)) {
        return $null
    }
    $recordedPid = 0
    if (-not [int]::TryParse((Get-Content -LiteralPath $pidPath -Raw).Trim(), [ref]$recordedPid)) {
        return $null
    }
    if ($recordedPid -le 0) {
        return $null
    }
    if (-not (Test-MarketAppVerifiedProcess `
        -ProcessId $recordedPid `
        -ProjectRoot $ProjectRoot `
        -RequiredCommandMarkers @('backend.closing_tape.live_recorder'))) {
        return $null
    }
    return [int]$recordedPid
}

function Get-MarketAppRecorderStatus {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$TradingDate,
        [ValidateRange(1, 3600)][int]$MaximumAgeSeconds = 90,
        [datetime]$NowUtc = [DateTime]::UtcNow
    )

    $statusPath = Join-Path ([System.IO.Path]::GetFullPath($ProjectRoot)) (
        "data\closing_tape\$TradingDate\status.json"
    )
    if (-not (Test-Path -LiteralPath $statusPath -PathType Leaf)) {
        return [pscustomobject]@{
            Healthy = $false
            State = 'missing'
            AgeSeconds = $null
            Reason = 'status file is missing'
        }
    }
    try {
        $payload = Get-Content -LiteralPath $statusPath -Raw -ErrorAction Stop | ConvertFrom-Json
        $observed = [DateTimeOffset]::Parse([string]$payload.observed_at_utc).UtcDateTime
        $age = [Math]::Max(0.0, ($NowUtc.ToUniversalTime() - $observed).TotalSeconds)
        $state = [string]$payload.session.status
        $healthy = $state -eq 'running' -and $age -le $MaximumAgeSeconds
        $reason = if ($state -ne 'running') {
            "session state is $state"
        }
        elseif ($age -gt $MaximumAgeSeconds) {
            "status is older than $MaximumAgeSeconds seconds"
        }
        else {
            $null
        }
        return [pscustomobject]@{
            Healthy = [bool]$healthy
            State = $state
            AgeSeconds = [Math]::Round($age, 1)
            Reason = $reason
        }
    }
    catch {
        return [pscustomobject]@{
            Healthy = $false
            State = 'unreadable'
            AgeSeconds = $null
            Reason = 'status file could not be parsed'
        }
    }
}

function Get-MarketAppOwnedListenerPid {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$LaunchProcessId,
        [int[]]$ListenerProcessIds
    )

    $currentPids = @(
        if ($PSBoundParameters.ContainsKey('ListenerProcessIds')) {
            $ListenerProcessIds | Sort-Object -Unique
        }
        else {
            Get-MarketAppListenerProcessIds -Port $Port
        }
    )
    # A successful launch must be the sole owner of the endpoint. Even when one
    # listener descends from our captured launch, a second listener makes
    # endpoint ownership ambiguous and must fail closed.
    if ($currentPids.Count -ne 1) {
        return $null
    }
    $listenerPid = [int]$currentPids[0]
    if (Test-MarketAppProcessDescendsFrom -ProcessId $listenerPid -AncestorProcessId $LaunchProcessId) {
        return $listenerPid
    }
    return $null
}

function Get-MarketAppEndpointContractPath {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract
    )

    if ($ExpectedEndpointContract -eq 'BackendHealth') {
        return '/health'
    }
    return '/_stcore/health'
}

function Test-MarketAppEndpointUriContract {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract
    )

    try {
        $uri = [uri]$Url
    }
    catch {
        return $false
    }
    $expectedPath = Get-MarketAppEndpointContractPath `
        -ExpectedEndpointContract $ExpectedEndpointContract
    return (
        $uri.IsAbsoluteUri -and
        $uri.Scheme -eq 'http' -and
        $uri.IsLoopback -and
        $uri.Port -eq $Port -and
        $uri.AbsolutePath -ceq $expectedPath -and
        [string]::IsNullOrEmpty($uri.Query) -and
        [string]::IsNullOrEmpty($uri.Fragment)
    )
}

function Test-MarketAppEndpointResponseContract {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract,
        [Parameter(Mandatory = $true)][psobject]$Response
    )

    if (-not (Test-MarketAppEndpointUriContract `
        -Url $Url `
        -Port $Port `
        -ExpectedEndpointContract $ExpectedEndpointContract)) {
        return $false
    }
    if ([int]$Response.StatusCode -ne 200) {
        return $false
    }

    $content = [string]$Response.Content
    if ($ExpectedEndpointContract -eq 'StreamlitHealth') {
        return $content.Trim().ToLowerInvariant() -ceq 'ok'
    }

    try {
        $payload = $content | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        return $false
    }
    if (-not $payload -or $payload -is [array]) {
        return $false
    }
    $propertyNames = @($payload.PSObject.Properties.Name)
    foreach ($requiredProperty in @(
        'status',
        'market_data_provider',
        'symbols_requested',
        'universe_provenance',
        'orb_reference_sampler'
    )) {
        if ($requiredProperty -notin $propertyNames) {
            return $false
        }
    }
    $universePropertyNames = @($payload.universe_provenance.PSObject.Properties.Name)
    $samplerPropertyNames = @($payload.orb_reference_sampler.PSObject.Properties.Name)
    return (
        -not [string]::IsNullOrWhiteSpace([string]$payload.status) -and
        ([string]$payload.market_data_provider).ToLowerInvariant() -ceq 'databento' -and
        -not ($payload.symbols_requested -is [string]) -and
        @($payload.symbols_requested | Where-Object { $_ }).Count -gt 0 -and
        'trading_date' -in $universePropertyNames -and
        'thread_alive' -in $samplerPropertyNames -and
        'interval_seconds' -in $samplerPropertyNames
    )
}

function Test-MarketAppHttpEndpointContract {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract,
        [ValidateRange(1, 30)][int]$TimeoutSeconds = 3
    )

    if (-not (Test-MarketAppEndpointUriContract `
        -Url $Url `
        -Port $Port `
        -ExpectedEndpointContract $ExpectedEndpointContract)) {
        return $false
    }
    try {
        $response = Invoke-WebRequest `
            -UseBasicParsing `
            -Uri $Url `
            -TimeoutSec $TimeoutSeconds `
            -MaximumRedirection 0 `
            -ErrorAction Stop
        return Test-MarketAppEndpointResponseContract `
            -Url $Url `
            -Port $Port `
            -ExpectedEndpointContract $ExpectedEndpointContract `
            -Response $response
    }
    catch {
        return $false
    }
}

function Wait-MarketAppOwnedEndpoint {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][ValidateRange(1, 65535)][int]$Port,
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$LaunchProcessId,
        [Parameter(Mandatory = $true)]
        [ValidateSet('BackendHealth', 'StreamlitHealth')]
        [string]$ExpectedEndpointContract,
        [ValidateRange(1, 600)][int]$TimeoutSeconds = 90,
        [ValidateRange(25, 5000)][int]$PollMilliseconds = 500
    )

    if (-not (Test-MarketAppEndpointUriContract `
        -Url $Url `
        -Port $Port `
        -ExpectedEndpointContract $ExpectedEndpointContract)) {
        throw "Endpoint URL '$Url' does not match the expected $ExpectedEndpointContract contract on port $Port."
    }

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $listenerPid = Get-MarketAppOwnedListenerPid -Port $Port -LaunchProcessId $LaunchProcessId
        if ($listenerPid -and (Test-MarketAppHttpEndpointContract `
            -Url $Url `
            -Port $Port `
            -ExpectedEndpointContract $ExpectedEndpointContract `
            -TimeoutSeconds 3)) {
            return [int]$listenerPid
        }
        Start-Sleep -Milliseconds $PollMilliseconds
    }
    return $null
}

function Get-MarketAppDescendantProcessIds {
    param([Parameter(Mandatory = $true)][int]$RootProcessId)

    $allProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    $pending = @($RootProcessId)
    $seen = @{}
    $descendants = @()
    while ($pending.Count -gt 0) {
        $parentId = [int]$pending[0]
        if ($pending.Count -gt 1) {
            $pending = @($pending[1..($pending.Count - 1)])
        }
        else {
            $pending = @()
        }
        foreach ($child in $allProcesses | Where-Object { [int]$_.ParentProcessId -eq $parentId }) {
            $childId = [int]$child.ProcessId
            if (-not $seen.ContainsKey($childId)) {
                $seen[$childId] = $true
                $descendants += $childId
                $pending += $childId
            }
        }
    }
    return @($descendants)
}

function Stop-MarketAppAttemptedLaunch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][ValidateRange(1, [int]::MaxValue)][int]$LaunchProcessId
    )

    # Stop only the captured launch and processes currently descending from it.
    $descendants = @(Get-MarketAppDescendantProcessIds -RootProcessId $LaunchProcessId)
    [array]::Reverse($descendants)
    foreach ($processId in @($descendants + $LaunchProcessId)) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

Export-ModuleMember -Function @(
    'Get-MarketAppSupervisorMutexName',
    'Enter-MarketAppSupervisorLock',
    'Exit-MarketAppSupervisorLock',
    'Write-MarketAppSupervisorLog',
    'Get-MarketAppSessionRecoveryJournalState',
    'New-MarketAppLaunchLogPaths',
    'Resolve-MarketAppPostCloseFinalizeResult',
    'Resolve-MarketAppUniverseCachePreparationResult',
    'Get-MarketAppUniversePreparationDeadline',
    'Test-MarketAppUniverseProviderDiscoveryAllowed',
    'Invoke-MarketAppBoundedChildProcess',
    'Invoke-MarketAppUniverseCachePreparation',
    'Resolve-MarketAppMissingTradingDateAction',
    'Resolve-MarketAppVerifiedCashClose',
    'Test-MarketAppBackendCurrentSessionUsable',
    'Resolve-MarketAppBackendReadinessAction',
    'Test-MarketAppDeadHandoffRecoveryEligible',
    'Test-MarketAppDeadHandoffCacheProof',
    'Resolve-MarketAppDashboardReadinessAction',
    'Get-MarketAppListenerProcessIds',
    'Test-MarketAppPortListener',
    'Assert-MarketAppExpectedListenerPid',
    'Wait-MarketAppProcessNetworkQuiescence',
    'Test-MarketAppProcessDescendsFrom',
    'Test-MarketAppVerifiedProcess',
    'Invoke-MarketAppVerifiedOrphanBackendCleanup',
    'Invoke-MarketAppBoundedAutomaticListenerStop',
    'Get-MarketAppVerifiedRecorderProcessId',
    'Get-MarketAppRecorderStatus',
    'Get-MarketAppOwnedListenerPid',
    'Test-MarketAppEndpointResponseContract',
    'Test-MarketAppHttpEndpointContract',
    'Wait-MarketAppOwnedEndpoint',
    'Stop-MarketAppAttemptedLaunch'
)
