# Retry policy for the existing, mutex-owned market-day bootstrap. This does
# not assert clock accuracy; the launcher and provider clock gates still apply.
function Get-MarketClockRecoveryDecision {
    param([Parameter(Mandatory=$true)][string]$Path,
          [datetime]$NowUtc = [datetime]::UtcNow)
    $NowUtc = $NowUtc.ToUniversalTime()
    $failures = 0
    $reason = 'INITIAL_ATTEMPT'
    if (Test-Path -LiteralPath $Path -PathType Leaf) {
        try {
            $state = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
            if ($state.schema_version -ne 'marketpin-clock-recovery.v1' -or
                $null -eq $state.consecutive_failures -or
                [int]$state.consecutive_failures -lt 0 -or
                [int]$state.consecutive_failures -gt 32) { throw 'Invalid retry state' }
            $failures = [int]$state.consecutive_failures
            if ($failures -gt 0) {
                $lastAttempt = [datetime]::Parse($state.attempted_at_utc).ToUniversalTime()
                $nextAttempt = [datetime]::Parse($state.next_attempt_at_utc).ToUniversalTime()
                $delay = ($nextAttempt - $lastAttempt).TotalSeconds
                if ($delay -lt 300 -or $delay -gt 1800) { throw 'Invalid retry interval' }
                # A backward wall-clock step must not defer repair indefinitely.
                # Invalid/future retry timestamps allow one bounded attempt;
                # they never authorize startup or establish clock health.
                if ($lastAttempt -le $NowUtc -and $nextAttempt -gt $NowUtc) {
                    return [pscustomobject]@{Allowed=$false; Failures=$failures;
                        AttemptedAtUtc=$lastAttempt.ToString('o');
                        RetryAfterSeconds=[int][math]::Ceiling(($nextAttempt-$NowUtc).TotalSeconds);
                        Reason='CLOCK_RECOVERY_BACKOFF'}
                }
            }
            $reason = 'RETRY_DUE'
        } catch {
            $failures = 0
            $reason = 'RETRY_STATE_INVALID'
        }
    }
    return [pscustomobject]@{Allowed=$true; Failures=$failures;
        RetryAfterSeconds=0; Reason=$reason}
}

function Set-MarketClockRecoveryState {
    param([Parameter(Mandatory=$true)][string]$Path,
          [ValidateRange(0,32)][int]$PreviousFailures=0,
          [switch]$Synchronized,
          [datetime]$NowUtc=[datetime]::UtcNow)
    $failures = if ($Synchronized) { 0 } else { [math]::Min(32, $PreviousFailures+1) }
    $delay = if ($Synchronized) { 0 } else { [math]::Min(1800, 300*[math]::Pow(2,[math]::Min(3,$failures-1))) }
    $state = [ordered]@{schema_version='marketpin-clock-recovery.v1';
        attempted_at_utc=$NowUtc.ToUniversalTime().ToString('o');
        consecutive_failures=$failures;
        next_attempt_at_utc=$NowUtc.ToUniversalTime().AddSeconds($delay).ToString('o');
        status=$(if ($Synchronized) {'synchronized'} else {'attempt_pending_or_failed'})}
    $absolute = [IO.Path]::GetFullPath($Path)
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($absolute)) | Out-Null
    $temporary = $absolute + '.' + [guid]::NewGuid().ToString('N') + '.tmp'
    try {
        [IO.File]::WriteAllText($temporary, ($state | ConvertTo-Json), [Text.UTF8Encoding]::new($false))
        if ([IO.File]::Exists($absolute)) { [IO.File]::Replace($temporary,$absolute,[NullString]::Value) }
        else { [IO.File]::Move($temporary,$absolute) }
    } finally {
        if ([IO.File]::Exists($temporary)) { [IO.File]::Delete($temporary) }
    }
}
Export-ModuleMember -Function Get-MarketClockRecoveryDecision,Set-MarketClockRecoveryState
