$ProjectRoot = Split-Path -Parent $PSScriptRoot
$EnsurePath = Join-Path $ProjectRoot 'ensure_market_app.ps1'
$EnsureSource = [System.IO.File]::ReadAllText($EnsurePath)
$tokens = $null
$parseErrors = $null
$EnsureAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $EnsurePath,
    [ref]$tokens,
    [ref]$parseErrors
)

if ($parseErrors.Count -gt 0) {
    throw "ensure_market_app.ps1 did not parse cleanly: $($parseErrors -join '; ')"
}

$bestEffortFunction = @(
    $EnsureAst.FindAll(
        {
            param($node)
            $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $node.Name -eq 'Write-WatchdogLogBestEffort'
        },
        $true
    )
)

Describe 'ensure_market_app terminal observability' {
    It 'keeps terminal logging best effort so it cannot replace the real result' {
        $bestEffortFunction.Count | Should Be 1
        $probe = & {
            param([string]$FunctionSource)
            Invoke-Expression $FunctionSource
            function Write-WatchdogLog { throw 'synthetic logging failure' }

            $loggingEscaped = $false
            try {
                Write-WatchdogLogBestEffort -Event 'invocation_completed' -Message 'synthetic'
            }
            catch {
                $loggingEscaped = $true
            }

            $observedMessage = $null
            try {
                try {
                    throw 'original invocation failure'
                }
                catch {
                    Write-WatchdogLogBestEffort -Event 'invocation_failed' -Message 'synthetic'
                    throw
                }
            }
            catch {
                $observedMessage = $_.Exception.Message
            }
            return [pscustomobject]@{
                LoggingEscaped = $loggingEscaped
                ObservedMessage = $observedMessage
            }
        } $bestEffortFunction[0].Extent.Text

        $probe.LoggingEscaped | Should Be $false
        $probe.ObservedMessage | Should Match 'original invocation failure'
    }

    It 'records one normalized terminal event from an outer finally' {
        ([regex]::Matches($EnsureSource, "-Event 'invocation_completed'")).Count | Should Be 1
        $EnsureSource | Should Match "(?s)catch\s*\{\s*\`$invocationErrorType\s*=.*?Write-WatchdogLogBestEffort.*?-Event 'invocation_failed'.*?normalized_exit_code=1.*?throw\s*\}\s*finally\s*\{\s*Write-WatchdogLogBestEffort.*?-Event 'invocation_completed'.*?outcome=\`$invocationOutcome.*?terminal_path=\`$invocationTerminalPath.*?error_type=\`$invocationErrorType.*?normalized_exit_code=\`$normalizedExitCode.*?\}\s*$"
    }

    It 'normalizes both clean early exits to zero before exiting' {
        ([regex]::Matches($EnsureSource, '(?m)^\s*exit 0\s*$')).Count | Should Be 2
        $EnsureSource | Should Match "(?s)-Event 'outside_watch_window'.*?\`$invocationOutcome\s*=\s*'noop'.*?\`$invocationTerminalPath\s*=\s*'outside_watch_window'.*?\`$normalizedExitCode\s*=\s*0\s*exit 0"
        $EnsureSource | Should Match "(?s)-Event 'supervisor_busy'.*?\`$invocationOutcome\s*=\s*'noop'.*?\`$invocationTerminalPath\s*=\s*'supervisor_busy'.*?\`$normalizedExitCode\s*=\s*0\s*exit 0"
    }

    It 'does not force an exit from either finally block' {
        $finallyBlocks = @(
            $EnsureAst.FindAll(
                {
                    param($node)
                    $node -is [System.Management.Automation.Language.TryStatementAst] -and
                        $null -ne $node.Finally
                },
                $true
            ) | ForEach-Object { $_.Finally.Extent.Text }
        )
        $finallyBlocks.Count | Should BeGreaterThan 1
        foreach ($finallyBlock in $finallyBlocks) {
            $finallyBlock | Should Not Match '(?im)^\s*exit(?:\s|$)'
        }
    }

    It 'logs supervisor lock release only after the release call succeeds' {
        $releaseCall = $EnsureSource.LastIndexOf('Exit-MarketAppSupervisorLock -LockHandle $supervisorLock')
        $releaseEvent = $EnsureSource.LastIndexOf("-Event 'supervisor_lock_released'")
        $releaseCall | Should BeGreaterThan -1
        $releaseEvent | Should BeGreaterThan $releaseCall
    }
}
