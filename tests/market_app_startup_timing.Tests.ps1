$ProjectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Force
$tokens = $null
$parseErrors = $null
$ensureAst = [System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $ProjectRoot 'ensure_market_app.ps1'), [ref]$tokens, [ref]$parseErrors
)
if (@($parseErrors).Count) { throw 'ensure_market_app.ps1 did not parse cleanly.' }

# Define test doubles only. Never dot-source a launcher or touch a live owner.
function Assert-OpeningAcceptanceMutationPreflight { param([switch]$AllowSchemaBootstrap) }
function Write-WatchdogLog { param($Event, $Message) }
function Invoke-MarketAppCurrentDayUniversePrestage {
    param($ProjectRootPath, $PythonPath, $Symbols, $ExpectedContractCap, $Now, $NotAfter, $PrestageInvocationId)
}
function Invoke-BackendPreparedAutomaticListenerStop {
    param($ExpectedPid, $DecisionTime, $PreparationDeadline, $ExpectedTradingDate,
          $ExpectedContractCap, $RecoveryReason, [switch]$RequireCurrentDay)
}

function Get-PreparationCallSiteScript {
    param([string]$ClockVariable)
    $block = @($ensureAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.StatementBlockAst] -and
        @($node.Statements | Where-Object {
            $_ -is [System.Management.Automation.Language.AssignmentStatementAst] -and
            $_.Left.Extent.Text -eq ('$' + $ClockVariable)
        }).Count -eq 1
    }, $true))[0]
    if (-not $block) { throw "Missing preparation clock: $ClockVariable" }
    $statements = @($block.Statements)
    $clockIndex = 0
    while ($statements[$clockIndex].Extent.Text -notmatch ('^\$' + $ClockVariable + '\s*=')) {
        $clockIndex++
    }
    $firstIndex = $clockIndex
    if ($clockIndex -gt 0 -and $statements[$clockIndex - 1].Extent.Text -match '^Assert-OpeningAcceptanceMutationPreflight') {
        $firstIndex--
    }
    $lastIndex = $clockIndex
    while ($statements[$lastIndex].Extent.Text -notmatch 'Invoke-BackendPreparedAutomaticListenerStop') {
        $lastIndex++
    }
    return [scriptblock]::Create(($statements[$firstIndex..$lastIndex].Extent.Text -join "`n"))
}

Describe 'Preparation deadlines exclude completed opening preflight work' {
    BeforeEach {
        $script:testClock = [datetime]'2026-09-09T07:00:00'
        $script:preflightSucceeded = $false
        $script:preparationStartedAt = $null
        $script:remainingPreparationSeconds = $null
        $script:PythonExe = 'python.exe'
        $script:InvocationId = 'timing-test'
        Mock Get-Date { $script:testClock }
        Mock Assert-OpeningAcceptanceMutationPreflight {
            if (-not $script:preflightSucceeded) {
                # September 8 observed a 74-second source/schema preflight.
                $script:testClock = $script:testClock.AddSeconds(74)
                $script:preflightSucceeded = $true
            }
        }
        Mock Write-WatchdogLog {}
    }

    It 'starts the 07:00 discovery clock after the source/schema preflight' {
        Mock Invoke-MarketAppCurrentDayUniversePrestage {
            $script:preparationStartedAt = $Now
        }
        $branch = @($ensureAst.FindAll({
            param($node)
            $node -is [System.Management.Automation.Language.IfStatementAst] -and
            $node.Clauses[0].Item1.Extent.Text -eq '$PrepareUniverseOnly' -and
            $node.Extent.Text -match '\$prestageNow = Get-Date'
        }, $true))[0]
        $body = $branch.Clauses[0].Item2.Extent.Text
        & ([scriptblock]::Create($body.Substring(1, $body.Length - 2)))

        $script:preparationStartedAt | Should Be ([datetime]'2026-09-09T07:01:14')
    }

    It 'does not extend 07:40 when preflight crosses the protected prestage cutoff' {
        $script:testClock = [datetime]'2026-09-09T07:39:00'
        Mock Invoke-MarketAppCurrentDayUniversePrestage {
            if ($Now -ge $NotAfter) { throw 'protected prestage cutoff reached' }
        }
        $branch = @($ensureAst.FindAll({
            param($node)
            $node -is [System.Management.Automation.Language.IfStatementAst] -and
            $node.Clauses[0].Item1.Extent.Text -eq '$PrepareUniverseOnly' -and
            $node.Extent.Text -match '\$prestageNow = Get-Date'
        }, $true))[0]
        $body = $branch.Clauses[0].Item2.Extent.Text

        { & ([scriptblock]::Create($body.Substring(1, $body.Length - 2))) } |
            Should Throw 'protected prestage cutoff reached'
    }

    foreach ($clockVariable in @('stalePreparationNow', 'rutPreparationNow', 'refreshPreparationNow')) {
        It "retains the full discovery budget at $clockVariable after preflight" {
            $script:testClock = [datetime]'2026-09-09T07:45:00'
            $backendPids = @(44224)
            $expectedTradingDate = '2026-09-09'
            $now = $script:testClock
            $rutUpgradeDeadline = $now.Date.AddHours(8).AddMinutes(25)
            Mock Invoke-BackendPreparedAutomaticListenerStop {
                # This helper also performs the invocation-cached preflight.
                Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap
                $script:remainingPreparationSeconds = ($PreparationDeadline - $script:testClock).TotalSeconds
            }
            & (Get-PreparationCallSiteScript -ClockVariable $clockVariable)

            $script:remainingPreparationSeconds | Should Be 285
        }

        It "retains the 08:25 hard cutoff at $clockVariable after a late preflight" {
            $script:testClock = [datetime]'2026-09-09T08:23:30'
            $backendPids = @(44224)
            $expectedTradingDate = '2026-09-09'
            $now = $script:testClock
            $rutUpgradeDeadline = $now.Date.AddHours(8).AddMinutes(25)
            Mock Invoke-BackendPreparedAutomaticListenerStop {
                Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap
                $script:preparationStartedAt = $PreparationDeadline
                $script:remainingPreparationSeconds = ($PreparationDeadline - $script:testClock).TotalSeconds
            }
            & (Get-PreparationCallSiteScript -ClockVariable $clockVariable)

            $script:preparationStartedAt | Should Be ([datetime]'2026-09-09T08:25:00')
            $script:remainingPreparationSeconds | Should Be 16
        }
    }
}
