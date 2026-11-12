$ProjectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Force
$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $ProjectRoot 'start_databento_app.ps1'), [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw 'Launcher did not parse' }
$definition = $ast.FindAll({ param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Stop-MarketPinProcesses'
}, $true)
Invoke-Expression $definition[0].Extent.Text
function Assert-OpeningAcceptanceMutationPreflight { }
function Write-StartupSupervisorLog { param($Event, $Message) }

Describe 'Manual launcher listener and parent stop order' {
    BeforeEach {
        $script:stopped = @()
        $script:remainingListeners = @{8000=10;8501=20}
        $script:unrelated = $false
        $script:reused = $false
        $script:createdAt = [datetime]'2026-10-26T07:45:00Z'
        Mock Assert-OpeningAcceptanceMutationPreflight { }
        Mock Get-CimInstance {
            if ($Filter) {
                [pscustomobject]@{ProcessId=[int]($Filter -replace 'ProcessId=', '');CreationDate=$script:createdAt}
            }
            else {
                @([pscustomobject]@{ProcessId=100;CreationDate=$script:createdAt}, [pscustomobject]@{ProcessId=200;CreationDate=$script:createdAt})
            }
        }
        Mock Test-MarketAppDirectComponentProcessRecord { $true }
        Mock Get-MarketAppListenerProcessIds {
            if ($script:remainingListeners.ContainsKey($Port)) { $script:remainingListeners[$Port] }
        }
        Mock Test-MarketAppVerifiedProcess { -not ($script:unrelated -and $ProcessId -eq 20) }
        Mock Get-MarketAppProcessIdentityTokenById {
            if ($script:reused -and $ProcessId -eq 10) { 'reused' }
            else { "$ProcessId|$($script:createdAt.ToUniversalTime().Ticks)" }
        }
        Mock Stop-Process {
            $script:stopped += $Id
            if ($Id -eq 10) { $script:remainingListeners.Remove(8000) }
            if ($Id -eq 20) { $script:remainingListeners.Remove(8501) }
        }
        Mock Wait-MarketAppProcessNetworkQuiescence {
            [pscustomobject]@{Quiescent=$true;ElapsedMilliseconds=1}
        }
    }

    It 'stops actual listeners before venv parents while ancestry is intact' {
        Stop-MarketPinProcesses -Root $ProjectRoot
        ($script:stopped -join ',') | Should Be '10,20,100,200'
    }

    It 'preserves every process when any initial listener is unverified' {
        $script:unrelated = $true
        { Stop-MarketPinProcesses -Root $ProjectRoot } | Should Throw 'not a verified MarketPinPredictor process'
        $script:stopped.Count | Should Be 0
    }

    It 'refuses a PID reused after the initial ownership check' {
        $script:reused = $true
        { Stop-MarketPinProcesses -Root $ProjectRoot } | Should Throw 'creation-time identity changed'
        $script:stopped.Count | Should Be 0
    }
}
