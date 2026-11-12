$ProjectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Force

Describe 'Independent recorder coexistence during backend recovery' {
    InModuleScope market_app_supervisor {


        It 'preserves the independently verified recorder without stopping any process' {
            Mock Get-MarketAppVerifiedRecorderProcessId { 5000 }
            Mock Test-MarketAppVerifiedProcess {
                param($ProcessId, $RequiredCommandMarkers)
                $ProcessId -eq 5001 -and $RequiredCommandMarkers -contains 'backend.closing_tape.live_recorder'
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                $ProcessId -eq 5001 -and $AncestorProcessId -eq 5000
            }
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(5001) }
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            $result = Invoke-MarketAppVerifiedOrphanBackendCleanup -ProjectRoot 'C:\MarketPinPredictor' -InvocationId test -Caller Pester
            $result.Cleaned | Should Be $false
            @($result.PreservedRecorderProcessIds) | Should Be @(5001)
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'still refuses another unknown provider consumer beside the verified recorder' {
            Mock Get-MarketAppVerifiedRecorderProcessId { 5000 }
            Mock Test-MarketAppVerifiedProcess {
                param($ProcessId, $RequiredCommandMarkers)
                $ProcessId -eq 5001 -and $RequiredCommandMarkers -contains 'backend.closing_tape.live_recorder'
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                $ProcessId -eq 5001 -and $AncestorProcessId -eq 5000
            }
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(5001) }
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(5001, 9999) }
            { Invoke-MarketAppVerifiedOrphanBackendCleanup -ProjectRoot 'C:\MarketPinPredictor' -InvocationId test -Caller Pester } | Should Throw 'unverified owner PID 9999'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'does not trust a recorder PID when no current-day root can be verified' {
            Mock Get-MarketAppVerifiedRecorderProcessId { 5000 }
            Mock Test-MarketAppVerifiedProcess {
                param($ProcessId, $RequiredCommandMarkers)
                $ProcessId -eq 5001 -and $RequiredCommandMarkers -contains 'backend.closing_tape.live_recorder'
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                $ProcessId -eq 5001 -and $AncestorProcessId -eq 5000
            }
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(5001) }
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Get-MarketAppVerifiedRecorderProcessId { $null }
            { Invoke-MarketAppVerifiedOrphanBackendCleanup -ProjectRoot 'C:\MarketPinPredictor' -InvocationId test -Caller Pester } | Should Throw 'unverified owner PID 5001'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'does not trust a recorder-looking owner outside the verified recorder tree' {
            Mock Get-MarketAppVerifiedRecorderProcessId { 5000 }
            Mock Test-MarketAppVerifiedProcess {
                param($ProcessId, $RequiredCommandMarkers)
                $ProcessId -eq 5001 -and $RequiredCommandMarkers -contains 'backend.closing_tape.live_recorder'
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                $ProcessId -eq 5001 -and $AncestorProcessId -eq 5000
            }
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(5001) }
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Test-MarketAppProcessDescendsFrom { $false }
            { Invoke-MarketAppVerifiedOrphanBackendCleanup -ProjectRoot 'C:\MarketPinPredictor' -InvocationId test -Caller Pester } | Should Throw 'unverified owner PID 5001'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }

        It 'does not trust a child whose canonical recorder command cannot be verified' {
            Mock Get-MarketAppVerifiedRecorderProcessId { 5000 }
            Mock Test-MarketAppVerifiedProcess {
                param($ProcessId, $RequiredCommandMarkers)
                $ProcessId -eq 5001 -and $RequiredCommandMarkers -contains 'backend.closing_tape.live_recorder'
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                $ProcessId -eq 5001 -and $AncestorProcessId -eq 5000
            }
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Get-MarketAppDirectBackendProcessIdentities { @() }
            Mock Get-MarketAppRemoteTcpOwnerProcessIds { @(5001) }
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Test-MarketAppVerifiedProcess { $false }
            { Invoke-MarketAppVerifiedOrphanBackendCleanup -ProjectRoot 'C:\MarketPinPredictor' -InvocationId test -Caller Pester } | Should Throw 'unverified owner PID 5001'
            Assert-MockCalled Stop-Process -Times 0 -Scope It
        }
        It 'cleans only backend owners while preserving and rechecking the recorder' {
            Mock Get-MarketAppVerifiedRecorderProcessId { 5000 }
            Mock Test-MarketAppVerifiedProcess {
                param($ProcessId, $RequiredCommandMarkers)
                ($ProcessId -eq 5001 -and $RequiredCommandMarkers -contains 'backend.closing_tape.live_recorder') -or
                ($ProcessId -eq 4000 -and $RequiredCommandMarkers -contains 'server.py')
            }
            Mock Test-MarketAppProcessDescendsFrom {
                param($ProcessId, $AncestorProcessId)
                ($ProcessId -eq 5001 -and $AncestorProcessId -eq 5000) -or
                ($ProcessId -eq 4000 -and $AncestorProcessId -eq 4000)
            }
            Mock Get-MarketAppListenerProcessIds { @() }
            $script:backendCalls = 0
            Mock Get-MarketAppDirectBackendProcessIdentities {
                $script:backendCalls++
                if ($script:backendCalls -eq 1) { [pscustomobject]@{ProcessId=4000} }
            }
            $script:ownerCalls = 0
            Mock Get-MarketAppRemoteTcpOwnerProcessIds {
                $script:ownerCalls++
                if ($script:ownerCalls -eq 1) { @(4000,5001) } else { @(5001) }
            }
            Mock Get-MarketAppDescendantProcessIds { @() }
            Mock Get-MarketAppProcessRecord {
                [pscustomobject]@{ProcessId=4000; ParentProcessId=100; CreationDate=[datetime]'2026-09-30T07:45:00'; CommandLine='verified backend'; ExecutablePath='C:\MarketPinPredictor\.venv\Scripts\python.exe'}
            }
            Mock Write-MarketAppSupervisorLog {}
            Mock Stop-Process {}
            Mock Wait-MarketAppProcessNetworkQuiescence { [pscustomobject]@{Quiescent=$true;ProcessExists=$false;TcpConnectionCount=0;ElapsedMilliseconds=1} }
            $result = Invoke-MarketAppVerifiedOrphanBackendCleanup -ProjectRoot 'C:\MarketPinPredictor' -InvocationId test -Caller Pester
            $result.Cleaned | Should Be $true
            @($result.ProcessIds) | Should Be @(4000)
            @($result.PreservedRecorderProcessIds) | Should Be @(5001)
            Assert-MockCalled Stop-Process -Times 1 -Scope It -ParameterFilter { $Id -eq 4000 }
            Assert-MockCalled Stop-Process -Times 0 -Scope It -ParameterFilter { $Id -eq 5001 }
            Assert-MockCalled Get-MarketAppVerifiedRecorderProcessId -Times 3 -Scope It
        }
    }
}
