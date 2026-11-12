$ProjectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Force

Describe 'Unresolved Windows listener ownership' {
    InModuleScope market_app_supervisor {
        It 'preserves an occupied port when the OS omits its listener owner' {
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Test-MarketAppTcpPortReachable { $true }
            Test-MarketAppPortListener -Port 8501 | Should Be $true
        }

        It 'reports a port absent only when inventory and connection both fail' {
            Mock Get-MarketAppListenerProcessIds { @() }
            Mock Test-MarketAppTcpPortReachable { $false }
            Test-MarketAppPortListener -Port 8501 | Should Be $false
        }

        It 'preserves a PID zero listener without needing a connection probe' {
            Mock Get-MarketAppListenerProcessIds { @(0) }
            Mock Test-MarketAppTcpPortReachable { throw 'Unexpected probe' }
            Test-MarketAppPortListener -Port 8000 | Should Be $true
            Assert-MockCalled Test-MarketAppTcpPortReachable -Times 0 -Exactly -Scope It
        }

        It 'treats a PID zero listener as unverified without querying a process' {
            Mock Get-MarketAppProcessRecord { throw 'PID zero must not be queried' }
            Test-MarketAppVerifiedProcess -ProcessId 0 -ProjectRoot 'C:\MarketPin' `
                -RequiredCommandMarkers @('server.py') | Should Be $false
            Assert-MockCalled Get-MarketAppProcessRecord -Times 0 -Exactly
        }

        It 'does not claim an unresolved listener belongs to a captured launch' {
            Mock Test-MarketAppProcessDescendsFrom { throw 'No resolvable owner' }
            Get-MarketAppOwnedListenerPid -Port 8000 -LaunchProcessId 1234 `
                -ListenerProcessIds @(0) | Should BeNullOrEmpty
            Assert-MockCalled Test-MarketAppProcessDescendsFrom -Times 0 -Exactly
        }

        It 'does not discard an unresolved listener beside a known owner' {
            Mock Test-MarketAppProcessDescendsFrom { $true }
            Get-MarketAppOwnedListenerPid -Port 8000 -LaunchProcessId 1234 `
                -ListenerProcessIds @(0, 1234) | Should BeNullOrEmpty
            Assert-MockCalled Test-MarketAppProcessDescendsFrom -Times 0 -Exactly
        }

        It 'continues to reject PID zero as a stop target' {
            Mock Stop-Process { throw 'No process should be stopped' }
            { Assert-MarketAppExpectedListenerPid -Port 8000 -ExpectedPid 0 `
                -ListenerProcessIds @(0) } | Should Throw
            Assert-MockCalled Stop-Process -Times 0 -Exactly
        }
    }
}
