$ProjectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Force

Describe 'Exact loopback server ownership' {
    InModuleScope market_app_supervisor {
        It 'matches only the server half of the held four-tuple' {
            $rows = @(
                ' TCP 127.0.0.1:8000 127.0.0.1:50001 ESTABLISHED 1234',
                ' TCP 127.0.0.1:50001 127.0.0.1:8000 ESTABLISHED 7777',
                ' TCP 127.0.0.1:8000 127.0.0.1:50002 ESTABLISHED 8888',
                ' TCP 10.0.0.1:8000 127.0.0.1:50001 ESTABLISHED 9999',
                ' TCP 127.0.0.1:8000 127.0.0.1:50001 TIME_WAIT 0'
            )
            $owners = @(Resolve-MarketAppLoopbackServerOwners -Lines $rows -ServerPort 8000 -ClientPort 50001)
            $owners.Count | Should Be 1
            $owners[0] | Should Be 1234
        }

        It 'preserves ambiguity so the caller cannot accept conflicting owners' {
            $rows = @(' TCP 127.0.0.1:8000 127.0.0.1:50001 ESTABLISHED 1234',
                      ' TCP 127.0.0.1:8000 127.0.0.1:50001 ESTABLISHED 5678')
            @(Resolve-MarketAppLoopbackServerOwners -Lines $rows -ServerPort 8000 -ClientPort 50001).Count | Should Be 2
        }

        It 'does not turn empty or unrelated rows into ownership' {
            @(Resolve-MarketAppLoopbackServerOwners -Lines @() -ServerPort 8000 -ClientPort 50001).Count | Should Be 0
            @(Resolve-MarketAppLoopbackServerOwners -Lines @(' TCP 0.0.0.0:8000 0.0.0.0:0 LISTENING 12') -ServerPort 8000 -ClientPort 50001).Count | Should Be 0
        }

        It 'finds the owner of a real held local socket even when listeners are omitted' {
            $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
            try {
                $listener.Start()
                $owners = @(Get-MarketAppLoopbackServerProcessIds -Port $listener.LocalEndpoint.Port)
                $owners.Count | Should Be 1
                $owners[0] | Should Be $PID
            }
            finally { $listener.Stop() }
        }

        It 'does not authorize a foreign process as our launch' {
            Mock Test-MarketAppProcessDescendsFrom { $false }
            Get-MarketAppOwnedListenerPid -Port 8000 -LaunchProcessId 1111 -ListenerProcessIds @(2222) | Should BeNullOrEmpty
        }
    }
}
