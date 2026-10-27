# Offline only: every OS process, listener and HTTP operation is mocked.
$modulePath=Join-Path (Split-Path $PSScriptRoot -Parent) 'tools\forecast_research_supervisor.psm1'
Import-Module (Join-Path (Split-Path $PSScriptRoot -Parent) 'market_app_supervisor.psm1') -Force
$tokens=$null
$parseErrors=$null
$ast=[System.Management.Automation.Language.Parser]::ParseFile($modulePath,[ref]$tokens,[ref]$parseErrors)
if ($parseErrors) { throw ($parseErrors -join '; ') }
foreach ($definition in $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true)) {
    Invoke-Expression $definition.Extent.Text
}

Describe 'Research listener inspection' {
    It 'uses the shared netstat lookup without CIM TCP enumeration' {
        Mock Import-Module { }
        Mock Get-MarketAppListenerProcessIds { @(42) }
        Mock Get-NetTCPConnection { throw 'CIM TCP lookup must not run' }
        @(Get-ResearchListeners -Root $TestDrive).Count | Should Be 1
        @(Get-ResearchListeners -Root $TestDrive)[0] | Should Be 42
        Assert-MockCalled Get-MarketAppListenerProcessIds -Scope It -Times 2 -Exactly -ParameterFilter { $Port -eq 8001 }
        Assert-MockCalled Get-NetTCPConnection -Scope It -Times 0 -Exactly
    }
}

Describe 'Independent research launch and supervision' {
        BeforeEach {
            $script:root = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
            New-Item -ItemType Directory -Path $script:root -Force | Out-Null
            $script:oldEnabled = $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART = '1'
            $script:started = $false
            $script:earlyExit = $false
            $script:noListener = $false
            $script:existing = $false
            $script:recordMissing = $false
            $script:launchTime = [datetime]::UtcNow
            $script:identity = [pscustomobject]@{loaded_source_sha256=('a'*64);source_identity_sha256=('b'*64)}
            $script:health = [pscustomobject]@{
                schema_version='marketpin-market-research.v1'; health_schema_version='marketpin-market-research-health.v1'
                component='forecast_research';process_alive=$true;process_id=42;instance_id=('c'*32)
                loaded_source_sha256=('a'*64);source_identity_sha256=('b'*64);source_database_access='query_only'
                worker=[pscustomobject]@{status='WAITING';thread_alive=$true;completed_capture_count=0}
            }
            Mock Enter-ResearchLock { [pscustomobject]@{Acquired=$true} }
            Mock Exit-ResearchLock { }
            Mock Get-ResearchExpectedIdentity { $script:identity }
            Mock Get-ResearchHealth { $script:health }
            Mock Get-ResearchListeners { if (($script:started -or $script:existing) -and -not $script:noListener) { 42 } }
            Mock Get-ResearchProcess {
                if (-not $script:recordMissing) {
                    [pscustomobject]@{ProcessId=42;ParentProcessId=1;ExecutablePath=(Join-Path $script:root '.venv\Scripts\python.exe')
                        CommandLine=('python.exe "' + (Join-Path $script:root 'tools\run_forecast_research.py') + '" --port 8001')
                        CreationDate=$script:launchTime}
                }
            }
            Mock Start-Process {
                $script:started=$true
                $script:lastStdout=$RedirectStandardOutput
                $script:lastStderr=$RedirectStandardError
                [pscustomobject]@{Id=42;Handle=123;HasExited=$script:earlyExit;ExitCode=1;StartTime=$script:launchTime}
            }
            Mock Start-Sleep { }
            Mock Invoke-RestMethod { throw 'Unexpected real HTTP call' }
            Mock Get-NetTCPConnection { throw 'Unexpected real listener lookup' }
            Mock Get-CimInstance { throw 'Unexpected real process lookup' }
            Mock Stop-Process { throw 'Must never stop a process' }
            Mock Stop-ResearchOwnedProcess { throw 'Unexpected reload' }
        }
        AfterEach {
            Assert-MockCalled Stop-Process -Scope It -Times 0 -Exactly
            Assert-MockCalled Invoke-RestMethod -Scope It -Times 0 -Exactly
            Assert-MockCalled Get-NetTCPConnection -Scope It -Times 0 -Exactly
            Assert-MockCalled Get-CimInstance -Scope It -Times 0 -Exactly
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART = $script:oldEnabled
        }
        It 'launches missing service with canonical interpreter and owned ready endpoint' {
            $result=Invoke-ForecastResearchEnsure -Root $script:root
            $result.Status | Should Be 'READY'
            Assert-MockCalled Start-Process -Scope It -Times 1 -Exactly -ParameterFilter {
                $FilePath -eq (Join-Path $script:root '.venv\Scripts\python.exe') -and $WindowStyle -eq 'Hidden'
            }
            $result.Stderr | Should Match '\d{8}T\d{9}Z-[a-f0-9]{32}\.stderr\.log$'
            Assert-MockCalled Exit-ResearchLock -Scope It -Times 1 -Exactly
        }
        It 'preserves an already running owned service without a second launch' {
            $script:existing=$true
            (Invoke-ForecastResearchEnsure -Root $script:root -Supervised).Status | Should Be 'READY'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'honors an explicit disable before any process or endpoint inspection' {
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART='0'
            (Invoke-ForecastResearchEnsure -Root $script:root).Status | Should Be 'DISABLED'
            Assert-MockCalled Get-ResearchListeners -Scope It -Times 0 -Exactly
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'requires opt-in for the existing watchdog' {
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART=$null
            (Invoke-ForecastResearchEnsure -Root $script:root -Supervised).Code | Should Be 'RESEARCH_SUPERVISION_NOT_ENABLED'
            Assert-MockCalled Enter-ResearchLock -Scope It -Times 0 -Exactly
        }
        It 'accepts a persisted scoped activation for a watchdog with no inherited setting' {
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART=$null
            New-Item -ItemType Directory -Path (Join-Path $script:root 'config') | Out-Null
            '{"schema_version":"marketpin-research-runtime.v1","supervision_enabled":true}' |
                Set-Content (Join-Path $script:root 'config\forecast_research_runtime.json')
            (Invoke-ForecastResearchEnsure -Root $script:root -Supervised).Status | Should Be 'READY'
        }
        It 'lets an explicit disable override the persisted activation' {
            New-Item -ItemType Directory -Path (Join-Path $script:root 'config') | Out-Null
            '{"schema_version":"marketpin-research-runtime.v1","supervision_enabled":true}' |
                Set-Content (Join-Path $script:root 'config\forecast_research_runtime.json')
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART='0'
            (Invoke-ForecastResearchEnsure -Root $script:root -Supervised).Status | Should Be 'DISABLED'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'rejects string truthiness and an unknown activation schema' {
            $env:MARKETPIN_FORECAST_RESEARCH_AUTOSTART=$null
            New-Item -ItemType Directory -Path (Join-Path $script:root 'config') | Out-Null
            foreach ($body in @('{"schema_version":"marketpin-research-runtime.v1","supervision_enabled":"true"}',
                                '{"schema_version":"unknown","supervision_enabled":true}')) {
                $body | Set-Content (Join-Path $script:root 'config\forecast_research_runtime.json')
                (Invoke-ForecastResearchEnsure -Root $script:root -Supervised).Status | Should Be 'DISABLED'
            }
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'serializes concurrent attempts before inspecting or launching' {
            Mock Enter-ResearchLock { [pscustomobject]@{Acquired=$false} }
            (Invoke-ForecastResearchEnsure -Root $script:root).Status | Should Be 'BUSY'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
            Assert-MockCalled Exit-ResearchLock -Scope It -Times 1 -Exactly
        }
        It 'refuses a foreign port owner without stopping anything' {
            $script:existing=$true
            Mock Get-ResearchProcess { [pscustomobject]@{ExecutablePath='C:\foreign\server.exe';CommandLine='foreign'} }
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_FOREIGN_PORT_OWNER'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'rejects an unrelated python script mentioning the entry path' {
            $record=[pscustomobject]@{ExecutablePath=(Join-Path $script:root '.venv\Scripts\python.exe');CommandLine=('python.exe other.py "' + (Join-Path $script:root 'tools\run_forecast_research.py') + '"');ParentProcessId=1}
            (Test-ResearchProcess $record $script:root) | Should Be $false
        }
        It 'rejects a different loaded code identity' {
            $script:existing=$true
            $script:health.loaded_source_sha256='d'*64
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_ENDPOINT_IDENTITY_MISMATCH'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'rejects the wrong source identity or health PID' {
            $script:existing=$true
            $script:health.source_identity_sha256='d'*64
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_ENDPOINT_IDENTITY_MISMATCH'
            $script:health.source_identity_sha256='b'*64
            $script:health.process_id=99
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_ENDPOINT_IDENTITY_MISMATCH'
        }
        It 'reloads only the verified endpoint when explicitly requested' {
            $script:existing=$true
            Mock Get-Process {
                $fake=[pscustomobject]@{Id=42;StartTime=$script:launchTime;Handle=123}
                $fake | Add-Member ScriptMethod Dispose { }
                $fake
            }
            Mock Stop-ResearchOwnedProcess { $script:existing=$false }
            $result=Invoke-ForecastResearchEnsure -Root $script:root -ReloadOwned
            $result.Status | Should Be 'READY'
            Assert-MockCalled Start-Process -Scope It -Times 1 -Exactly
            Assert-MockCalled Stop-ResearchOwnedProcess -Scope It -Times 1 -Exactly
            @(Get-ChildItem (Join-Path $script:root 'output\forecast_research') -Filter '*.reload-before.json').Count | Should Be 1
        }
        It 'refuses an explicit reload when the service source identity is wrong' {
            $script:existing=$true
            $script:health.source_identity_sha256='d'*64
            { Invoke-ForecastResearchEnsure -Root $script:root -ReloadOwned } | Should Throw 'RESEARCH_RELOAD_IDENTITY_UNPROVEN'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'refuses an explicit reload after PID reuse' {
            $script:existing=$true
            Mock Get-Process {
                $fake=[pscustomobject]@{Id=42;StartTime=$script:launchTime.AddSeconds(1);Handle=123}
                $fake | Add-Member ScriptMethod Dispose { }
                $fake
            }
            { Invoke-ForecastResearchEnsure -Root $script:root -ReloadOwned } | Should Throw 'RESEARCH_RELOAD_PROCESS_CHANGED'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'reports a stalled worker without spawning over its process' {
            $script:existing=$true
            $script:health.worker.status='STALLED'
            (Invoke-ForecastResearchEnsure -Root $script:root).Status | Should Be 'DEGRADED'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'records early exit and applies backoff to the next attempt' {
            $script:earlyExit=$true
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_EARLY_EXIT'
            $script:recordMissing=$true
            $script:started=$false
            (Invoke-ForecastResearchEnsure -Root $script:root).Status | Should Be 'BACKOFF'
            Assert-MockCalled Start-Process -Scope It -Times 1 -Exactly
        }
        It 'bounds a readiness timeout and remembers an unbound living process' {
            $script:noListener=$true
            { Invoke-ForecastResearchEnsure -Root $script:root -ReadinessSeconds 1 } | Should Throw 'RESEARCH_READINESS_TIMEOUT'
            (Invoke-ForecastResearchEnsure -Root $script:root).Code | Should Be 'RESEARCH_PENDING_PROCESS'
            Assert-MockCalled Start-Process -Scope It -Times 1 -Exactly
        }
        It 'fails preflight before launch and backs off even on import failure' {
            Mock Get-ResearchExpectedIdentity { throw 'RESEARCH_PREFLIGHT_FAILED' }
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_PREFLIGHT_FAILED'
            (Invoke-ForecastResearchEnsure -Root $script:root).Status | Should Be 'BACKOFF'
            Assert-MockCalled Start-Process -Scope It -Times 0 -Exactly
        }
        It 'keeps a foreign listener that wins the bind race intact' {
            Mock Get-ResearchProcess { [pscustomobject]@{ExecutablePath='C:\foreign\server.exe';CommandLine='foreign';ProcessId=99;ParentProcessId=1} }
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_FOREIGN_PORT_OWNER'
            Assert-MockCalled Start-Process -Scope It -Times 1 -Exactly
        }
        It 'retains distinct launch logs across retries and increases delay' {
            $script:earlyExit=$true
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_EARLY_EXIT'
            $first=$script:lastStderr
            $statePath=Join-Path $script:root 'output\forecast_research\retry-state.json'
            $saved=Get-Content $statePath -Raw | ConvertFrom-Json
            $saved.NextAttemptUtc=[datetime]::UtcNow.AddSeconds(-1).ToString('o')
            $saved.LaunchPid=0
            $saved | ConvertTo-Json | Set-Content $statePath
            $script:started=$false
            { Invoke-ForecastResearchEnsure -Root $script:root } | Should Throw 'RESEARCH_EARLY_EXIT'
            ($first -ne $script:lastStderr) | Should Be $true
            $saved=Get-Content $statePath -Raw | ConvertFrom-Json
            $saved.Failures | Should Be 2
            (([datetime]$saved.NextAttemptUtc).ToUniversalTime()-[datetime]::UtcNow).TotalSeconds | Should BeGreaterThan 110
        }
}

Describe 'Research mutex across independent runspaces' {
    It 'admits one owner and releases for the next invocation without launching any service' {
        $root=Join-Path $TestDrive 'research-lock'
        $lock=Enter-ResearchLock -Root $root
        $lock.Acquired | Should Be $true
        $other=[powershell]::Create()
        try {
            $null=$other.AddScript({
                param($Path,$Root)
                $module=Import-Module $Path -PassThru
                & $module {
                    param($Root)
                    $handle=Enter-ResearchLock -Root $Root
                    try { $handle.Acquired } finally { Exit-ResearchLock $handle }
                } $Root
            }).AddArgument($modulePath).AddArgument($root)
            $result=$other.Invoke()
            $other.HadErrors | Should Be $false
            $result[0] | Should Be $false
        }
        finally {
            $other.Dispose()
            Exit-ResearchLock $lock
        }
        $next=Enter-ResearchLock -Root $root
        try { $next.Acquired | Should Be $true } finally { Exit-ResearchLock $next }
    }
}

