$ProjectRoot = Split-Path -Parent $PSScriptRoot
$LauncherPath = Join-Path $ProjectRoot 'start_databento_app.ps1'
$LauncherSource = Get-Content -LiteralPath $LauncherPath -Raw
$SupervisorModule = Join-Path $ProjectRoot 'market_app_supervisor.psm1'
$LauncherTokens = $null
$LauncherParseErrors = $null
$LauncherAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $LauncherPath,
    [ref]$LauncherTokens,
    [ref]$LauncherParseErrors
)
if (@($LauncherParseErrors).Count -ne 0) {
    throw "start_databento_app.ps1 did not parse cleanly: $($LauncherParseErrors -join '; ')"
}
Import-Module -Name $SupervisorModule -Force

function Get-LauncherFunctionSource {
    param([Parameter(Mandatory = $true)][string]$Name)

    $match = [regex]::Match(
        $LauncherSource,
        "(?ms)^function $([regex]::Escape($Name)) \{.*?(?=^function |\z)"
    )
    if (-not $match.Success) {
        throw "Could not isolate launcher function: $Name"
    }
    return $match.Value
}

foreach ($functionName in @(
    'Invoke-OpeningAcceptancePreflight',
    'Assert-OpeningAcceptanceMutationPreflight',
    'Stop-MarketPinProcesses'
)) {
    $definition = @($LauncherAst.FindAll(
        {
            param($candidate)
            $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $candidate.Name -eq $functionName
        },
        $true
    ))
    if ($definition.Count -ne 1) {
        throw "Expected exactly one $functionName definition in start_databento_app.ps1."
    }
    Invoke-Expression $definition[0].Extent.Text
}

$AppDir = [System.IO.Path]::GetFullPath($ProjectRoot)
$PythonExe = Join-Path $AppDir '.venv\Scripts\python.exe'
$OpeningAcceptancePreflightScript = Join-Path $AppDir 'tools\preflight_opening_acceptance.py'
$OpeningAcceptancePreflightSchemaVersion = 'marketpin-opening-acceptance-preflight.v2'
$OpeningAcceptancePreflightRequiredFileCount = 67
$OpeningAcceptancePreflightRequiredModuleCount = 50
$OpeningAcceptancePreflightTimeoutSeconds = 150
$InvocationId = 'manual-launcher-pester'

function Write-StartupSupervisorLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Event = 'status'
    )
}

function New-ValidStrictPreflightJson {
    param(
        [string]$Phase = 'strict',
        [string]$DatabaseContract = 'strict',
        [bool]$BootstrapRequired = $false,
        [AllowNull()][object]$BootstrapScope = $null,
        [bool]$BootstrapPerformed = $false,
        [int]$BootstrapAttemptCount = 0,
        [object[]]$DatabaseIssues = @()
    )

    return [ordered]@{
        schema_version = $OpeningAcceptancePreflightSchemaVersion
        phase = $Phase
        checked_at_utc = '2026-09-09T08:30:00+00:00'
        ready = $true
        project_root = $AppDir
        database_path = Join-Path $AppDir 'data\market_data.db'
        database_contract = $DatabaseContract
        schema_bootstrap_required = $BootstrapRequired
        schema_bootstrap_scope = $BootstrapScope
        schema_bootstrap_performed = $BootstrapPerformed
        bootstrap_attempt_count = $BootstrapAttemptCount
        source_fingerprint_sha256 = ('a' * 64)
        required_file_count = 67
        required_module_count = 50
        database_issues = $DatabaseIssues
        issues = @()
    } | ConvertTo-Json -Compress -Depth 8
}

Describe 'Manual Databento full-stack launcher mutation guard' {
    It 'is valid Windows PowerShell syntax' {
        @($LauncherParseErrors).Count | Should Be 0
    }

    It 'fails closed when either canonical virtual-environment executable is missing' {
        $LauncherSource | Should Match 'Join-Path \$AppDir ''\.venv\\Scripts\\python\.exe'''
        $LauncherSource | Should Match 'Join-Path \$AppDir ''\.venv\\Scripts\\streamlit\.exe'''
        $LauncherSource | Should Match 'Test-Path -LiteralPath \$PythonExe -PathType Leaf'
        $LauncherSource | Should Match 'Test-Path -LiteralPath \$StreamlitExe -PathType Leaf'
        $LauncherSource | Should Match 'Canonical MarketPin Python was not found'
        $LauncherSource | Should Match 'Canonical MarketPin Streamlit was not found'
        $LauncherSource | Should Match 'marketpin-opening-acceptance-preflight\.v2'
        $LauncherSource | Should Match 'tools\\preflight_opening_acceptance\.py'
        $LauncherSource | Should Not Match '(?m)^\s*\$PythonExe\s*=\s*["'']python["'']'
        $LauncherSource | Should Not Match '(?m)^\s*\$StreamlitExe\s*=\s*["'']streamlit["'']'
        $LauncherSource | Should Match 'Start-Process\s+`\s*\r?\n\s*-FilePath \$PythonExe'
        $LauncherSource | Should Match 'Start-Process -FilePath \$StreamlitExe'
    }

    It 'disables Streamlit source watching in the production launcher' {
        $LauncherSource | Should Match '"--server\.fileWatcherType"\s*,\s*\r?\n\s*"none"'
    }

    It 'uses the bounded opening-acceptance preflight and fails closed on incomplete proof' {
        $LauncherSource | Should Match '\$OpeningAcceptancePreflightTimeoutSeconds = 150'
        $preflight = Get-LauncherFunctionSource -Name 'Invoke-OpeningAcceptancePreflight'
        $preflight | Should Match "ValidateSet\('strict', 'bootstrap-eligibility', 'bootstrap', 'fingerprint'\)"
        $preflight | Should Match 'Invoke-MarketAppBoundedChildProcess'
        $preflight | Should Match '-FilePath \$PythonPath'
        $preflight | Should Match '\$child\.TimedOut'
        $preflight | Should Match '\$null -ne \$child\.ExitCode'
        $preflight | Should Match 'ConvertFrom-Json -ErrorAction Stop'
        $preflight | Should Match '\$rawOutput\.Count -eq 1'
        $preflight | Should Match '\$standardError\.Count -ne 0'
        $preflight | Should Match '\$payload\.schema_version -ceq \$OpeningAcceptancePreflightSchemaVersion'
        $preflight | Should Match '\$payload\.phase -ceq \$Phase'
        $preflight | Should Match '\$payloadFieldsMatch'
        $preflight | Should Match '\[regex\]::Matches\('
        $preflight | Should Match '\$payloadTimestampFieldMatches\.Count -eq 1'
        $preflight | Should Match '\$payloadTimestampMatches\.Count -eq 1'
        $preflight | Should Match '\[Globalization\.DateTimeStyles\]::RoundtripKind'
        $preflight | Should Match '\$payloadTimestampText -cmatch'
        $preflight | Should Not Match '\[string\]\$payload\.checked_at_utc'
        $preflight | Should Match '\$payloadTimestampValid'
        $preflight | Should Match '\$payloadIssuesAreArray'
        $preflight | Should Match '\$payloadDatabaseIssuesAreArray'
        $preflight | Should Match '\$OpeningAcceptancePreflightRequiredFileCount'
        $preflight | Should Match '\$OpeningAcceptancePreflightRequiredModuleCount'
        $preflight | Should Match '\$payloadFingerprint -ceq \$ExpectedSourceFingerprint'
        $preflight | Should Match '\$payloadIssuesEmpty'
        $preflight | Should Match 'action=abort_before_process_change'

        $assertion = Get-LauncherFunctionSource -Name 'Assert-OpeningAcceptanceMutationPreflight'
        $assertion | Should Match '-PythonPath \$PythonExe'
        $assertion | Should Match '-PreflightScript \$OpeningAcceptancePreflightScript'
        $assertion | Should Match '-TimeoutSeconds \$OpeningAcceptancePreflightTimeoutSeconds'
        $assertion | Should Match "-Phase 'fingerprint'"
        $assertion | Should Match "'bootstrap-eligibility'"
        $assertion | Should Match "-Phase 'bootstrap'"
        $assertion | Should Match '-BootstrapScope \$bootstrapScope'
        $invokeIndex = $assertion.IndexOf('$preflight = Invoke-OpeningAcceptancePreflight')
        $cacheIndex = $assertion.IndexOf('$script:OpeningAcceptancePreflightResult = $preflight')
        ($invokeIndex -ge 0) | Should Be $true
        ($cacheIndex -gt $invokeIndex) | Should Be $true
        ([regex]::Matches($assertion, "-Phase 'bootstrap'" )).Count | Should Be 1
    }

    It 'keeps the stop helper locally guarded and re-verifies every selected process' {
        $stop = Get-LauncherFunctionSource -Name 'Stop-MarketPinProcesses'
        $preflightIndex = $stop.IndexOf('Assert-OpeningAcceptanceMutationPreflight | Out-Null')
        $enumerationIndex = $stop.IndexOf('Get-CimInstance Win32_Process')
        $verificationIndex = $stop.IndexOf('Test-MarketAppVerifiedProcess')
        $stopIndex = $stop.IndexOf('Stop-Process -Id $proc.ProcessId')
        $quiescenceIndex = $stop.IndexOf('Wait-MarketAppProcessNetworkQuiescence')

        ($preflightIndex -ge 0) | Should Be $true
        ($enumerationIndex -gt $preflightIndex) | Should Be $true
        ($verificationIndex -gt $enumerationIndex) | Should Be $true
        ($stopIndex -gt $verificationIndex) | Should Be $true
        ($quiescenceIndex -gt $stopIndex) | Should Be $true
        $stop | Should Match 'Refusing to stop PID.*not a verified MarketPinPredictor process'
        $stop | Should Match 'process_exited=true tcp_connection_count=0'
    }

    It 'completes executable checks, universe preparation, and preflight before any existing-process stop' {
        $mainStart = $LauncherSource.IndexOf("-Event 'invocation_started'")
        $pythonCheck = $LauncherSource.IndexOf(
            'if (-not (Test-Path -LiteralPath $PythonExe -PathType Leaf))',
            $mainStart
        )
        $streamlitCheck = $LauncherSource.IndexOf(
            'if (-not (Test-Path -LiteralPath $StreamlitExe -PathType Leaf))',
            $pythonCheck
        )
        $universePreparation = $LauncherSource.IndexOf(
            '$universePreparation = Invoke-MarketAppUniverseCachePreparation',
            $streamlitCheck
        )
        $preflight = $LauncherSource.IndexOf(
            'Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap | Out-Null',
            $universePreparation
        )
        $stop = $LauncherSource.IndexOf(
            'Stop-MarketPinProcesses -Root $AppDir',
            $preflight
        )
        $backendLaunch = $LauncherSource.IndexOf(
            '$backendProcess = Start-Process',
            $stop
        )

        ($mainStart -ge 0) | Should Be $true
        ($pythonCheck -gt $mainStart) | Should Be $true
        ($streamlitCheck -gt $pythonCheck) | Should Be $true
        ($universePreparation -gt $streamlitCheck) | Should Be $true
        ($preflight -gt $universePreparation) | Should Be $true
        ($stop -gt $preflight) | Should Be $true
        ($backendLaunch -gt $stop) | Should Be $true
    }

    It 'rejects a malformed ready-true child payload behaviorally' {
        $script:OpeningAcceptancePreflightResult = $null
        Mock Invoke-MarketAppBoundedChildProcess {
            [pscustomobject]@{
                TimedOut = $false
                ExitCode = 0
                Output = @('{"ready":true}')
                StandardError = @()
                ProcessId = 41001
                ElapsedMilliseconds = 1
                TerminationConfirmed = $true
            }
        }

        { Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict' } | Should Throw
        Assert-MockCalled Invoke-MarketAppBoundedChildProcess -Times 1 -Scope It
    }

    It 'accepts exactly one contract-complete strict payload behaviorally' {
        $script:OpeningAcceptancePreflightResult = $null
        $validJson = New-ValidStrictPreflightJson
        $decodedTimestamp = ($validJson | ConvertFrom-Json).checked_at_utc
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            $decodedTimestamp -is [datetime] | Should Be $true
        }
        Mock Invoke-MarketAppBoundedChildProcess {
            [pscustomobject]@{
                TimedOut = $false
                ExitCode = 0
                Output = @($validJson)
                StandardError = @()
                ProcessId = 41005
                ElapsedMilliseconds = 1
                TerminationConfirmed = $true
            }
        }

        $result = Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict'

        $result.ready | Should Be $true
        $result.phase | Should Be 'strict'
        Assert-MockCalled Invoke-MarketAppBoundedChildProcess -Times 1 -Scope It
    }

    It 'rejects a missing raw timestamp value even when the JSON field exists' {
        $script:OpeningAcceptancePreflightResult = $null
        $invalidJson = (New-ValidStrictPreflightJson) -replace (
            '"checked_at_utc":"[^"]+"',
            '"checked_at_utc":null'
        )
        Mock Invoke-MarketAppBoundedChildProcess {
            [pscustomobject]@{
                TimedOut = $false
                ExitCode = 0
                Output = @($invalidJson)
                StandardError = @()
                ProcessId = 41006
                ElapsedMilliseconds = 1
                TerminationConfirmed = $true
            }
        }

        { Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict' } | Should Throw
    }

    It 'rejects duplicate raw timestamp fields even when JSON parsing succeeds' {
        $script:OpeningAcceptancePreflightResult = $null
        $validJson = New-ValidStrictPreflightJson
        $duplicateJson = $validJson.Insert(
            1,
            '"checked_at_utc":null,'
        )
        Mock Invoke-MarketAppBoundedChildProcess {
            [pscustomobject]@{
                TimedOut = $false
                ExitCode = 0
                Output = @($duplicateJson)
                StandardError = @()
                ProcessId = 41007
                ElapsedMilliseconds = 1
                TerminationConfirmed = $true
            }
        }

        { Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict' } | Should Throw
    }

    It 'rejects timeout behavior before accepting any proof' {
        $script:OpeningAcceptancePreflightResult = $null
        Mock Invoke-MarketAppBoundedChildProcess {
            [pscustomobject]@{
                TimedOut = $true
                ExitCode = $null
                Output = @()
                StandardError = @()
                ProcessId = 41002
                ElapsedMilliseconds = 120000
                TerminationConfirmed = $true
            }
        }

        { Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict' } | Should Throw
        Assert-MockCalled Invoke-MarketAppBoundedChildProcess -Times 1 -Scope It
    }

    It 'rejects stdout chatter and nonempty stderr behaviorally' {
        $script:OpeningAcceptancePreflightResult = $null
        $validJson = New-ValidStrictPreflightJson
        $script:PreflightChildResult = [pscustomobject]@{
            TimedOut = $false
            ExitCode = 0
            Output = @('diagnostic chatter', $validJson)
            StandardError = @()
            ProcessId = 41003
            ElapsedMilliseconds = 1
            TerminationConfirmed = $true
        }
        Mock Invoke-MarketAppBoundedChildProcess { $script:PreflightChildResult }

        { Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict' } | Should Throw

        $script:PreflightChildResult = [pscustomobject]@{
            TimedOut = $false
            ExitCode = 0
            Output = @($validJson)
            StandardError = @('unexpected warning')
            ProcessId = 41004
            ElapsedMilliseconds = 1
            TerminationConfirmed = $true
        }
        { Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonExe `
            -PreflightScript $OpeningAcceptancePreflightScript `
            -DatabasePath (Join-Path $AppDir 'data\market_data.db') `
            -Phase 'strict' } | Should Throw
        Assert-MockCalled Invoke-MarketAppBoundedChildProcess -Times 2 -Scope It
    }

    It 'keeps a strict-complete database to one full eligibility pass' {
        $script:OpeningAcceptancePreflightResult = $null
        Mock Invoke-OpeningAcceptancePreflight {
            [pscustomobject]@{
                schema_bootstrap_required = $false
                source_fingerprint_sha256 = ('a' * 64)
            }
        }

        $result = Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap

        $result.schema_bootstrap_required | Should Be $false
        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 1 -Scope It `
            -ParameterFilter { $Phase -eq 'bootstrap-eligibility' }
        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 0 -Scope It `
            -ParameterFilter { $Phase -eq 'bootstrap' }
    }

    It 'permits at most one scoped bootstrap attempt' {
        $script:OpeningAcceptancePreflightResult = $null
        Mock Get-MarketAppListenerProcessIds { @() }
        Mock Invoke-OpeningAcceptancePreflight {
            if ($Phase -eq 'bootstrap-eligibility') {
                return [pscustomobject]@{
                    schema_bootstrap_required = $true
                    schema_bootstrap_scope = 'canonical_init_db'
                    database_contract = 'additive_bootstrap_required'
                    database_issues = @([pscustomobject]@{ code = 'RUNTIME_TABLE_MISSING' })
                    source_fingerprint_sha256 = ('a' * 64)
                }
            }
            if ($Phase -eq 'bootstrap') {
                return [pscustomobject]@{
                    schema_bootstrap_required = $false
                    schema_bootstrap_scope = 'canonical_init_db'
                    schema_bootstrap_performed = $true
                    bootstrap_attempt_count = 1
                    source_fingerprint_sha256 = ('a' * 64)
                }
            }
            throw "Unexpected phase: $Phase"
        }

        $result = Assert-OpeningAcceptanceMutationPreflight -AllowSchemaBootstrap

        $result.bootstrap_attempt_count | Should Be 1
        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 1 -Scope It `
            -ParameterFilter { $Phase -eq 'bootstrap' -and $BootstrapScope -eq 'canonical_init_db' }
    }

    It 'never enumerates or stops a process when the initial proof fails' {
        $script:OpeningAcceptancePreflightResult = $null
        Mock Invoke-OpeningAcceptancePreflight { throw 'mock preflight failure' }
        Mock Get-CimInstance { throw 'process enumeration must not occur' }
        Mock Stop-Process { throw 'process stop must not occur' }

        { Stop-MarketPinProcesses -Root $AppDir } | Should Throw

        Assert-MockCalled Get-CimInstance -Times 0 -Scope It
        Assert-MockCalled Stop-Process -Times 0 -Scope It
    }

    It 'revalidates a cached fingerprint and refuses to stop when it changed' {
        $script:OpeningAcceptancePreflightResult = [pscustomobject]@{
            source_fingerprint_sha256 = ('a' * 64)
        }
        Mock Invoke-OpeningAcceptancePreflight { throw 'mock source fingerprint mismatch' } `
            -ParameterFilter { $Phase -eq 'fingerprint' }
        Mock Get-CimInstance { throw 'process enumeration must not occur' }
        Mock Stop-Process { throw 'process stop must not occur' }

        { Stop-MarketPinProcesses -Root $AppDir } | Should Throw

        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 1 -Scope It `
            -ParameterFilter {
                $Phase -eq 'fingerprint' -and
                $ExpectedSourceFingerprint -eq ('a' * 64)
            }
        Assert-MockCalled Get-CimInstance -Times 0 -Scope It
        Assert-MockCalled Stop-Process -Times 0 -Scope It
    }
}
