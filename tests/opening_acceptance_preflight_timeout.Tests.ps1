$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ModulePath = Join-Path $ProjectRoot 'market_app_supervisor.psm1'
$EnsurePath = Join-Path $ProjectRoot 'ensure_market_app.ps1'
$StartMarketDayPath = Join-Path $ProjectRoot 'start_market_day.ps1'
$PythonPath = Join-Path $ProjectRoot '.venv\Scripts\python.exe'

Import-Module -Name $ModulePath -Force

$ensureTokens = $null
$ensureParseErrors = $null
$ensureAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $EnsurePath,
    [ref]$ensureTokens,
    [ref]$ensureParseErrors
)
if (@($ensureParseErrors).Count -ne 0) {
    throw "ensure_market_app.ps1 did not parse cleanly: $($ensureParseErrors -join '; ')"
}
foreach ($functionName in @(
    'Invoke-OpeningAcceptancePreflight',
    'Assert-OpeningAcceptanceMutationPreflight'
)) {
    $definition = @($ensureAst.FindAll(
        {
            param($candidate)
            $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $candidate.Name -eq $functionName
        },
        $true
    ))
    if ($definition.Count -ne 1) {
        throw "Expected exactly one $functionName definition."
    }
    Invoke-Expression $definition[0].Extent.Text
}

function Write-WatchdogLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [string]$Event = 'status'
    )
    $script:CapturedWatchdogEvents += [pscustomobject]@{
        Event = $Event
        Message = $Message
    }
}

function New-TestPreflightScript {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Body
    )
    $path = Join-Path $TestDrive $Name
    [System.IO.File]::WriteAllText(
        $path,
        $Body,
        [System.Text.UTF8Encoding]::new($false)
    )
    return $path
}

function New-EncodedPowerShellArguments {
    param([Parameter(Mandatory = $true)][string]$Body)
    $encoded = [Convert]::ToBase64String(
        [Text.Encoding]::Unicode.GetBytes($Body)
    )
    return @('-NoProfile', '-NonInteractive', '-EncodedCommand', $encoded)
}

function New-TestPreflightPayload {
    param(
        [string]$Phase = 'strict',
        [bool]$Ready = $true,
        [object[]]$Issues = @(),
        [string]$DatabaseContract = 'strict',
        [bool]$SchemaBootstrapRequired = $false,
        [AllowNull()][object]$SchemaBootstrapScope = $null,
        [bool]$SchemaBootstrapPerformed = $false,
        [int]$BootstrapAttemptCount = 0,
        [object[]]$DatabaseIssues = @()
    )
    $payload = [ordered]@{
        schema_version = 'marketpin-opening-acceptance-preflight.v2'
        phase = $Phase
        checked_at_utc = '2026-09-09T08:00:00+00:00'
        ready = $Ready
        project_root = [System.IO.Path]::GetFullPath($TestDrive)
        database_path = [System.IO.Path]::GetFullPath((Join-Path $TestDrive 'market.db'))
        database_contract = $DatabaseContract
        schema_bootstrap_required = $SchemaBootstrapRequired
        schema_bootstrap_scope = $SchemaBootstrapScope
        schema_bootstrap_performed = $SchemaBootstrapPerformed
        bootstrap_attempt_count = $BootstrapAttemptCount
        source_fingerprint_sha256 = ('a' * 64)
        required_file_count = 67
        required_module_count = 50
        database_issues = @($DatabaseIssues)
        issues = @($Issues)
    }
    if ($Phase -ceq 'bootstrap') {
        $payload['prebootstrap_database_issues'] = @(
            [pscustomobject]@{code='RUNTIME_TABLE_MISSING';table='orb_reference_sample_decisions'}
        )
    }
    return [pscustomobject]$payload
}

function New-TestJsonPrinterBody {
    param([Parameter(Mandatory = $true)][psobject]$Payload)
    $json = $Payload | ConvertTo-Json -Depth 8 -Compress
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
    return "import base64`nprint(base64.b64decode('$encoded').decode('utf-8'))"
}

Describe 'Bounded opening-acceptance preflight' {
    BeforeEach {
        $script:CapturedWatchdogEvents = @()
        $script:ProjectRoot = $TestDrive
        $script:InvocationId = [guid]::NewGuid().ToString('N')
        $script:OpeningAcceptancePreflightTimeoutSeconds = 150
        $script:OpeningAcceptancePreflightSchemaVersion = 'marketpin-opening-acceptance-preflight.v2'
        $script:OpeningAcceptancePreflightRequiredFileCount = 67
        $script:OpeningAcceptancePreflightRequiredModuleCount = 50
        $script:OpeningAcceptancePreflightResult = $null
        $script:PythonExe = $PythonPath
        $script:OpeningAcceptancePreflightScript = Join-Path $TestDrive 'preflight.py'
    }

    It 'returns a successful JSON contract from the bounded child' {
        $payload = New-TestPreflightPayload
        $tool = New-TestPreflightScript `
            -Name 'success.py' `
            -Body (New-TestJsonPrinterBody -Payload $payload)

        $result = Invoke-OpeningAcceptancePreflight `
            -PythonPath $PythonPath `
            -PreflightScript $tool `
            -DatabasePath (Join-Path $TestDrive 'market.db') `
            -TimeoutSeconds 5

        $result.ready | Should Be $true
        $result.schema_version | Should Be 'marketpin-opening-acceptance-preflight.v2'
        @($script:CapturedWatchdogEvents | Where-Object {
            $_.Event -eq 'opening_acceptance_preflight_passed'
        }).Count | Should Be 1
    }

    It 'extracts exactly one UTC timestamp from raw child JSON' {
        $preflightDefinition = @($ensureAst.FindAll(
            {
                param($candidate)
                $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                    $candidate.Name -eq 'Invoke-OpeningAcceptancePreflight'
            },
            $true
        ))[0].Extent.Text

        $preflightDefinition | Should Match '\[regex\]::Matches\('
        $preflightDefinition | Should Match '\$payloadTimestampFieldMatches\.Count -eq 1'
        $preflightDefinition | Should Match '\$payloadTimestampMatches\.Count -eq 1'
        $preflightDefinition | Should Match '\[Globalization\.DateTimeStyles\]::RoundtripKind'
        $preflightDefinition | Should Match '\$payloadTimestampText -cmatch'
        $preflightDefinition | Should Not Match '\[string\]\$payload\.checked_at_utc'
    }

    It 'fails closed on a nonzero child exit and records the exit evidence' {
        $payload = New-TestPreflightPayload `
            -Ready $false `
            -Issues @([pscustomobject]@{code='TEST_FAILURE'})
        $tool = New-TestPreflightScript `
            -Name 'nonzero.py' `
            -Body ((New-TestJsonPrinterBody -Payload $payload) + "`nraise SystemExit(7)")

        {
            Invoke-OpeningAcceptancePreflight `
                -PythonPath $PythonPath `
                -PreflightScript $tool `
                -DatabasePath (Join-Path $TestDrive 'market.db') `
                -TimeoutSeconds 5
        } | Should Throw

        $failure = @($script:CapturedWatchdogEvents | Where-Object {
            $_.Event -eq 'opening_acceptance_preflight_failed'
        })
        $failure.Count | Should Be 1
        $failure[0].Message | Should Match 'exit_code=7'
        $failure[0].Message | Should Match 'issues=TEST_FAILURE'
        $failure[0].Message | Should Match 'action=abort_before_process_change'
    }

    It 'times out only its exact child and preserves an existing listener process' {
        $signalPath = Join-Path $TestDrive 'existing-listener.signal'
        $escapedSignal = $signalPath.Replace("'", "''")
        $listenerBody = @"
`$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
`$listener.Start()
[IO.File]::WriteAllText('$escapedSignal', "`$PID,`$(`$listener.LocalEndpoint.Port)")
Start-Sleep -Seconds 30
"@
        $pwsh = (Get-Command pwsh.exe -ErrorAction Stop).Source
        $existingListener = Start-Process `
            -FilePath $pwsh `
            -ArgumentList (New-EncodedPowerShellArguments -Body $listenerBody) `
            -WindowStyle Hidden `
            -PassThru
        try {
            $signalDeadline = [DateTime]::UtcNow.AddSeconds(5)
            while (-not (Test-Path -LiteralPath $signalPath) -and
                [DateTime]::UtcNow -lt $signalDeadline) {
                Start-Sleep -Milliseconds 50
            }
            Test-Path -LiteralPath $signalPath | Should Be $true
            $listenerIdentity = (Get-Content -LiteralPath $signalPath -Raw).Trim() -split ','
            [int]$listenerIdentity[0] | Should Be $existingListener.Id
            $listenerPort = [int]$listenerIdentity[1]

            $tool = New-TestPreflightScript -Name 'timeout.py' -Body @'
import time
time.sleep(30)
'@
            {
                Invoke-OpeningAcceptancePreflight `
                    -PythonPath $PythonPath `
                    -PreflightScript $tool `
                    -DatabasePath (Join-Path $TestDrive 'market.db') `
                    -TimeoutSeconds 1
            } | Should Throw

            $timeout = @($script:CapturedWatchdogEvents | Where-Object {
                $_.Event -eq 'opening_acceptance_preflight_timed_out'
            })
            $timeout.Count | Should Be 1
            $timeout[0].Message | Should Match 'timeout_seconds=1'
            $timeout[0].Message | Should Match 'termination_confirmed=True'
            $timeout[0].Message | Should Match 'action=abort_before_process_change'
            $childPidMatch = [regex]::Match($timeout[0].Message, 'child_pid=(\d+)')
            $childPidMatch.Success | Should Be $true
            $timedOutChildPid = [int]$childPidMatch.Groups[1].Value
            Get-Process -Id $timedOutChildPid -ErrorAction SilentlyContinue |
                Should BeNullOrEmpty

            $existingListener.Refresh()
            $existingListener.HasExited | Should Be $false
            $listenerProbe = [Net.Sockets.TcpClient]::new()
            try {
                $connected = $listenerProbe.ConnectAsync('127.0.0.1', $listenerPort)
                $connected.Wait(1000) | Should Be $true
                $listenerProbe.Connected | Should Be $true
            }
            finally {
                $listenerProbe.Dispose()
            }
        }
        finally {
            $existingListener.Refresh()
            if (-not $existingListener.HasExited) {
                $existingListener.Kill()
                [void]$existingListener.WaitForExit(5000)
            }
        }
    }

    It 'memoizes one full proof and revalidates its fingerprint before reuse' {
        $script:successfulProof = [pscustomobject]@{
            schema_version = 'marketpin-opening-acceptance-preflight.v2'
            ready = $true
            schema_bootstrap_required = $false
            source_fingerprint_sha256 = ('a' * 64)
        }
        Mock Invoke-OpeningAcceptancePreflight { $script:successfulProof }

        $first = Assert-OpeningAcceptanceMutationPreflight
        $second = Assert-OpeningAcceptanceMutationPreflight

        [object]::ReferenceEquals($first, $script:successfulProof) | Should Be $true
        [object]::ReferenceEquals($second, $script:successfulProof) | Should Be $true
        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 2 -Scope It
        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 1 -Scope It -ParameterFilter {
            $Phase -eq 'fingerprint' -and $ExpectedSourceFingerprint -eq ('a' * 64)
        }
    }

    It 'does not cache a failed preflight for the next mutation attempt' {
        $script:preflightAttemptCount = 0
        Mock Invoke-OpeningAcceptancePreflight {
            $script:preflightAttemptCount += 1
            if ($script:preflightAttemptCount -eq 1) {
                throw 'synthetic preflight failure'
            }
            [pscustomobject]@{
                schema_version = 'marketpin-opening-acceptance-preflight.v2'
                ready = $true
                schema_bootstrap_required = $false
                source_fingerprint_sha256 = ('a' * 64)
            }
        }

        { Assert-OpeningAcceptanceMutationPreflight } | Should Throw 'synthetic preflight failure'
        $result = Assert-OpeningAcceptanceMutationPreflight

        $result.ready | Should Be $true
        $script:preflightAttemptCount | Should Be 2
        Assert-MockCalled Invoke-OpeningAcceptancePreflight -Times 2 -Scope It
    }

    It 'keeps timeout and failure fail-closed while leaving healthy checks lazy' {
        $ensureSource = Get-Content -LiteralPath $EnsurePath -Raw
        $startSource = Get-Content -LiteralPath $StartMarketDayPath -Raw

        $ensureSource | Should Match '\$OpeningAcceptancePreflightTimeoutSeconds = 150'
        $ensureSource | Should Match '111\.716 and 113\.151 seconds'
        $ensureSource | Should Match 'former 120-second cutoff'
        $ensureSource | Should Match 'opening_acceptance_preflight_timed_out'
        $ensureSource | Should Match 'action=abort_before_process_change'
        $ensureSource | Should Match 'Assign only after Invoke-OpeningAcceptancePreflight returns a successful'
        $ensureSource | Should Not Match '\$openingAcceptancePreflight\s*=\s*Invoke-OpeningAcceptancePreflight'

        $checkOnlyExit = $startSource.IndexOf('if ($CheckOnly) { exit 0 }')
        $ensureLaunch = $startSource.IndexOf('& powershell.exe', $checkOnlyExit)
        ($checkOnlyExit -ge 0) | Should Be $true
        ($ensureLaunch -gt $checkOnlyExit) | Should Be $true
    }
}
