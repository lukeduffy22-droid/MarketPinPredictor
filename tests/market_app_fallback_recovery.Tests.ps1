$ProjectRoot = Split-Path -Parent $PSScriptRoot
Import-Module (Join-Path $ProjectRoot 'market_app_supervisor.psm1') -Force
$tokens = $null
$parseErrors = $null
$ensureAst = [System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $ProjectRoot 'ensure_market_app.ps1'), [ref]$tokens, [ref]$parseErrors
)
if (@($parseErrors).Count) { throw 'ensure_market_app.ps1 did not parse cleanly.' }

# Execute only the real recovery decision and following launch branch, with
# process/provider/preflight boundaries mocked. Never dot-source a launcher.
$proof = $ensureAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Test-MarketAppCurrentDayUniversePreparationProof'
}, $true)[0]
Invoke-Expression $proof.Extent.Text
$recoveryBranch = $ensureAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
        $node.Clauses[0].Item1.Extent.Text -eq '$observedTradingDate -and $observedTradingDate -ne $expectedTradingDate'
}, $true)[0]
$launchBranch = $ensureAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
        $node.Clauses[0].Item1.Extent.Text -eq '$backendListenerPresent'
}, $true)[0]
if (-not $recoveryBranch -or -not $launchBranch) { throw 'Missing backend recovery/launch branch.' }
$recoveryScript = [scriptblock]::Create($recoveryBranch.Extent.Text)
$launchScript = [scriptblock]::Create($launchBranch.Extent.Text)

function New-TestMarketPrimaryReadiness {
    param(
        [Parameter(Mandatory = $true)][string]$Symbol,
        [Parameter(Mandatory = $true)][string]$TradingDate
    )

    $isVix = $Symbol -ceq 'VIX'
    $pairCount = if ($Symbol -in @('SPX', 'NDX')) { 100 } else { 10 }
    $primaryExpiration = if ($isVix) {
        ([datetime]::ParseExact(
            $TradingDate,
            'yyyy-MM-dd',
            [System.Globalization.CultureInfo]::InvariantCulture
        )).AddDays(7).ToString('yyyy-MM-dd')
    }
    else {
        $TradingDate
    }
    return [pscustomobject]@{
        subscription_available = $true
        admission_passes = $true
        primary_plan_count = 1
        primary_expiration = $primaryExpiration
        primary_contract_count = 2 * $pairCount
        selected_strike_pairs = $pairCount
        minimum_pair_count = if ($Symbol -in @('SPX', 'NDX')) { 100 } else { 1 }
        complete_pair_count = $pairCount
        orb_reference_minimum_pair_count = 5
        primary_expiration_authority = if ($isVix) {
            'vix_forward_expiration_context_only'
        }
        else {
            'primary_expiration'
        }
        primary_expiration_context_only = [bool]$isVix
        primary_expiration_same_day_authority = [bool](-not $isVix)
        primary_expiration_selection_basis = if ($isVix) {
            'vix_last_trading_day_precedes_settlement_date'
        }
        else {
            'earliest_live_eligible_expiration'
        }
    }
}

function New-TestCurrentDayUniversePreparation {
    param(
        [Parameter(Mandatory = $true)][string[]]$Symbols,
        [string]$TradingDate = '2026-09-09',
        [int]$SelectedContractCount = 1000
    )

    $readiness = [ordered]@{}
    foreach ($symbol in $Symbols) {
        $readiness[$symbol] = New-TestMarketPrimaryReadiness `
            -Symbol $symbol `
            -TradingDate $TradingDate
    }
    return [pscustomobject]@{
        StartupMayContinue = $true
        UsesFallback = $false
        ProvenanceLabel = 'CURRENT_DAY_CACHE'
        Outcome = 'cache_only_ready'
        Result = [pscustomobject]@{
            current_day_cache_ready = $true
            selected_contract_count = $SelectedContractCount
            selected_universe_sha256 = ('a' * 64)
            requested_symbols = @($Symbols)
            market_primary_readiness = [pscustomobject]$readiness
            provenance = [pscustomobject]@{
                mode = 'current_day_cache'
                is_fallback = $false
                trading_date = $TradingDate
                source_date = $TradingDate
            }
        }
    }
}

function Assert-OpeningAcceptanceMutationPreflight { param([switch]$AllowSchemaBootstrap) }
function Write-WatchdogLog { param($Event, $Message) }
function Start-VerifiedComponent {
    param($Component, $Port, $Url, $ExpectedEndpointContract, $FilePath, $ArgumentList,
          $StandardOutputPath, $StandardErrorPath)
}

Describe 'Same-day fallback backend adopts proven morning cache before the opening cutoff' {
    BeforeEach {
        $script:now = [datetime]'2026-09-09T07:45:00'
        $script:expectedTradingDate = '2026-09-09'
        $script:observedTradingDate = '2026-09-09'
        $script:backendUniverseState = [pscustomobject]@{
            TradingDate = '2026-09-09'; SourceDate = '2026-09-08'
            IsFallback = $true; ProvenanceMode = 'prior_cache_filtered'
        }
        $script:backendRecoveryAlreadySucceeded = $true
        $script:backendListenerPresent = $true
        $script:backendWasStopped = $false
        $script:preparedBackendUniverse = $null
        $script:backendRecoveryCompletionPending = $false
        $script:evaluateBackendReadiness = $false
        $script:PythonExe = 'python.exe'
        $script:InvocationId = 'same-day-fallback-test'
        $script:Caller = 'test'
        $env:DATABENTO_SYMBOLS = 'SPX,NDX'
        $env:DATABENTO_MAX_SUBSCRIPTION_CONTRACTS = '3600'
        $script:prepared = New-TestCurrentDayUniversePreparation `
            -Symbols @('SPX', 'NDX') `
            -TradingDate '2026-09-09' `
            -SelectedContractCount 3200
        Mock Get-Date { $script:now }
        Mock Assert-OpeningAcceptanceMutationPreflight {}
        Mock Write-WatchdogLog {}
        Mock Get-MarketAppListenerProcessIds { @(44224) }
        Mock Invoke-MarketAppUniverseCachePreparation { $script:prepared }
        Mock Invoke-MarketAppBoundedAutomaticListenerStop { [pscustomobject]@{ Stopped = $true } }
        Mock New-MarketAppLaunchLogPaths { [pscustomobject]@{} }
        Mock Start-VerifiedComponent { [pscustomobject]@{ ListenerProcessId = 42424 } }
    }

    foreach ($clock in @('07:45:00', '08:15:00')) {
        It "adopts today's prestaged cache at $clock despite a same-day backend and recovery latch" {
            $now = [datetime]("2026-09-09T$clock")
            . $recoveryScript
            $backendWasStopped | Should Be $true
            $backendListenerPresent | Should Be $false
            [object]::ReferenceEquals($preparedBackendUniverse, $script:prepared) | Should Be $true
            . $launchScript

            Assert-MockCalled Invoke-MarketAppUniverseCachePreparation -Times 1 -Exactly -Scope It
            Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 1 -Exactly -Scope It -ParameterFilter {
                $ExpectedPid -eq 44224 -and $Port -eq 8000 -and
                $RecoveryReason -eq 'universe_fallback_recovered'
            }
            Assert-MockCalled Start-VerifiedComponent -Times 1 -Exactly -Scope It
        }
    }

    foreach ($failure in @('fallback', 'wrong_source_day', 'wrong_trading_day', 'bad_hash', 'over_cap', 'failed_preparation')) {
        It "preserves the exact listener when current-day preparation proof fails: $failure" {
            switch ($failure) {
                'fallback' { $script:prepared.UsesFallback = $true }
                'wrong_source_day' { $script:prepared.Result.provenance.source_date = '2026-09-08' }
                'wrong_trading_day' { $script:prepared.Result.provenance.trading_date = '2026-09-08' }
                'bad_hash' { $script:prepared.Result.selected_universe_sha256 = 'bad' }
                'over_cap' { $script:prepared.Result.selected_contract_count = 3601 }
                'failed_preparation' { $script:prepared.StartupMayContinue = $false }
            }
            . $recoveryScript
            $backendWasStopped | Should Be $false
            $backendListenerPresent | Should Be $true
            Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Exactly -Scope It
            Assert-MockCalled Start-VerifiedComponent -Times 0 -Exactly -Scope It
        }
    }

    It 'preserves the listener when the final owner/cutoff check refuses a stop' {
        Mock Invoke-MarketAppBoundedAutomaticListenerStop { [pscustomobject]@{ Stopped = $false } }
        . $recoveryScript
        $backendListenerPresent | Should Be $true
        $preparedBackendUniverse | Should BeNullOrEmpty
        Assert-MockCalled Start-VerifiedComponent -Times 0 -Exactly -Scope It
    }

    It 'does not stage or restart fallback at 08:25 and defers to current-session checks' {
        $now = [datetime]'2026-09-09T08:25:00'
        . $recoveryScript
        $evaluateBackendReadiness | Should Be $true
        $backendListenerPresent | Should Be $true
        Assert-MockCalled Invoke-MarketAppUniverseCachePreparation -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-MarketAppBoundedAutomaticListenerStop -Times 0 -Exactly -Scope It
    }
}

Describe 'Current-day preparation proof requires every configured primary family' {
    It 'accepts complete SPX NDX VIX RUT primary evidence within the launch cap' {
        $prepared = New-TestCurrentDayUniversePreparation `
            -Symbols @('SPX', 'NDX', 'VIX', 'RUT') `
            -TradingDate '2026-09-10' `
            -SelectedContractCount 2064

        Test-MarketAppCurrentDayUniversePreparationProof `
            -Preparation $prepared `
            -ExpectedTradingDate '2026-09-10' `
            -ExpectedContractCap 3200 `
            -ExpectedSymbols @('SPX', 'NDX', 'VIX', 'RUT') | Should Be $true
    }

    foreach ($missingFamily in @('VIX', 'RUT')) {
        It "rejects current-day readiness when $missingFamily primary evidence is missing" {
            $prepared = New-TestCurrentDayUniversePreparation `
                -Symbols @('SPX', 'NDX', 'VIX', 'RUT') `
                -TradingDate '2026-09-10' `
                -SelectedContractCount 2064
            $prepared.Result.market_primary_readiness.PSObject.Properties.Remove(
                $missingFamily
            )

            Test-MarketAppCurrentDayUniversePreparationProof `
                -Preparation $prepared `
                -ExpectedTradingDate '2026-09-10' `
                -ExpectedContractCap 3200 `
                -ExpectedSymbols @('SPX', 'NDX', 'VIX', 'RUT') | Should Be $false
        }
    }

    foreach ($sparseFamily in @('VIX', 'RUT')) {
        It "rejects current-day ORB readiness when $sparseFamily complete-pair evidence is missing" {
            $prepared = New-TestCurrentDayUniversePreparation `
                -Symbols @('SPX', 'NDX', 'VIX', 'RUT') `
                -TradingDate '2026-09-10' `
                -SelectedContractCount 2064
            $prepared.Result.market_primary_readiness.$sparseFamily.PSObject.Properties.Remove(
                'complete_pair_count'
            )

            Test-MarketAppCurrentDayUniversePreparationProof `
                -Preparation $prepared `
                -ExpectedTradingDate '2026-09-10' `
                -ExpectedContractCap 3200 `
                -ExpectedSymbols @('SPX', 'NDX', 'VIX', 'RUT') | Should Be $false
        }

        It "rejects current-day ORB readiness when $sparseFamily has only one complete pair" {
            $prepared = New-TestCurrentDayUniversePreparation `
                -Symbols @('SPX', 'NDX', 'VIX', 'RUT') `
                -TradingDate '2026-09-10' `
                -SelectedContractCount 2064
            $prepared.Result.market_primary_readiness.$sparseFamily.complete_pair_count = 1

            Test-MarketAppCurrentDayUniversePreparationProof `
                -Preparation $prepared `
                -ExpectedTradingDate '2026-09-10' `
                -ExpectedContractCap 3200 `
                -ExpectedSymbols @('SPX', 'NDX', 'VIX', 'RUT') | Should Be $false
        }
    }

    It 'accepts the exact five-complete-pair ORB threshold for VIX and RUT' {
        $prepared = New-TestCurrentDayUniversePreparation `
            -Symbols @('SPX', 'NDX', 'VIX', 'RUT') `
            -TradingDate '2026-09-10' `
            -SelectedContractCount 2064
        $prepared.Result.market_primary_readiness.VIX.complete_pair_count = 5
        $prepared.Result.market_primary_readiness.RUT.complete_pair_count = 5

        Test-MarketAppCurrentDayUniversePreparationProof `
            -Preparation $prepared `
            -ExpectedTradingDate '2026-09-10' `
            -ExpectedContractCap 3200 `
            -ExpectedSymbols @('SPX', 'NDX', 'VIX', 'RUT') | Should Be $true
    }

    It 'rejects a VIX primary mislabeled as same-day spot authority' {
        $prepared = New-TestCurrentDayUniversePreparation `
            -Symbols @('SPX', 'NDX', 'VIX', 'RUT') `
            -TradingDate '2026-09-10' `
            -SelectedContractCount 2064
        $prepared.Result.market_primary_readiness.VIX.primary_expiration = '2026-09-10'
        $prepared.Result.market_primary_readiness.VIX.primary_expiration_authority = 'primary_expiration'
        $prepared.Result.market_primary_readiness.VIX.primary_expiration_context_only = $false
        $prepared.Result.market_primary_readiness.VIX.primary_expiration_same_day_authority = $true

        Test-MarketAppCurrentDayUniversePreparationProof `
            -Preparation $prepared `
            -ExpectedTradingDate '2026-09-10' `
            -ExpectedContractCap 3200 `
            -ExpectedSymbols @('SPX', 'NDX', 'VIX', 'RUT') | Should Be $false
    }
}
