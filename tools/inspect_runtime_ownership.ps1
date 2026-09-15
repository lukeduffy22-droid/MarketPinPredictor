[CmdletBinding()]
param(
    [string]$ProjectRoot,
    [switch]$NoMain
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}

function Test-MarketAppRuntimeCommandContract {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('backend', 'dashboard')]
        [string]$Component,

        [Parameter(Mandatory = $true)]
        [psobject]$ProcessRecord,

        [Parameter(Mandatory = $true)]
        [string]$ProjectRoot
    )

    $root = [IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\')
    $commandLine = [string]$ProcessRecord.CommandLine
    $executablePath = [string]$ProcessRecord.ExecutablePath
    $commandReadable = -not [string]::IsNullOrWhiteSpace($commandLine)
    $rootReferenced = [bool](
        $commandReadable -and
        $commandLine.IndexOf($root, [StringComparison]::OrdinalIgnoreCase) -ge 0
    )

    $failureReasons = [Collections.Generic.List[string]]::new()
    if (-not $commandReadable) { $failureReasons.Add('command_line_unreadable') }
    if (-not $rootReferenced) { $failureReasons.Add('canonical_project_root_not_referenced') }

    $canonicalEntrypoint = $false
    $canonicalRuntime = $false
    $runtimeMarkerPresent = $false
    $fileWatcherDisabled = $null

    if ($Component -eq 'dashboard') {
        $entrypoint = [IO.Path]::GetFullPath((Join-Path $root 'app.py'))
        $streamlitExe = [IO.Path]::GetFullPath(
            (Join-Path $root '.venv\Scripts\streamlit.exe')
        )
        $canonicalEntrypoint = [bool](
            $commandReadable -and
            $commandLine.IndexOf($entrypoint, [StringComparison]::OrdinalIgnoreCase) -ge 0
        )
        $canonicalRuntime = [bool](
            $executablePath.Equals($streamlitExe, [StringComparison]::OrdinalIgnoreCase) -or
            ($commandReadable -and
                $commandLine.IndexOf($streamlitExe, [StringComparison]::OrdinalIgnoreCase) -ge 0)
        )
        $runtimeMarkerPresent = [bool](
            $commandReadable -and
            $commandLine -match '(?i)(?:^|\s)run(?:\s|$)'
        )
        $fileWatcherDisabled = [bool](
            $commandReadable -and
            $commandLine -match '(?i)--server\.fileWatcherType(?:=|\s+)["'']?none(?:["'']?(?:\s|$))'
        )
        if (-not $canonicalEntrypoint) { $failureReasons.Add('canonical_app_entrypoint_not_proven') }
        if (-not $canonicalRuntime) { $failureReasons.Add('canonical_streamlit_executable_not_proven') }
        if (-not $runtimeMarkerPresent) { $failureReasons.Add('streamlit_run_marker_missing') }
        if (-not $fileWatcherDisabled) { $failureReasons.Add('streamlit_file_watcher_none_not_proven') }
    }
    else {
        $entrypoint = [IO.Path]::GetFullPath((Join-Path $root 'server.py'))
        $pythonExe = [IO.Path]::GetFullPath(
            (Join-Path $root '.venv\Scripts\python.exe')
        )
        $canonicalEntrypoint = [bool](
            $commandReadable -and
            $commandLine.IndexOf($entrypoint, [StringComparison]::OrdinalIgnoreCase) -ge 0
        )
        $canonicalRuntime = [bool](
            $executablePath.Equals($pythonExe, [StringComparison]::OrdinalIgnoreCase) -or
            ($commandReadable -and
                $commandLine.IndexOf($pythonExe, [StringComparison]::OrdinalIgnoreCase) -ge 0)
        )
        $runtimeMarkerPresent = [bool](
            $commandReadable -and
            $commandLine -match '(?i)(?:^|[\\/\s])server\.py(?:["'']?(?:\s|$))'
        )
        if (-not $canonicalEntrypoint) { $failureReasons.Add('canonical_server_entrypoint_not_proven') }
        if (-not $canonicalRuntime) { $failureReasons.Add('canonical_python_executable_not_proven') }
        if (-not $runtimeMarkerPresent) { $failureReasons.Add('backend_server_marker_missing') }
    }

    return [pscustomobject]@{
        Component = $Component
        ProcessId = [int]$ProcessRecord.ProcessId
        CommandLineReadable = $commandReadable
        CanonicalProjectRootReferenced = $rootReferenced
        CanonicalEntrypointArgument = $canonicalEntrypoint
        CanonicalVirtualEnvironmentExecutable = $canonicalRuntime
        RuntimeMarkerPresent = $runtimeMarkerPresent
        FileWatcherTypeNone = $fileWatcherDisabled
        CommandContractVerified = [bool](
            $commandReadable -and
            $rootReferenced -and
            $canonicalEntrypoint -and
            $canonicalRuntime -and
            $runtimeMarkerPresent -and
            ($Component -ne 'dashboard' -or $fileWatcherDisabled)
        )
        FailureReasons = @($failureReasons)
    }
}

function Get-MarketAppRuntimeOwnershipReport {
    [CmdletBinding()]
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $root = [IO.Path]::GetFullPath($ProjectRoot)
    $supervisor = Join-Path $root 'market_app_supervisor.psm1'
    Import-Module -Name $supervisor -Force -ErrorAction Stop

    $definitions = @(
        [pscustomobject]@{
            Component = 'backend'
            Port = 8000
            Url = 'http://127.0.0.1:8000/health'
            EndpointContract = 'BackendHealth'
        },
        [pscustomobject]@{
            Component = 'dashboard'
            Port = 8501
            Url = 'http://127.0.0.1:8501/_stcore/health'
            EndpointContract = 'StreamlitHealth'
        }
    )

    $components = foreach ($definition in $definitions) {
        $listenerIds = @(Get-MarketAppListenerProcessIds -Port $definition.Port)
        $record = if ($listenerIds.Count -eq 1) {
            Get-CimInstance Win32_Process `
                -Filter "ProcessId = $([int]$listenerIds[0])" `
                -ErrorAction SilentlyContinue
        }
        else { $null }
        $commandContract = if ($null -ne $record) {
            Test-MarketAppRuntimeCommandContract `
                -Component $definition.Component `
                -ProcessRecord $record `
                -ProjectRoot $root
        }
        else { $null }
        $endpointVerified = Test-MarketAppHttpEndpointContract `
            -Url $definition.Url `
            -Port $definition.Port `
            -ExpectedEndpointContract $definition.EndpointContract `
            -TimeoutSeconds 3

        [pscustomobject]@{
            Component = $definition.Component
            Port = $definition.Port
            ListenerCount = $listenerIds.Count
            ListenerProcessId = if ($listenerIds.Count -eq 1) { [int]$listenerIds[0] } else { $null }
            EndpointContractVerified = [bool]$endpointVerified
            CommandContract = $commandContract
            OwnershipAndEndpointProven = [bool](
                $listenerIds.Count -eq 1 -and
                $null -ne $commandContract -and
                $commandContract.CommandContractVerified -and
                $endpointVerified
            )
        }
    }

    return [pscustomobject]@{
        SchemaVersion = 'marketpin-runtime-ownership-inspection.v1'
        ObservedAtUtc = [datetime]::UtcNow.ToString('o')
        ProjectRoot = $root
        Scope = 'process_and_endpoint_only_not_market_readiness'
        RawCommandLinesIncluded = $false
        Components = @($components)
    }
}

if (-not $NoMain) {
    Get-MarketAppRuntimeOwnershipReport -ProjectRoot $ProjectRoot |
        ConvertTo-Json -Depth 8
}
