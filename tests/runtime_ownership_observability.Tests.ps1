$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ToolPath = Join-Path $ProjectRoot 'tools\inspect_runtime_ownership.ps1'
. $ToolPath -NoMain

Describe 'Read-only runtime ownership command contracts' {
    It 'loads through a standalone File invocation when ProjectRoot is omitted' {
        $output = @(
            & powershell.exe -NoProfile -NonInteractive -File $ToolPath -NoMain 2>&1
        )
        $exitCode = $LASTEXITCODE

        $exitCode | Should Be 0
        $output.Count | Should Be 0
    }

    It 'proves the canonical dashboard entrypoint and disabled file watcher' {
        $record = [pscustomobject]@{
            ProcessId = 8501
            ExecutablePath = Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe'
            CommandLine = '"' + (Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe') +
                '" run "' + (Join-Path $ProjectRoot 'app.py') +
                '" --server.port 8501 --server.fileWatcherType none'
        }

        $result = Test-MarketAppRuntimeCommandContract `
            -Component dashboard `
            -ProcessRecord $record `
            -ProjectRoot $ProjectRoot

        $result.CommandContractVerified | Should Be $true
        $result.CanonicalEntrypointArgument | Should Be $true
        $result.FileWatcherTypeNone | Should Be $true
        ('CommandLine' -notin $result.PSObject.Properties.Name) | Should Be $true
    }

    It 'accepts representative production command lines with absolute entrypoints' {
        $python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
        $streamlit = Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe'
        $backendEntrypoint = [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'server.py'))
        $dashboardEntrypoint = [IO.Path]::GetFullPath((Join-Path $ProjectRoot 'app.py'))
        $backendRecord = [pscustomobject]@{
            ProcessId = 8000
            ExecutablePath = $python
            CommandLine = '"' + $python + '" "' + $backendEntrypoint + '"'
        }
        $dashboardRecord = [pscustomobject]@{
            ProcessId = 8501
            ExecutablePath = $streamlit
            CommandLine = '"' + $streamlit + '" run "' + $dashboardEntrypoint +
                '" --server.address 127.0.0.1 --server.port 8501 ' +
                '--server.headless true --server.fileWatcherType none'
        }

        (Test-MarketAppRuntimeCommandContract -Component backend `
            -ProcessRecord $backendRecord -ProjectRoot $ProjectRoot).CommandContractVerified |
            Should Be $true
        (Test-MarketAppRuntimeCommandContract -Component dashboard `
            -ProcessRecord $dashboardRecord -ProjectRoot $ProjectRoot).CommandContractVerified |
            Should Be $true
    }

    It 'does not treat relative app.py plus HTTP-capable Streamlit as canonical proof' {
        $record = [pscustomobject]@{
            ProcessId = 8501
            ExecutablePath = Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe'
            CommandLine = '"' + (Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe') +
                '" run app.py --server.port 8501 --server.fileWatcherType none'
        }

        $result = Test-MarketAppRuntimeCommandContract `
            -Component dashboard `
            -ProcessRecord $record `
            -ProjectRoot $ProjectRoot

        $result.CommandContractVerified | Should Be $false
        ('canonical_app_entrypoint_not_proven' -in $result.FailureReasons) | Should Be $true
    }

    It 'fails closed when the dashboard file-watcher argument is absent' {
        $record = [pscustomobject]@{
            ProcessId = 8501
            ExecutablePath = Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe'
            CommandLine = '"' + (Join-Path $ProjectRoot '.venv\Scripts\streamlit.exe') +
                '" run "' + (Join-Path $ProjectRoot 'app.py') + '" --server.port 8501'
        }

        $result = Test-MarketAppRuntimeCommandContract `
            -Component dashboard `
            -ProcessRecord $record `
            -ProjectRoot $ProjectRoot

        $result.CommandContractVerified | Should Be $false
        ('streamlit_file_watcher_none_not_proven' -in $result.FailureReasons) | Should Be $true
    }

    It 'distinguishes a canonical backend entrypoint from relative server.py' {
        $python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
        $canonical = [pscustomobject]@{
            ProcessId = 8000
            ExecutablePath = $python
            CommandLine = '"' + $python + '" "' + (Join-Path $ProjectRoot 'server.py') + '"'
        }
        $relative = [pscustomobject]@{
            ProcessId = 8001
            ExecutablePath = $python
            CommandLine = '"' + $python + '" server.py'
        }

        (Test-MarketAppRuntimeCommandContract -Component backend `
            -ProcessRecord $canonical -ProjectRoot $ProjectRoot).CommandContractVerified |
            Should Be $true
        (Test-MarketAppRuntimeCommandContract -Component backend `
            -ProcessRecord $relative -ProjectRoot $ProjectRoot).CommandContractVerified |
            Should Be $false
    }
}
