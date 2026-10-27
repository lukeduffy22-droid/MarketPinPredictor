# Invoke only tiny local Python fixtures, never the actual settlement command.
$root=Split-Path $PSScriptRoot -Parent
$python=Join-Path $root '.venv\Scripts\python.exe'
$tokens=$null; $errors=$null
$ast=[System.Management.Automation.Language.Parser]::ParseFile((Join-Path $root 'tools\run_verified_close_postclose.ps1'),[ref]$tokens,[ref]$errors)
if ($errors) { throw ($errors -join '; ') }
$function=$ast.Find({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Invoke-PostclosePython' },$true)
Invoke-Expression $function.Extent.Text

Describe 'Verified-close native process failure receipts' {
    BeforeEach {
        $script:oldPreference=$ErrorActionPreference
        $ErrorActionPreference='Stop'
    }
    AfterEach { $ErrorActionPreference=$script:oldPreference }
    It 'captures a failing interpreter exit and stderr without aborting the log path' {
        $fixture=Join-Path $TestDrive 'failure fixture.py'
        Set-Content -LiteralPath $fixture -Value 'import sys; print("fixture failure", file=sys.stderr); sys.exit(7)'
        $result=Invoke-PostclosePython -PythonPath $python -Arguments @($fixture)
        $result.ExitCode | Should Be 7
        ($result.Output -join "`n") | Should Match 'fixture failure'
        $ErrorActionPreference | Should Be 'Stop'
        $log=Join-Path $TestDrive 'failure.log'
        Add-Content -LiteralPath $log -Value ("exit={0}`n{1}" -f $result.ExitCode,($result.Output -join "`n"))
        Get-Content -LiteralPath $log -Raw | Should Match 'exit=7'
    }
    It 'retains stderr warnings without turning a successful exit into failure' {
        $fixture=Join-Path $TestDrive 'warning fixture.py'
        Set-Content -LiteralPath $fixture -Value 'import sys; print("fixture warning", file=sys.stderr); print("fixture success")'
        $result=Invoke-PostclosePython -PythonPath $python -Arguments @($fixture)
        $result.ExitCode | Should Be 0
        ($result.Output -join "`n") | Should Match 'fixture warning'
        ($result.Output -join "`n") | Should Match 'fixture success'
    }
    It 'preserves successful output and exit status' {
        $fixture=Join-Path $TestDrive 'success fixture.py'
        Set-Content -LiteralPath $fixture -Value 'print("fixture success")'
        $result=Invoke-PostclosePython -PythonPath $python -Arguments @($fixture)
        $result.ExitCode | Should Be 0
        ($result.Output -join "`n") | Should Match 'fixture success'
    }
}

Describe 'Verified-close scheduled task reliability contract' {
    BeforeAll {
        $script:installerText = Get-Content -LiteralPath (Join-Path $root 'tools\install_verified_close_postclose_tasks.ps1') -Raw
    }

    It 'keeps selection retries bounded and wakeable without overlapping runs' {
        $script:installerText | Should Match '(?s)\$selectionSettings\s*=\s*New-ScheduledTaskSettingsSet.+?-WakeToRun.+?-MultipleInstances\s+IgnoreNew.+?-RestartCount\s+3.+?-RestartInterval\s+\(New-TimeSpan\s+-Minutes\s+5\)'
        $script:installerText | Should Match '(?s)\$selectionDefinition\s*=\s*New-ScheduledTask.+?-Settings\s+\$selectionSettings'
    }

    It 'keeps settlement wakeable and nonoverlapping without burst retries' {
        $settlementSettings = [regex]::Match(
            $script:installerText,
            '(?s)\$settlementSettings\s*=\s*New-ScheduledTaskSettingsSet(?<body>.+?)\$principal\s*='
        ).Groups['body'].Value
        $settlementSettings | Should Match '-WakeToRun'
        $settlementSettings | Should Match '-MultipleInstances\s+IgnoreNew'
        $settlementSettings | Should Not Match '-RestartCount'
        $script:installerText | Should Match '(?s)\$settlementDefinition\s*=\s*New-ScheduledTask.+?-Settings\s+\$settlementSettings'
    }

    It 'preserves the interactive limited user principal' {
        $script:installerText | Should Match '(?s)New-ScheduledTaskPrincipal.+?-LogonType\s+Interactive.+?-RunLevel\s+Limited'
    }
}
