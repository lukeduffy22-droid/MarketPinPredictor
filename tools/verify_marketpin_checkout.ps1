[CmdletBinding()]
param(
    [string]$ExpectedRoot = 'C:\Cprojectsgpu_app\MarketPinPredictor',
    [string]$ExpectedBranch,
    [string]$EditorPath
)

# Read-only identity checks. Never fetch, switch branches, save editor buffers,
# stage changes, start services, or modify a database.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Resolve-OrdinaryPath([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force
    $resolved = $item.FullName
    while ($null -ne $item) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Cannot verify identity through a filesystem link: $($item.FullName)"
        }
        $item = if ($item -is [IO.FileInfo]) { $item.Directory } else { $item.Parent }
    }
    return [IO.Path]::GetFullPath($resolved).TrimEnd('\', '/')
}

function Read-Git([string]$Directory, [string[]]$GitArguments) {
    $output = @(& git --no-optional-locks -C $Directory @GitArguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "Git identity check failed in $Directory : $($output -join ' ')"
    }
    return $output
}

try {
    $actualDirectory = Resolve-OrdinaryPath -Path (Get-Location).Path
    $expectedDirectory = Resolve-OrdinaryPath -Path $ExpectedRoot
    $gitRoot = Resolve-OrdinaryPath -Path (
        [string](Read-Git -Directory $actualDirectory -GitArguments @('rev-parse', '--show-toplevel'))
    )
    if (-not $gitRoot.Equals($expectedDirectory, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Wrong checkout. Actual: $gitRoot ; expected: $expectedDirectory"
    }
    $branch = [string](Read-Git -Directory $gitRoot -GitArguments @('branch', '--show-current'))
    if ($ExpectedBranch -and $branch -cne $ExpectedBranch) {
        throw "Unexpected branch '$branch'. Expected '$ExpectedBranch'. No branch was changed."
    }

    if ($EditorPath) {
        $editor = Get-Item -LiteralPath $EditorPath -Force
        if ($editor.PSIsContainer) { throw 'The active editor path must be a file.' }
        $filePath = Resolve-OrdinaryPath -Path $EditorPath
        if (-not $filePath.StartsWith($gitRoot + '\', [StringComparison]::OrdinalIgnoreCase)) {
            throw "Active file is outside the canonical checkout: $filePath"
        }
        $fileGitRoot = Resolve-OrdinaryPath -Path (
            [string](Read-Git -Directory $editor.DirectoryName -GitArguments @('rev-parse', '--show-toplevel'))
        )
        if (-not $fileGitRoot.Equals($gitRoot, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Active file belongs to a different nested repository: $fileGitRoot"
        }
        Write-Output "Active file: $filePath"
    }

    $commit = Read-Git -Directory $gitRoot -GitArguments @('log', '-1', '--format=%h | %cI | %s')
    $changes = @(Read-Git -Directory $gitRoot -GitArguments @('status', '--short', '--untracked-files=normal'))
    Write-Output $(if ($ExpectedBranch) {
        'PASS: canonical checkout and expected branch verified.'
    }
    else {
        'PASS: canonical checkout verified; no branch constraint was requested.'
    })
    Write-Output "Checked: $((Get-Date).ToString('o'))"
    Write-Output "Folder: $gitRoot"
    Write-Output "Branch: $branch"
    Write-Output "Branch constraint: $(if ($ExpectedBranch) { $ExpectedBranch } else { 'not requested' })"
    Write-Output "Base commit: $commit"
    Write-Output "Uncommitted status entries: $($changes.Count) (includes runtime files and untracked directories)."
    Write-Output 'Local edits are part of this working copy; the base commit alone does not describe them.'
    Write-Output 'This checks saved files, not unsaved editor buffers, other worktrees, or loaded server code.'
    exit 0
}
catch {
    Write-Output "FAIL: $($_.Exception.Message)"
    exit 1
}
