[CmdletBinding()]
param(
    [string]$ProjectRoot = 'C:\Cprojectsgpu_app\MarketPinPredictor',
    [string]$ParentRoot = 'C:\Cprojectsgpu_app'
)

# Read-only inventory. It does not delete, move, fetch, switch branches, or modify Git state.
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-FolderSize([string]$Path) {
    $bytes = (Get-ChildItem -LiteralPath $Path -Force -Recurse -File -ErrorAction SilentlyContinue |
        Measure-Object -Property Length -Sum).Sum
    return [math]::Round(([double]$bytes / 1GB), 2)
}

function Get-GitValue([string]$Directory, [string[]]$Arguments) {
    $value = @(& git --no-optional-locks -C $Directory @Arguments 2>$null)
    if ($LASTEXITCODE -ne 0) { return '<not a readable Git checkout>' }
    return (($value -join ' ').Trim())
}

function Get-GitStatusCount([string]$Directory) {
    $status = @(& git --no-optional-locks -C $Directory status --short --untracked-files=normal 2>$null)
    if ($LASTEXITCODE -ne 0) { return -1 }
    return $status.Count
}

Write-Output "Project root: $ProjectRoot"
Write-Output "Inventory time: $((Get-Date).ToString('o'))"
Write-Output ''
Write-Output '=== FOLDER SIZES (GB) ==='
Get-ChildItem -LiteralPath $ParentRoot -Directory -Force |
    ForEach-Object {
        [PSCustomObject]@{
            SizeGB = Get-FolderSize -Path $_.FullName
            Path = $_.FullName
        }
    } |
    Sort-Object SizeGB -Descending |
    Format-Table -AutoSize

Write-Output '=== CHECKOUTS AND WORKTREES ==='
$worktreeLines = @(& git --no-optional-locks -C $ProjectRoot worktree list --porcelain 2>$null)
$entries = @()
$current = @{}
foreach ($line in $worktreeLines) {
    if ($line -match '^worktree (.+)$') { $current.Path = $Matches[1] }
    elseif ($line -match '^HEAD (.+)$') { $current.Commit = $Matches[1].Substring(0, 12) }
    elseif ($line -match '^branch refs/heads/(.+)$') { $current.Branch = $Matches[1] }
    elseif ([string]::IsNullOrWhiteSpace($line) -and $current.Count -gt 0) {
        $entries += [PSCustomObject]$current
        $current = @{}
    }
}
if ($current.Count -gt 0) { $entries += [PSCustomObject]$current }

$entries | ForEach-Object {
    $branch = $_.PSObject.Properties['Branch']
    [PSCustomObject]@{
        Branch = if ($null -ne $branch) { $branch.Value } else { '<detached>' }
        Commit = $_.Commit
        Changes = Get-GitStatusCount -Directory $_.Path
        SizeGB = Get-FolderSize -Path $_.Path
        Path = $_.Path
    }
} | Format-Table -AutoSize

Write-Output ''
Write-Output 'No files were changed. Review branch and Changes columns before archiving or deleting any checkout.'