param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('select', 'settle', 'status', 'catchup')]
    [string]$Mode,
    [string]$TradingDate
)

$ErrorActionPreference = 'Stop'

function Invoke-PostclosePython {
    param([string]$PythonPath, [string[]]$Arguments)
    # Windows PowerShell 5.1 turns redirected native stderr into error records.
    # Capture them before restoring Stop, so failures retain their actual exit
    # code and diagnostic log instead of terminating before Add-Content.
    $savedPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $PythonPath @Arguments 2>&1
        $exitCode = $LASTEXITCODE
        return [pscustomobject]@{ Output=@($output); ExitCode=$exitCode }
    }
    finally { $ErrorActionPreference = $savedPreference }
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Canonical MarketPin Python was not found: $python"
}

$central = [System.TimeZoneInfo]::FindSystemTimeZoneById('Central Standard Time')
$nowCentral = [System.TimeZoneInfo]::ConvertTime([DateTimeOffset]::UtcNow, $central)
if (-not $TradingDate) {
    $TradingDate = $nowCentral.ToString('yyyy-MM-dd')
}

$databasePath = (Join-Path $projectRoot 'data\market_data.db').Replace('\', '/')
$env:DATABASE_URL = "sqlite:///$databasePath"
$logDirectory = Join-Path $projectRoot 'logs\verified_close_postclose'
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
$logPath = Join-Path $logDirectory ("{0}-{1}.log" -f $TradingDate, $Mode)

$arguments = @(
    (Join-Path $projectRoot 'tools\run_verified_close_postclose.py'),
    $Mode,
    '--trading-date',
    $TradingDate,
    '--project-root',
    $projectRoot
)
$execution = Invoke-PostclosePython -PythonPath $python -Arguments $arguments
$output = $execution.Output
$exitCode = $execution.ExitCode
$stamp = [DateTimeOffset]::UtcNow.ToString('o')
Add-Content -LiteralPath $logPath -Value ("[{0}] exit={1}" -f $stamp, $exitCode)
if ($output) {
    Add-Content -LiteralPath $logPath -Value ($output -join [Environment]::NewLine)
    $output
}
if ($exitCode -ne 0) {
    exit $exitCode
}
