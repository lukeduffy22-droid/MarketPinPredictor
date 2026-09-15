$MarketCalendarPath = Join-Path $PSScriptRoot 'config\us_cash_equity_calendar.json'

function Test-MarketCalendarIsoDate {
    param([Parameter(Mandatory = $true)][string]$Value)

    if ($Value -notmatch '^\d{4}-\d{2}-\d{2}$') {
        return $false
    }
    $parsed = [datetime]::MinValue
    return [datetime]::TryParseExact(
        $Value,
        'yyyy-MM-dd',
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::None,
        [ref]$parsed
    ) -and $parsed.ToString('yyyy-MM-dd') -ceq $Value
}

function Test-UsCashEquityMarketCalendarContract {
    param([Parameter(Mandatory = $true)][psobject]$Calendar)

    $required = @(
        'schema_version',
        'supported_years',
        'regular_full_closures',
        'one_off_full_closures'
    )
    $properties = @($Calendar.PSObject.Properties.Name)
    if (@($required | Where-Object { $_ -notin $properties }).Count -gt 0) {
        return $false
    }
    if (
        [string]$Calendar.schema_version -cne 'marketpin-us-cash-equity-calendar.v1' -or
        $Calendar.supported_years -isnot [array] -or
        $Calendar.regular_full_closures -isnot [array] -or
        $Calendar.one_off_full_closures -isnot [pscustomobject]
    ) {
        return $false
    }

    $supportedYears = @($Calendar.supported_years)
    if ($supportedYears.Count -eq 0) {
        return $false
    }
    $normalizedYears = [System.Collections.Generic.List[int]]::new()
    foreach ($year in $supportedYears) {
        if ($year -isnot [int] -and $year -isnot [long]) {
            return $false
        }
        $integerYear = [int]$year
        if ($integerYear -lt 2000 -or $integerYear -gt 2100 -or $integerYear -in $normalizedYears) {
            return $false
        }
        $normalizedYears.Add($integerYear)
    }

    $regularClosures = @($Calendar.regular_full_closures)
    if ($regularClosures.Count -eq 0) {
        return $false
    }
    $allClosureDates = [System.Collections.Generic.HashSet[string]]::new(
        [StringComparer]::Ordinal
    )
    $regularYears = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($closure in $regularClosures) {
        if ($closure -isnot [string] -or -not (Test-MarketCalendarIsoDate -Value $closure)) {
            return $false
        }
        $closureYear = [int]$closure.Substring(0, 4)
        if ($closureYear -notin $normalizedYears -or -not $allClosureDates.Add($closure)) {
            return $false
        }
        [void]$regularYears.Add($closureYear)
    }
    if (@($normalizedYears | Where-Object { $_ -notin $regularYears }).Count -gt 0) {
        return $false
    }

    foreach ($property in @($Calendar.one_off_full_closures.PSObject.Properties)) {
        $closure = [string]$property.Name
        if (
            -not (Test-MarketCalendarIsoDate -Value $closure) -or
            [int]$closure.Substring(0, 4) -notin $normalizedYears -or
            -not $allClosureDates.Add($closure) -or
            [string]::IsNullOrWhiteSpace([string]$property.Value)
        ) {
            return $false
        }
    }
    return $true
}

function Get-UsCashEquityMarketCalendarStatus {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][datetime]$Candidate,
        [string]$CalendarPath = $MarketCalendarPath
    )

    $day = $Candidate.Date
    $isoDate = $day.ToString('yyyy-MM-dd')
    if (-not (Test-Path -LiteralPath $CalendarPath -PathType Leaf)) {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $false
            MarketOpen = $false
            Reason = 'calendar_file_missing'
        }
    }
    try {
        $calendar = Get-Content -LiteralPath $CalendarPath -Raw -ErrorAction Stop |
            ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $false
            MarketOpen = $false
            Reason = 'calendar_file_invalid'
        }
    }
    if (-not (Test-UsCashEquityMarketCalendarContract -Calendar $calendar)) {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $false
            MarketOpen = $false
            Reason = 'calendar_contract_invalid'
        }
    }
    $supportedYears = @($calendar.supported_years | ForEach-Object { [int]$_ })
    if ($day.Year -notin $supportedYears) {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $false
            MarketOpen = $false
            Reason = 'calendar_year_unsupported'
        }
    }
    if ($day.DayOfWeek -in @([DayOfWeek]::Saturday, [DayOfWeek]::Sunday)) {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $true
            MarketOpen = $false
            Reason = 'weekend'
        }
    }
    $regularClosures = @($calendar.regular_full_closures | ForEach-Object { [string]$_ })
    if ($isoDate -in $regularClosures) {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $true
            MarketOpen = $false
            Reason = 'regular_full_closure'
        }
    }
    $oneOffClosures = @($calendar.one_off_full_closures.PSObject.Properties.Name)
    if ($isoDate -in $oneOffClosures) {
        return [pscustomobject]@{
            Date = $isoDate
            Supported = $true
            MarketOpen = $false
            Reason = 'one_off_full_closure'
        }
    }
    return [pscustomobject]@{
        Date = $isoDate
        Supported = $true
        MarketOpen = $true
        Reason = 'regular_session_date'
    }
}

function Test-UsCashEquityMarketOpenDate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][datetime]$Candidate,
        [string]$CalendarPath = $MarketCalendarPath
    )

    return [bool](Get-UsCashEquityMarketCalendarStatus `
        -Candidate $Candidate `
        -CalendarPath $CalendarPath).MarketOpen
}

Export-ModuleMember -Function @(
    'Get-UsCashEquityMarketCalendarStatus',
    'Test-UsCashEquityMarketOpenDate'
)
