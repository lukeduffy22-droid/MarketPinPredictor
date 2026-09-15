# Start the production FastAPI backend
# Run this to launch your institutional-grade prediction API

$AppDir = $PSScriptRoot
Set-Location $AppDir

Write-Host "🚀 Starting Market Pin Predictor Backend" -ForegroundColor Cyan
Write-Host ""

# Check Python environment
if (-not (Test-Path ".venv")) {
    Write-Host "❌ Virtual environment not found. Creating..." -ForegroundColor Yellow
    python -m venv .venv
}

# Activate environment
& .\.venv\Scripts\Activate.ps1

# Check for API key
if (-not $env:Massive_API) {
    Write-Host "⚠️  Warning: POLYGON_API_KEY not set!" -ForegroundColor Yellow
    Write-Host "   Set it with: `$env:Massive_API = 'your-key'" -ForegroundColor Yellow
    Write-Host ""
}

# Initialize database
Write-Host "📊 Initializing database..." -ForegroundColor Green
python -c "from backend.database import init_db; init_db()"

# Start the API server
Write-Host ""
Write-Host "✅ Starting API server on http://localhost:8000" -ForegroundColor Green
Write-Host "   Docs: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "   Health: http://localhost:8000/" -ForegroundColor Cyan
Write-Host ""

python -m backend.app
