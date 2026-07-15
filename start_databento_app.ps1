Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force

$AppDir = $PSScriptRoot
$env:MARKET_DATA_PROVIDER = "databento"
$env:DATABENTO_SYMBOLS = "SPX,NDX,VIX"
$env:DATABENTO_REPLAY_MINUTES = "2"
$env:DATABENTO_SNAPSHOT_INTERVAL_SECONDS = "60"

Write-Host "Starting MarketPinPredictor with Databento OPRA..." -ForegroundColor Cyan

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "cd `"$AppDir`"; `$env:MARKET_DATA_PROVIDER='databento'; `$env:DATABENTO_SYMBOLS='SPX,NDX,VIX'; `$env:DATABENTO_REPLAY_MINUTES='2'; `$env:DATABENTO_SNAPSHOT_INTERVAL_SECONDS='60'; .\.venv\Scripts\python.exe -m uvicorn backend.app:app --host 127.0.0.1 --port 8000"
)

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "cd `"$AppDir`"; .\.venv\Scripts\streamlit.exe run streamlit_app.py --server.address 127.0.0.1 --server.port 8501"
)

Start-Sleep -Seconds 3
Start-Process "http://127.0.0.1:8501"

Write-Host "Backend:   http://127.0.0.1:8000" -ForegroundColor Green
Write-Host "Dashboard: http://127.0.0.1:8501" -ForegroundColor Green
