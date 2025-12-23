@echo off
echo ======================================
echo   Gamma Model Local Setup (with GPU)
echo ======================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Python not found. Please install Python 3.8+ first.
    pause
    exit /b 1
)

echo Python found
echo.

REM Create virtual environment
echo Creating virtual environment...
python -m venv venv
call venv\Scripts\activate.bat

echo.
echo Installing PyTorch with CUDA support...
echo (This may take a few minutes)
echo.

REM Install PyTorch with CUDA 11.8
pip install --upgrade pip
pip install torch --index-url https://download.pytorch.org/whl/cu118

REM Install other dependencies
pip install numpy pandas

echo.
echo ======================================
echo   Checking GPU availability...
echo ======================================
python -c "import torch; print('CUDA available:', torch.cuda.is_available()); print('GPU:', torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'None')"

echo.
echo ======================================
echo   Setup Complete!
echo ======================================
echo.
echo To train a model, run:
echo   venv\Scripts\activate.bat
echo   python train_gamma_model.py your_data.csv SPX
echo.
pause
