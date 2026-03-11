@echo off
setlocal enabledelayedexpansion

echo.
echo ==========================================
echo   SlideCap - Windows Setup
echo ==========================================
echo.

:: ---- Check Python ----
echo [1/5] Checking Python...
where python >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Please install Python 3.11+ from https://python.org
    echo        Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)
for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo   Found Python %PYVER%

:: ---- Check Node.js ----
echo [2/5] Checking Node.js...
where node >nul 2>&1
if errorlevel 1 (
    echo ERROR: Node.js not found. Please install Node.js 18+ from https://nodejs.org
    pause
    exit /b 1
)
for /f "tokens=1 delims= " %%v in ('node --version 2^>^&1') do set NODEVER=%%v
echo   Found Node.js %NODEVER%

:: ---- Install backend dependencies ----
echo [3/5] Installing Python dependencies...
cd /d "%~dp0..\backend"
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install Python dependencies.
    pause
    exit /b 1
)
pip install pyinstaller
if errorlevel 1 (
    echo ERROR: Failed to install PyInstaller.
    pause
    exit /b 1
)
echo   Python dependencies installed.

:: ---- Install frontend dependencies ----
echo [4/5] Installing frontend dependencies...
cd /d "%~dp0..\frontend"
call npm install
if errorlevel 1 (
    echo ERROR: Failed to install npm dependencies.
    pause
    exit /b 1
)
echo   Frontend dependencies installed.

:: ---- Build ----
echo [5/5] Building SlideCap...
echo.
echo   Building backend executable...
cd /d "%~dp0..\backend"
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
pyinstaller slidecap-backend.spec --noconfirm
if errorlevel 1 (
    echo ERROR: PyInstaller build failed.
    pause
    exit /b 1
)
echo   Backend built successfully.

echo.
echo   Building frontend...
cd /d "%~dp0..\frontend"
call npm run build
if errorlevel 1 (
    echo ERROR: Frontend build failed.
    pause
    exit /b 1
)
echo   Frontend built successfully.

echo.
echo   Packaging desktop app...
call npx electron-builder --win
if errorlevel 1 (
    echo ERROR: Electron packaging failed.
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   Setup complete!
echo ==========================================
echo.
echo   Installer: frontend\release\SlideCap Setup 0.1.0.exe
echo.
echo   You can also run in dev mode:
echo     Terminal 1: cd backend ^& python -m uvicorn app.main:app
echo     Terminal 2: cd frontend ^& npm run dev
echo     Open http://localhost:5173
echo.
pause
