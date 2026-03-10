@echo off
REM =============================================================
REM SlideCap Build Script for Windows
REM Builds a self-contained desktop app (.exe installer)
REM
REM Prerequisites (one-time):
REM   pip install -r backend\requirements.txt pyinstaller
REM   cd frontend && npm install
REM =============================================================

set ROOT_DIR=%~dp0..
set BACKEND_DIR=%ROOT_DIR%\backend
set FRONTEND_DIR=%ROOT_DIR%\frontend

echo ==========================================
echo   SlideCap Build (Windows)
echo ==========================================

REM Step 1: Build backend
echo [1/3] Building backend executable...
cd /d "%BACKEND_DIR%"
if exist dist rmdir /s /q dist
if exist build rmdir /s /q build
pyinstaller slidecap-backend.spec --noconfirm
if errorlevel 1 (
    echo ERROR: PyInstaller failed
    exit /b 1
)
echo   OK: Backend built

REM Step 2: Build frontend
echo [2/3] Building frontend...
cd /d "%FRONTEND_DIR%"
call npm run build
if errorlevel 1 (
    echo ERROR: Vite build failed
    exit /b 1
)
echo   OK: Frontend built

REM Step 3: Package
echo [3/3] Packaging desktop app...
call npx electron-builder --win
if errorlevel 1 (
    echo ERROR: electron-builder failed
    exit /b 1
)

echo.
echo ==========================================
echo   Build complete!
echo ==========================================
echo Output: %FRONTEND_DIR%\release\
dir "%FRONTEND_DIR%\release\" 2>nul
