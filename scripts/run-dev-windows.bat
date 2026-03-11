@echo off
echo ==========================================
echo   SlideCap - Dev Mode
echo ==========================================
echo.
echo Starting backend and frontend...
echo Close this window to stop both.
echo.

cd /d "%~dp0..\backend"
start "SlideCap Backend" cmd /k "python -m uvicorn app.main:app --host 127.0.0.1 --port 8000"

timeout /t 3 /nobreak >nul

cd /d "%~dp0..\frontend"
start "SlideCap Frontend" cmd /k "npm run dev"

timeout /t 5 /nobreak >nul

echo.
echo Opening http://localhost:5173 ...
start http://localhost:5173
echo.
echo Both servers are running in separate windows.
echo Close those windows to stop the servers.
pause
