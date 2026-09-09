@echo off
echo ==========================================
echo   SlideCap - Dev Mode
echo ==========================================
echo.
echo Starting backend and frontend...
echo Close this window to stop both.
echo.

REM Serve over https when certs\ exists (scripts\make-certs.bat creates it).
REM Plain http costs us .zip downloads and clipboard access from any non-
REM localhost origin -- see backend\app\config.py.
set CERT_DIR=%~dp0..\certs
set SCHEME=http
if exist "%CERT_DIR%\slidecap.pem" (
    set SSL_CERTFILE=%CERT_DIR%\slidecap.pem
    set SSL_KEYFILE=%CERT_DIR%\slidecap-key.pem
    set SCHEME=https
    echo Using TLS certificates from %CERT_DIR%
) else (
    echo No certs found -- serving over http. Run scripts\make-certs.bat to
    echo enable https ^(needed for ZIP downloads from other machines^).
)

REM HOST pinned to loopback to match this script's previous behaviour; the
REM run_server.py default (0.0.0.0) is for the real multi-user deployment.
set HOST=127.0.0.1
set PORT=8000

cd /d "%~dp0..\backend"
start "SlideCap Backend" cmd /k "python run_server.py"

timeout /t 3 /nobreak >nul

cd /d "%~dp0..\frontend"
start "SlideCap Frontend" cmd /k "npm run dev"

timeout /t 5 /nobreak >nul

echo.
echo Opening %SCHEME%://localhost:5173 ...
start %SCHEME%://localhost:5173
echo.
echo Both servers are running in separate windows.
echo Close those windows to stop the servers.
pause
