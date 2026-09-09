@echo off
REM =============================================================
REM Generate a TLS cert/key pair for SlideCap (Windows).
REM
REM Why: browsers treat a plain-http origin that isn't localhost as
REM untrustworthy and quietly withhold capabilities from it. Chromium blocks
REM .zip downloads from one (a Data Pull export stalls at 100%% and never
REM finalizes in browsers that don't surface the "Keep" prompt), and
REM navigator.clipboard is undefined there. https fixes both.
REM
REM Prerequisite (one-time):  choco install mkcert
REM
REM Usage: scripts\make-certs.bat [hostname ...]
REM Defaults to this machine's hostname plus localhost.
REM =============================================================

set ROOT_DIR=%~dp0..
set CERT_DIR=%ROOT_DIR%\certs

where mkcert >nul 2>nul
if errorlevel 1 (
    echo ERROR: mkcert not found. Install it ^(choco install mkcert^) and re-run.
    exit /b 1
)

if not exist "%CERT_DIR%" mkdir "%CERT_DIR%"

set HOSTS=%*
if "%HOSTS%"=="" set HOSTS=%COMPUTERNAME% localhost 127.0.0.1

echo Installing local CA...
mkcert -install
if errorlevel 1 exit /b 1

echo Issuing certificate for: %HOSTS%
mkcert -cert-file "%CERT_DIR%\slidecap.pem" -key-file "%CERT_DIR%\slidecap-key.pem" %HOSTS%
if errorlevel 1 exit /b 1

echo.
echo Done. Certificate written to %CERT_DIR%
echo.
echo Point both halves of the app at it before starting:
echo   set SSL_CERTFILE=%CERT_DIR%\slidecap.pem
echo   set SSL_KEYFILE=%CERT_DIR%\slidecap-key.pem
echo.
echo scripts\run-dev-windows.bat picks these up automatically if certs\ exists.
echo Then browse to https://%COMPUTERNAME%:3000
echo.
echo On every OTHER workstation, run 'mkcert -install' once with the CA copied
echo from this machine's mkcert -CAROOT folder, or accept the warning once.
