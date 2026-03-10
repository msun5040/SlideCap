"""
Entry point for PyInstaller-bundled backend.
Runs uvicorn with the FastAPI app. Accepts NETWORK_ROOT via environment variable.
"""
import os
import sys
import uvicorn

# When frozen by PyInstaller, ensure the app package is importable
if getattr(sys, 'frozen', False):
    # Running as compiled exe
    base_dir = sys._MEIPASS
    sys.path.insert(0, base_dir)

from app.config import settings

if __name__ == "__main__":
    # Allow overriding via environment
    host = os.environ.get("HOST", settings.HOST)
    port = int(os.environ.get("PORT", str(settings.PORT)))

    print(f"Starting SlideCap backend on {host}:{port}")
    print(f"Network root: {settings.NETWORK_ROOT}")

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )
