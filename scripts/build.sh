#!/bin/bash
set -e

# =============================================================
# SlideCap Build Script
# Builds a self-contained desktop app (Mac .dmg or Windows .exe)
# =============================================================
#
# Prerequisites (one-time):
#   pip install -r backend/requirements.txt pyinstaller
#   cd frontend && npm install
#
# Usage:
#   ./scripts/build.sh          # Build for current platform
#   ./scripts/build.sh mac      # Build .dmg
#   ./scripts/build.sh win      # Build Windows installer
#   ./scripts/build.sh backend  # Only build backend exe
#   ./scripts/build.sh frontend # Only build frontend + package

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
TARGET="${1:-$(uname -s)}"

echo "=========================================="
echo "  SlideCap Build"
echo "=========================================="
echo "Root:     $ROOT_DIR"
echo "Target:   $TARGET"
echo ""

# ----- Step 1: Build backend with PyInstaller -----
build_backend() {
    echo "[1/3] Building backend executable..."
    cd "$BACKEND_DIR"

    # Clean previous build
    rm -rf build/ dist/

    pyinstaller slidecap-backend.spec --noconfirm

    echo "  ✓ Backend built: $BACKEND_DIR/dist/slidecap-backend/"
}

# ----- Step 2: Build frontend -----
build_frontend() {
    echo "[2/3] Building frontend..."
    cd "$FRONTEND_DIR"
    npm run build
    echo "  ✓ Frontend built: $FRONTEND_DIR/dist/"
}

# ----- Step 3: Package with electron-builder -----
package_app() {
    echo "[3/3] Packaging desktop app..."
    cd "$FRONTEND_DIR"

    case "$TARGET" in
        mac|Mac|Darwin)
            npx electron-builder --mac
            echo "  ✓ Mac app built: $FRONTEND_DIR/release/"
            ;;
        win|Win|windows|Windows|MINGW*|MSYS*)
            npx electron-builder --win
            echo "  ✓ Windows installer built: $FRONTEND_DIR/release/"
            ;;
        *)
            npx electron-builder
            echo "  ✓ App built: $FRONTEND_DIR/release/"
            ;;
    esac
}

case "$TARGET" in
    backend)
        build_backend
        ;;
    frontend)
        build_frontend
        package_app
        ;;
    *)
        build_backend
        build_frontend
        package_app
        ;;
esac

echo ""
echo "=========================================="
echo "  Build complete!"
echo "=========================================="
echo "Output: $FRONTEND_DIR/release/"
echo ""
ls -lh "$FRONTEND_DIR/release/" 2>/dev/null || echo "(check release directory)"
