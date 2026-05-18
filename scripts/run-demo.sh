#!/usr/bin/env bash
# Launch the SlideCap demo environment alongside prod.
#
# - Backend: port 8001, APP_MODE=demo, isolated SQLite at ~/.slidecap-demo
# - Frontend: vite on port 3001, VITE_APP_MODE=demo, targets backend on :8001
#
# Prod (port 8000 / 3000) keeps running untouched. Both instances share the
# same NETWORK_ROOT so the demo sees real slides — PHI redaction happens at
# the display layer in the browser.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
  echo
  echo "Shutting down demo instances..."
  kill 0 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cd "$REPO_ROOT/backend"
APP_MODE=demo \
  PORT=8001 \
  LOCAL_DATA_DIR="$HOME/.slidecap-demo" \
  python run_server.py &

cd "$REPO_ROOT/frontend"
npm run dev:demo &

wait
