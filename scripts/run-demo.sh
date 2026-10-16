#!/usr/bin/env bash
# Launch the SlideCap demo environment alongside prod.
#
# - Backend: port 8001, APP_MODE=demo, isolated SQLite at ~/.slidecap-demo
# - Frontend: vite on port 3001, VITE_APP_MODE=demo, targets backend on :8001
#
# Prod (port 8000 / 3000) keeps running untouched.
#
# The demo runs against a SYNTHETIC library (~/.slidecap-demo/library), built by
# scripts/seed_demo_library.py. It used to share prod's NETWORK_ROOT and redact
# accessions in the browser, which leaves the real ones in the API, the network
# tab and the filenames — fine for a screen-share, not fine for an audience that
# might open devtools. With a generated library there is no real data at any
# layer, so there is nothing to redact and nothing to leak.
#
# DEMO_NETWORK_ROOT overrides the library path if you really do want the demo
# pointed somewhere else. Do not point it at the live share.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
  echo
  echo "Shutting down demo instances..."
  kill 0 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# DEMO_PYTHON overrides the interpreter used to start the backend. Falls back
# to `python`, which on a pyenv-managed machine may be a shim pointing at
# `system` with no SlideCap deps installed. Set DEMO_PYTHON to a full path
# (or use PYENV_VERSION=<env> ./scripts/run-demo.sh) when that happens.
DEMO_PYTHON="${DEMO_PYTHON:-python}"

DEMO_ROOT="${DEMO_NETWORK_ROOT:-$HOME/.slidecap-demo/library}"

if [ ! -d "$DEMO_ROOT/slides" ]; then
  echo "No demo library at $DEMO_ROOT/slides"
  echo
  echo "Build one first (about a minute):"
  echo "  cd backend && python scripts/seed_demo_library.py"
  exit 1
fi

# Belt and braces: never let the demo come up against the live share, whatever
# DEMO_NETWORK_ROOT says. The whole point is that this instance holds no PHI.
case "$DEMO_ROOT" in
  *test_directory_pt_slides*|/Volumes/DFCI*|[A-Z]:*|*Ligon*)
    echo "Refusing to start: DEMO_ROOT ($DEMO_ROOT) looks like the real slide share."
    echo "The demo instance must run against a synthetic library."
    exit 1
    ;;
esac

echo "Demo library : $DEMO_ROOT"
echo "Backend      : http://localhost:8001"
echo "Frontend     : http://localhost:3001"
echo

cd "$REPO_ROOT/backend"
# Loopback only. Demo mode skips the network-drive challenge (there is no share
# to prove access to), so binding 0.0.0.0 would put an unauthenticated instance
# on the LAN. Set DEMO_HOST=0.0.0.0 deliberately if you need to show it from
# another machine.
APP_MODE=demo \
  PORT=8001 \
  HOST="${DEMO_HOST:-127.0.0.1}" \
  NETWORK_ROOT="$DEMO_ROOT" \
  LOCAL_DATA_DIR="$HOME/.slidecap-demo" \
  "$DEMO_PYTHON" run_server.py &

cd "$REPO_ROOT/frontend"
npm run dev:demo &

wait
