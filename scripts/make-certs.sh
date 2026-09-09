#!/usr/bin/env bash
# Generate a TLS cert/key pair for SlideCap.
#
# Why: browsers treat a plain-http origin that isn't localhost as untrustworthy
# and quietly withhold capabilities from it. Chromium blocks .zip downloads from
# one (a Data Pull export stalls at 100% and never finalizes in browsers that
# don't surface the "Keep" prompt), and navigator.clipboard is undefined there.
# Serving over https fixes both.
#
# Uses mkcert, which also installs a local CA — workstations that trust that CA
# get a clean padlock with no warnings. Install it first:
#   macOS:    brew install mkcert
#   Windows:  choco install mkcert   (or scoop install mkcert)
#
# Usage: scripts/make-certs.sh [hostname ...]
# Defaults to this machine's hostname, its LAN IP, and localhost.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$REPO_ROOT/certs"

if ! command -v mkcert >/dev/null 2>&1; then
  echo "ERROR: mkcert not found. Install it (brew install mkcert) and re-run." >&2
  exit 1
fi

if [ $# -gt 0 ]; then
  HOSTS=("$@")
else
  LAN_IP="$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}' || true)"
  HOSTS=("$(hostname)" "localhost" "127.0.0.1")
  [ -n "${LAN_IP:-}" ] && HOSTS+=("$LAN_IP")
fi

mkdir -p "$CERT_DIR"
echo "Installing local CA (may prompt for your password)..."
mkcert -install

echo "Issuing certificate for: ${HOSTS[*]}"
mkcert -cert-file "$CERT_DIR/slidecap.pem" \
       -key-file  "$CERT_DIR/slidecap-key.pem" \
       "${HOSTS[@]}"

cat <<MSG

Done. Certificate written to certs/

Point both halves of the app at it:

  export SSL_CERTFILE="$CERT_DIR/slidecap.pem"
  export SSL_KEYFILE="$CERT_DIR/slidecap-key.pem"

then start the backend (python run_server.py) and frontend (npm run dev) as
usual — both read those two variables. Browse to https://<hostname>:3000

On every OTHER machine that uses SlideCap, run 'mkcert -install' once with the
CA copied from "\$(mkcert -CAROOT)" on this machine, or accept the browser
warning once per workstation.
MSG
