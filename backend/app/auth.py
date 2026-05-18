"""
Network-drive proof-of-access authentication.

Users prove they can read a file from the lab's network drive to get a session token.
No passwords, no user database — drive access IS the credential.
"""
import json
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .config import settings

# Paths exempt from authentication
PUBLIC_PATHS = {
    "/health",
    "/auth/challenge",
    "/auth/verify",
    "/auth/status",
    "/",
    "/docs",
    "/openapi.json",
    "/redoc",
}


# ── Challenge management ────────────────────────────────────────

def create_challenge() -> tuple[str, str]:
    """
    Generate a 6-digit code and write it to the network drive.

    Returns (code, file_path) where file_path is the path the user
    needs to open on their mounted drive to read the code.
    """
    challenges_dir = settings.auth_challenges_path
    challenges_dir.mkdir(parents=True, exist_ok=True)

    code = "".join(random.choices(string.digits, k=6))

    # Write human-readable file
    verify_file = challenges_dir / "verify.txt"
    verify_file.write_text(
        f"SlideCap Verification Code\n"
        f"==========================\n\n"
        f"Your code: {code}\n\n"
        f"Enter this code in the browser to complete authentication.\n"
        f"This code expires in {settings.AUTH_CHALLENGE_EXPIRY_MINUTES} minutes.\n"
    )

    # Write machine-readable metadata
    meta_file = challenges_dir / "verify.meta"
    meta_file.write_text(json.dumps({
        "code": code,
        "created_at": datetime.utcnow().isoformat(),
    }))

    return code, str(verify_file)


def verify_challenge(code: str) -> bool:
    """
    Check if the submitted code matches the current challenge and hasn't expired.
    Deletes challenge files on success.
    """
    meta_file = settings.auth_challenges_path / "verify.meta"
    if not meta_file.exists():
        return False

    try:
        meta = json.loads(meta_file.read_text())
    except (json.JSONDecodeError, OSError):
        return False

    # Check expiry
    created_at = datetime.fromisoformat(meta["created_at"])
    if datetime.utcnow() - created_at > timedelta(minutes=settings.AUTH_CHALLENGE_EXPIRY_MINUTES):
        cleanup_challenge_files()
        return False

    # Check code
    if meta["code"] != code.strip():
        return False

    # Success — clean up
    cleanup_challenge_files()
    return True


def cleanup_challenge_files():
    """Remove challenge files."""
    challenges_dir = settings.auth_challenges_path
    for name in ("verify.txt", "verify.meta"):
        f = challenges_dir / name
        if f.exists():
            try:
                f.unlink()
            except OSError:
                pass


def cleanup_expired_challenges():
    """Remove challenge files if they've expired. Called on startup."""
    meta_file = settings.auth_challenges_path / "verify.meta"
    if not meta_file.exists():
        return
    try:
        meta = json.loads(meta_file.read_text())
        created_at = datetime.fromisoformat(meta["created_at"])
        if datetime.utcnow() - created_at > timedelta(minutes=settings.AUTH_CHALLENGE_EXPIRY_MINUTES):
            cleanup_challenge_files()
    except (json.JSONDecodeError, OSError, KeyError):
        cleanup_challenge_files()


# ── JWT management ──────────────────────────────────────────────

def create_token() -> str:
    """Create a JWT session token."""
    secret = settings.get_secret_key()
    payload = {
        "sub": "slidecap_user",
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(days=settings.AUTH_TOKEN_EXPIRY_DAYS),
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def verify_token(token: str) -> bool:
    """Verify a JWT session token."""
    try:
        secret = settings.get_secret_key()
        jwt.decode(token, secret, algorithms=["HS256"])
        return True
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return False


# ── Middleware ──────────────────────────────────────────────────

class AuthMiddleware(BaseHTTPMiddleware):
    """Reject unauthenticated requests to protected endpoints."""

    async def dispatch(self, request, call_next):
        # Always allow CORS preflight
        if request.method == "OPTIONS":
            return await call_next(request)

        # Allow public paths
        if request.url.path in PUBLIC_PATHS:
            return await call_next(request)

        # Check token
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else ""
        if not token or not verify_token(token):
            return JSONResponse(
                status_code=401,
                content={"detail": "Not authenticated"},
            )

        return await call_next(request)
