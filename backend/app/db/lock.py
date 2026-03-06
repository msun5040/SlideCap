"""
Application-level write lock for SQLite on network drives.

Uses a threading.Lock for intra-process (thread) coordination.

The file-based lock is NOT used because NFS/SMB mounts (e.g. /Volumes/...)
keep oplocks on files even after a process exits, making the lock file
undeletable and blocking all subsequent writes.

SQLite's built-in PRAGMA busy_timeout handles any remaining cross-process
contention at the database level.
"""
import time
import threading
from pathlib import Path
from contextlib import contextmanager


class DatabaseLock:
    def __init__(self, app_data_path: Path):
        self.max_wait = 60  # Maximum seconds to wait for lock
        self._thread_lock = threading.Lock()

    @contextmanager
    def write_lock(self, timeout: float = None):
        """
        Context manager that serializes database writes within this process.

        Raises:
            TimeoutError: If the lock cannot be acquired within timeout seconds
        """
        timeout = timeout or self.max_wait
        acquired = self._thread_lock.acquire(timeout=max(timeout, 0))
        if not acquired:
            raise TimeoutError(
                f"Could not acquire database write lock within {timeout}s. "
                f"Another thread may be writing to the database."
            )
        try:
            yield
        finally:
            self._thread_lock.release()


# Global lock instance (initialized in main.py)
_db_lock: DatabaseLock = None


def init_lock(app_data_path: Path):
    """Initialize the global database lock."""
    global _db_lock
    _db_lock = DatabaseLock(app_data_path)
    # Try to clean up any stale lock file left by a previous run.
    # Failure is silently ignored (NFS oplock issue — not our problem anymore).
    lock_file = Path(app_data_path) / ".db_write.lock"
    try:
        lock_file.unlink()
    except OSError:
        pass
    return _db_lock


def get_lock() -> DatabaseLock:
    """Get the global database lock."""
    if _db_lock is None:
        raise RuntimeError("Database lock not initialized. Call init_lock() first.")
    return _db_lock
