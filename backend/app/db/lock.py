"""
Application-level write lock for SQLite on network drives.

Uses a lock file to ensure only one process writes at a time.
"""
import time
import os
from pathlib import Path
from contextlib import contextmanager


class DatabaseLock:
    """
    File-based lock for coordinating database writes across multiple app instances.

    Usage:
        lock = DatabaseLock(Path("/network/drive/.slidecap"))

        with lock.write_lock():
            # Perform database write operations
            db.commit()
    """

    def __init__(self, app_data_path: Path):
        self.lock_file = Path(app_data_path) / ".db_write.lock"
        self.max_wait = 60  # Maximum seconds to wait for lock
        self.check_interval = 0.5  # Seconds between lock checks
        self.stale_threshold = 300  # Consider lock stale after 5 minutes

    def _is_lock_stale(self) -> bool:
        """Check if existing lock file is stale (process may have crashed)."""
        if not self.lock_file.exists():
            return False

        try:
            mtime = self.lock_file.stat().st_mtime
            age = time.time() - mtime
            return age > self.stale_threshold
        except OSError:
            return True

    def _acquire_lock(self) -> bool:
        """Try to acquire the lock. Returns True if successful."""
        try:
            # Check for stale lock
            if self._is_lock_stale():
                print(f"[LOCK] Removing stale lock file")
                self._release_lock()

            if self.lock_file.exists():
                return False

            # Create lock file with our PID
            self.lock_file.write_text(f"{os.getpid()}\n{time.time()}")
            return True
        except OSError:
            return False

    def _release_lock(self):
        """Release the lock."""
        try:
            if self.lock_file.exists():
                self.lock_file.unlink()
        except OSError:
            pass  # Lock file may have been removed by another process

    def _refresh_lock(self):
        """Update lock file timestamp to show we're still active."""
        try:
            if self.lock_file.exists():
                self.lock_file.write_text(f"{os.getpid()}\n{time.time()}")
        except OSError:
            pass

    @contextmanager
    def write_lock(self, timeout: float = None):
        """
        Context manager for acquiring write lock.

        Usage:
            with lock.write_lock():
                # Do writes here

        Raises:
            TimeoutError: If lock cannot be acquired within timeout
        """
        timeout = timeout or self.max_wait
        start_time = time.time()
        acquired = False

        try:
            # Try to acquire lock
            while not acquired:
                acquired = self._acquire_lock()

                if acquired:
                    break

                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    raise TimeoutError(
                        f"Could not acquire database write lock within {timeout}s. "
                        f"Another user may be writing to the database."
                    )

                time.sleep(self.check_interval)

            yield

        finally:
            if acquired:
                self._release_lock()


# Global lock instance (initialized in main.py)
_db_lock: DatabaseLock = None


def init_lock(app_data_path: Path):
    """Initialize the global database lock."""
    global _db_lock
    _db_lock = DatabaseLock(app_data_path)
    return _db_lock


def get_lock() -> DatabaseLock:
    """Get the global database lock."""
    if _db_lock is None:
        raise RuntimeError("Database lock not initialized. Call init_lock() first.")
    return _db_lock
