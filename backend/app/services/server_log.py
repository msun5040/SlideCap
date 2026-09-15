"""
In-process capture of the backend's console output, for the password-gated
Server Log view (Settings → Server log).

The backend logs through a mix of print() (most of main.py and the services) and
the logging module (uvicorn's startup, access and error lines). Both are captured:

  * sys.stdout / sys.stderr are wrapped in a tee that still writes to the real
    console — whatever is watching the terminal or service output sees exactly
    what it did before — and additionally records complete lines.
  * A logging.Handler is attached to uvicorn's loggers, because uvicorn builds its
    handlers before the app is imported and they hold references to the original
    streams, bypassing the tee.

Lines go to a bounded in-memory ring (what the UI tails) and to a rotating file
under LOCAL_DATA_DIR/logs (what "Download" serves, and what survives restarts).

Capture must never take the server down: every failure path degrades to plain
console output, and a re-entrancy guard stops a failing file write from printing
a traceback that is itself captured, forever.
"""
from __future__ import annotations

import logging
import logging.handlers
import re
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

LOG_FILE_NAME = "server.log"
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024
LOG_FILE_BACKUPS = 5
# A single runaway line (a dumped array, a base64 blob) shouldn't eat the buffer.
MAX_LINE_CHARS = 8000

# The Server Log view polls this path every couple of seconds; without a filter
# the access log would fill with its own polling.
POLL_PATH_PREFIX = "/admin/logs"

_ERROR_RE = re.compile(
    r"\b(ERROR|CRITICAL|FATAL)\b|Traceback \(most recent call last\)|\b[A-Za-z_]*(Error|Exception):"
)
_WARN_RE = re.compile(r"\bWARN(ING)?\b|\bWarning:")

_local = threading.local()


@dataclass
class LogLine:
    seq: int
    ts: float
    stream: str   # stdout | stderr | access | uvicorn
    level: str    # error | warning | info | debug
    text: str

    def to_dict(self) -> dict:
        return {"seq": self.seq, "ts": self.ts, "stream": self.stream,
                "level": self.level, "text": self.text}


class LogBuffer:
    """Thread-safe ring of recent lines with monotonically increasing seq ids."""

    def __init__(self, max_lines: int = 5000):
        self._lines: deque[LogLine] = deque(maxlen=max(100, int(max_lines)))
        self._lock = threading.Lock()
        self._seq = 0
        self.started_at = datetime.now()
        self.boot_id = f"{time.time():.6f}"
        self.file_path: Optional[Path] = None
        self._file_logger: Optional[logging.Logger] = None
        # Per-stream level of the previous line, so traceback continuation
        # lines ("  File ...", "    raise ...") inherit "error".
        self._last_level: dict[str, str] = {}

    @property
    def capacity(self) -> int:
        return self._lines.maxlen or 0

    def attach_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=LOG_FILE_MAX_BYTES, backupCount=LOG_FILE_BACKUPS, encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        lg = logging.getLogger("slidecap.server_log.file")
        lg.propagate = False
        lg.handlers[:] = [handler]
        lg.setLevel(logging.INFO)
        self._file_logger = lg
        self.file_path = path

    def _classify(self, text: str, stream: str, level: Optional[str]) -> str:
        if level:
            lv = level.upper()
            if lv in ("ERROR", "CRITICAL", "FATAL"):
                return "error"
            if lv in ("WARNING", "WARN"):
                return "warning"
            if lv == "DEBUG":
                return "debug"
            return "info"
        if _ERROR_RE.search(text):
            return "error"
        if _WARN_RE.search(text):
            return "warning"
        prev = self._last_level.get(stream)
        if prev == "error" and (text[:1].isspace() or text.startswith(("During handling", "The above exception"))):
            return "error"
        return "info"

    def add(self, text: str, stream: str, level: Optional[str] = None) -> None:
        if getattr(_local, "busy", False):
            return
        text = text.rstrip("\r")
        if len(text) > MAX_LINE_CHARS:
            text = text[:MAX_LINE_CHARS] + " …[truncated]"
        with self._lock:
            lvl = self._classify(text, stream, level)
            self._last_level[stream] = lvl
            self._seq += 1
            line = LogLine(self._seq, time.time(), stream, lvl, text)
            self._lines.append(line)
        fl = self._file_logger
        if fl is not None:
            _local.busy = True
            try:
                stamp = datetime.fromtimestamp(line.ts).strftime("%Y-%m-%d %H:%M:%S")
                fl.info("%s %-7s %s", stamp, lvl.upper(), text)
            except Exception:
                pass
            finally:
                _local.busy = False

    def since(self, after: int, limit: int) -> dict:
        """
        Lines with seq > after. `after=0` means "start tailing": return the most
        recent `limit` lines. Otherwise return the oldest `limit` newer lines and
        set has_more so the client keeps paging. `gap` is true when lines the
        client hasn't seen have already fallen out of the ring.
        """
        with self._lock:
            lines = list(self._lines)
            latest = self._seq
        oldest = lines[0].seq if lines else latest + 1
        newer = [ln for ln in lines if ln.seq > after]
        has_more = False
        if after == 0:
            newer = newer[-limit:]
        elif len(newer) > limit:
            newer = newer[:limit]
            has_more = True
        gap = after > 0 and after + 1 < oldest
        return {
            "lines": [ln.to_dict() for ln in newer],
            "next_seq": newer[-1].seq if newer else (latest if after == 0 else after),
            "latest_seq": latest,
            "has_more": has_more,
            "gap": gap,
            "boot_id": self.boot_id,
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "capacity": self.capacity,
            "file_available": bool(self.file_path and self.file_path.exists()),
        }


class _Tee:
    """Writes through to the real stream and records complete lines."""

    def __init__(self, original, stream_name: str, buffer: LogBuffer):
        self._original = original
        self._name = stream_name
        self._buffer = buffer
        self._pending = ""
        self._lock = threading.Lock()

    def write(self, s):
        n = len(s) if isinstance(s, str) else 0
        if self._original is not None:
            try:
                n = self._original.write(s)
            except Exception:
                pass
        if not isinstance(s, str) or getattr(_local, "busy", False):
            return n
        try:
            with self._lock:
                parts = (self._pending + s).split("\n")
                self._pending = parts.pop()
                if len(self._pending) > MAX_LINE_CHARS:
                    parts.append(self._pending)
                    self._pending = ""
            for part in parts:
                if part.strip():
                    self._buffer.add(part, self._name)
        except Exception:
            pass
        return n

    def flush(self):
        if self._original is not None:
            try:
                self._original.flush()
            except Exception:
                pass

    def isatty(self):
        try:
            return bool(self._original and self._original.isatty())
        except Exception:
            return False

    def __getattr__(self, name):
        # encoding, fileno, buffer, … — whatever the real stream offers. A frozen
        # windowed build can have sys.stdout = None; fail like a closed stream.
        if self._original is None:
            raise AttributeError(name)
        return getattr(self._original, name)


class _BufferHandler(logging.Handler):
    """Records uvicorn's log records (whose own handlers bypass the tee)."""

    def __init__(self, buffer: LogBuffer):
        super().__init__(logging.INFO)
        self._buffer = buffer
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(_local, "busy", False):
            return
        try:
            msg = self.format(record)
            level = record.levelname
            stream = "uvicorn"
            if record.name == "uvicorn.access":
                stream = "access"
                args = record.args
                if isinstance(args, tuple) and len(args) >= 5:
                    try:
                        code = int(args[4])
                        level = "ERROR" if code >= 500 else "WARNING" if code >= 400 else level
                    except (TypeError, ValueError):
                        pass
            for line in msg.splitlines() or [""]:
                if line.strip():
                    self._buffer.add(line, stream, level)
        except Exception:
            pass


class _QuietLogPolling(logging.Filter):
    """Drop the Server Log view's own polling requests from the access log."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if (isinstance(args, tuple) and len(args) >= 3 and isinstance(args[2], str)
                and args[2].startswith(POLL_PATH_PREFIX)):
            return False
        return True


_buffer: Optional[LogBuffer] = None


def install(log_dir: Optional[Path], max_lines: int = 5000) -> LogBuffer:
    """Start capturing. Idempotent — safe if the app module is imported twice."""
    global _buffer
    if _buffer is not None:
        return _buffer
    buf = LogBuffer(max_lines)
    _buffer = buf

    if log_dir is not None:
        try:
            buf.attach_file(Path(log_dir) / LOG_FILE_NAME)
        except Exception as e:
            print(f"[server-log] could not open log file in {log_dir}: {e} — keeping memory only")

    if not isinstance(sys.stdout, _Tee):
        sys.stdout = _Tee(sys.stdout, "stdout", buf)
    if not isinstance(sys.stderr, _Tee):
        sys.stderr = _Tee(sys.stderr, "stderr", buf)

    handler = _BufferHandler(buf)
    for name in ("uvicorn", "uvicorn.access"):
        lg = logging.getLogger(name)
        if not any(isinstance(h, _BufferHandler) for h in lg.handlers):
            lg.addHandler(handler)
    access = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, _QuietLogPolling) for f in access.filters):
        access.addFilter(_QuietLogPolling())
    return buf


def get_buffer() -> Optional[LogBuffer]:
    return _buffer
