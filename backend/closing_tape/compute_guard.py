from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class ActiveCaptureSession:
    session_id: str
    trading_date: str
    pid: int
    observed_at_utc: str
    status_path: str
    feed_names: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _pid_is_running(pid: int) -> bool:
    """Return whether *pid* is live without sending it a signal on Windows."""
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        still_active = 259
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
        if not handle:
            # Access denied still proves that a process owns the PID. Other
            # failures (especially ERROR_INVALID_PARAMETER) mean it is absent.
            return ctypes.get_last_error() == 5
        try:
            exit_code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return True
            return exit_code.value == still_active
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def find_active_capture_sessions(
    project_root: str | Path,
    *,
    max_status_age_seconds: float = 120.0,
    now_utc: datetime | None = None,
) -> tuple[ActiveCaptureSession, ...]:
    """Find running recorders from a fresh heartbeat or a still-live stale PID."""
    if max_status_age_seconds <= 0:
        raise ValueError("max_status_age_seconds must be positive")
    observed_now = now_utc or datetime.now(timezone.utc)
    if observed_now.tzinfo is None:
        raise ValueError("now_utc must include a timezone")
    root = Path(project_root).resolve()
    tape_root = root / "data" / "closing_tape"
    if not tape_root.is_dir():
        return ()

    active: list[ActiveCaptureSession] = []
    for status_path in sorted(tape_root.glob("*/status.json")):
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            session = payload.get("session") or {}
            feeds = payload.get("feeds") or []
            observed = datetime.fromisoformat(
                str(payload.get("observed_at_utc") or "").replace("Z", "+00:00")
            )
            pid = int(payload.get("pid") or 0)
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if observed.tzinfo is None or pid <= 0:
            continue
        status_age = (observed_now.astimezone(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds()
        if status_age < -5.0:
            continue
        if str(session.get("status") or "").lower() != "running":
            continue
        running_feeds = tuple(
            sorted(
                str(feed.get("feed_name") or "")
                for feed in feeds
                if isinstance(feed, dict)
                and str(feed.get("status") or "").lower() == "running"
                and str(feed.get("feed_name") or "")
            )
        )
        if not running_feeds:
            continue
        if status_age > max_status_age_seconds and not _pid_is_running(pid):
            continue
        active.append(
            ActiveCaptureSession(
                session_id=str(session.get("session_id") or ""),
                trading_date=str(session.get("trading_date") or status_path.parent.name),
                pid=pid,
                observed_at_utc=observed.astimezone(timezone.utc).isoformat(),
                status_path=str(status_path.resolve()),
                feed_names=running_feeds,
            )
        )
    return tuple(active)


def require_compute_window(
    project_root: str | Path,
    *,
    operation: str,
    allow_active_capture: bool = False,
    override_supported: bool = True,
    max_status_age_seconds: float = 120.0,
) -> tuple[ActiveCaptureSession, ...]:
    """Refuse expensive research work while a recorder session is running."""
    active = find_active_capture_sessions(
        project_root,
        max_status_age_seconds=max_status_age_seconds,
    )
    if active and not allow_active_capture:
        sessions = ", ".join(item.session_id or item.trading_date for item in active)
        next_step = (
            "wait for terminal finalization or use the explicit active-capture override"
            if override_supported
            else "wait for terminal finalization; this operation has no active-capture override"
        )
        raise RuntimeError(
            f"{operation} refused while closing-tape capture is active: {sessions}; "
            f"{next_step}"
        )
    return active
