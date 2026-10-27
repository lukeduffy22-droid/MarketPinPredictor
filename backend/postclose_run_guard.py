"""Cross-process leases for scheduled and manual post-close CLI operations."""
from contextlib import contextmanager
import os
from pathlib import Path


class PostcloseAlreadyRunning(RuntimeError):
    pass


@contextmanager
def postclose_run_guard(project_root, mode):
    # A slow old-date settlement must not prevent the current day's immutable
    # selection from being frozen before publication.
    lane = "selection" if mode == "select" else "settlement"
    directory = Path(project_root).resolve() / "output" / "postclose_locks"
    directory.mkdir(parents=True, exist_ok=True)
    handle = (directory / (lane + ".lock")).open("a+b")
    locked = False
    try:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except OSError as exc:
            raise PostcloseAlreadyRunning("POSTCLOSE_OPERATION_ALREADY_RUNNING") from exc
        yield
    finally:
        if locked:
            if os.name == "nt":
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
