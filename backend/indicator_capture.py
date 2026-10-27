"""Single-writer minute-bar capture for display-only technical indicators."""
from __future__ import annotations

import json
import os
from pathlib import Path

from backend.equity_context import EquityContextTracker

ALLOWED_SYMBOLS = ("SPY", "QQQ", "IWM", "DIA")


def load_capture_config(path):
    """Explicit opt-in; loading configuration performs no provider or DB I/O."""
    path = Path(path)
    if not path.is_file():
        return {"enabled": False, "symbols": list(ALLOWED_SYMBOLS)}
    config = json.loads(path.read_text(encoding="utf-8-sig")).get("indicator_capture", {})
    if not isinstance(config, dict) or type(config.get("enabled", False)) is not bool:
        raise ValueError("INDICATOR_CAPTURE_CONFIG_INVALID")
    symbols = config.get("symbols", list(ALLOWED_SYMBOLS))
    if (not isinstance(symbols, list) or not symbols or len(symbols) > 4
            or any(s not in ALLOWED_SYMBOLS for s in symbols) or len(set(symbols)) != len(symbols)):
        raise ValueError("INDICATOR_CAPTURE_SYMBOLS_INVALID")
    return {"enabled": config.get("enabled", False), "symbols": symbols}


class IndicatorCapture(EquityContextTracker):
    """A separate store and OS lease keep dashboard reads and OPRA independent."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._lease = None

    def _acquire_lease(self):
        if self.db_path is None:
            raise ValueError("INDICATOR_CAPTURE_STORE_REQUIRED")
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        lease = self.db_path.with_suffix(".capture.lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                lease.seek(0, 2)
                if not lease.tell():
                    lease.write(b"\0")
                    lease.flush()
                lease.seek(0)
                msvcrt.locking(lease.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(lease.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            lease.close()
            raise
        self._lease = lease

    def _release_lease(self):
        lease, self._lease = self._lease, None
        if lease is not None:
            lease.close()

    def start(self):
        if not self.enabled or (self._thread and self._thread.is_alive()):
            return False
        try:
            self._acquire_lease()
        except (OSError, ValueError):
            with self._lock:
                self._failure = "INDICATOR_CAPTURE_WRITER_UNAVAILABLE"
            return False
        try:
            with self._lock:
                if self._failure in {"INDICATOR_CAPTURE_WRITER_UNAVAILABLE", "INDICATOR_CAPTURE_START_FAILED"}:
                    self._failure = None
            if not super().start():
                self._release_lease()
                return False
            return True
        except Exception:
            self._release_lease()
            with self._lock:
                self._failure = "INDICATOR_CAPTURE_START_FAILED"
            return False

    def _run(self):
        # Retain the lease until any in-flight request exits, even after stop.
        try:
            super()._run()
        finally:
            self._release_lease()

    def snapshot(self, now=None):
        state = super().snapshot(now=now)
        state.update(scope="technical_indicator_display_only", prediction_inputs_changed=False)
        return state
